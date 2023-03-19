from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster import InputContext, MetadataValue, OutputContext, TableColumn, TableSchema
from .io_manager import TrinoDbClient
from .types import TableFilePaths, TrinoQuery

from trino.exceptions import TrinoQueryError

import pandas as pd
import pyarrow
from pyarrow import parquet
from .utils import arrow as arrow_utils

import os

class TrinoBaseTypeHandler(DbTypeHandler):
    @property
    def requires_fsspec(self):
        False

class TrinoQueryTypeHandler(TrinoBaseTypeHandler):
    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: TrinoQuery, connection
    ):
        """Stores the Trino Query in a Table."""
        try:
            connection.execute(
                f"create table {table_slice.schema}.{table_slice.table} as {obj}"
            )
        except TrinoQueryError as e:
            if e.error_name != 'TABLE_ALREADY_EXISTS':
                raise e
            # table was not created, therefore already exists. Insert the data
            connection.execute(
                f"insert into {table_slice.schema}.{table_slice.table} {obj}"
            )
        context.add_output_metadata(
            {
                "create_statement": obj
            }
        )

    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection
    ) -> TrinoQuery:
        """Loads the input as a Trino Query"""
        return TrinoDbClient.get_select_statement(table_slice)
    
    @property
    def supported_types(self):
        return [TrinoQuery]
    
class FilePathTypeHandler(TrinoBaseTypeHandler):
    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: TableFilePaths, connection
    ):
        """Loads content of files saved at the given location into a Trino managed table."""
        if len(obj) == 0:
            raise FileNotFoundError("The list of files to load in the table is empty.")
        table_dir = os.path.dirname(obj[0])
        fs = context.resources.fsspec.fs
        arrow_schema = parquet.read_schema(obj[0], filesystem=fs)
        trino_columns = arrow_utils._get_trino_columns_from_arrow_schema(arrow_schema)
        context.log.info(arrow_schema)
        context.log.info(trino_columns)
        tmp_table_name = f'{table_slice.schema}.tmp_dagster_{table_slice.table}'
        drop_query = f'''
            DROP TABLE IF EXISTS {tmp_table_name}
        '''
        create_query = f'''
            CREATE TABLE {tmp_table_name}(
                {trino_columns}
            )
            WITH (
                format = 'PARQUET',
                external_location = '{table_dir}'
            )
        '''
        select_query = f'''
            SELECT * FROM {tmp_table_name}
        '''
        connection.execute(f"{drop_query}") #if previous cleanup failed
        connection.execute(f"{create_query}")
        try:
            connection.execute(
                f"create table {table_slice.schema}.{table_slice.table} as ({select_query})"
            )
        except TrinoQueryError as e:
            if e.error_name != 'TABLE_ALREADY_EXISTS':
                raise e
            # table was not created, therefore already exists. Insert the data
            connection.execute(
                f"insert into {table_slice.schema}.{table_slice.table} ({select_query})"
            )
        context.add_output_metadata(
            {
                "run_data_files": obj
            }
        )
        connection.execute(f"{drop_query}") #cleanup temp table


    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection
    ) -> TableFilePaths:
        """Loads the paths of object storage files populating the table"""
        #TODO: work on partitioning
        query = f'''
            SELECT DISTINCT "$path" FROM {table_slice.schema}.{table_slice.table}
        '''
        try:
            res = connection.execute(query)
        except TrinoQueryError as e:
            # TODO add messaging around this functionality is supported only with the Hive connector
            raise e
        return [filepath for pathlist in res for filepath in pathlist]
    
    @property
    def supported_types(self):
        return [TableFilePaths, list]
    
    @property
    def requires_fsspec(self):
        return True
    
class ArrowTypeHandler(TrinoBaseTypeHandler):
    '''
        Type Handler to load/handle pandas types by reading/writing parquet 
        files for Trino tables backed by Parquet working against a Hive catalog.
    '''
    def __init__(self):
        self.file_handler = FilePathTypeHandler()

    def handle_output(
            self, context: OutputContext, table_slice: TableSlice, obj: pyarrow.Table, connection
        ):
        fs = context.resources.fsspec.fs
        tmp_folder = context.resources.fsspec.tmp_folder
        staging_path = os.path.join(tmp_folder, f"{table_slice.schema}_{table_slice.table}")
        parquet.write_table(obj, staging_path, filesystem=fs)
        files = fs.ls(staging_path)
        self.file_handler.handle_output(context, table_slice, 
                                            [f"{context.resources.fsspec.protocol}://{file}" for file in files], connection)
        fs.rm(staging_path, recursive=True)

    def load_input(self, context: InputContext, table_slice: TableSlice, connection) -> pyarrow.Table:
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pyarrow.Table()
        file_paths = self.file_handler.load_input(context, table_slice, connection)
        fs = context.resources.fsspec.fs
        arrow_df = parquet.ParquetDataset(file_paths, filesystem=fs)
        return arrow_df.read()
    
    @property
    def supported_types(self):
        return [pyarrow.Table]
    
    @property
    def requires_fsspec(self):
        return True
    
class PandasArrowTypeHandler(TrinoBaseTypeHandler):
    
    def __init__(self):
        self.arrow_handler = ArrowTypeHandler()

    def handle_output(self, context: OutputContext, table_slice: TableSlice, obj: pd.DataFrame, connection):
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pd.DataFrame()
        return self.arrow_handler.handle_output(context, table_slice, pyarrow.Table.from_pandas(obj), connection)
    
    def load_input(self, context: InputContext, table_slice: TableSlice, connection):
        return self.arrow_handler.load_input(context, table_slice, connection).to_pandas()
    
    @property
    def supported_types(self):
        return [pd.DataFrame]
    
    @property
    def requires_fsspec(self):
        return True