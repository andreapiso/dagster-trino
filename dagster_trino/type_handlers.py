from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster import InputContext, MetadataValue, OutputContext, TableColumn, TableSchema
from .io_manager import TrinoDbClient
from .types import TableFilePaths, TrinoQuery

from trino.exceptions import TrinoQueryError

import pandas as pd
import pyarrow
from pyarrow import parquet
import fsspec

import os

class TrinoQueryTypeHandler(DbTypeHandler):
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
    
class FilePathTypeHandler(DbTypeHandler):
    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: TableFilePaths, connection
    ):
        """Loads content of files saved at the given location into a Trino managed table."""
        if len(obj) == 0:
            raise FileNotFoundError("The list of files to load in the table is empty.")
        table_dir = os.path.dirname(obj[0])
        tmp_table_name = f'{table_slice.schema}.tmp_dagster_{table_slice.table}'
        drop_query = f'''
            DROP TABLE IF EXISTS {tmp_table_name}
        '''
        create_query = f'''
            CREATE TABLE {tmp_table_name}(
                id DOUBLE,
                number DOUBLE,
                nationwide_number DOUBLE,
                name VARCHAR,
                original_name VARCHAR,
                sex VARCHAR,
                year_of_birth DOUBLE,
                nationality VARCHAR,
                military_civilian VARCHAR,
                selection VARCHAR,
                year_of_selection DOUBLE,
                mission_number DOUBLE,
                total_number_of_missions DOUBLE,
                occupation VARCHAR,
                year_of_mission DOUBLE,
                mission_title VARCHAR,
                ascend_shuttle VARCHAR,
                in_orbit VARCHAR,
                descend_shuttle VARCHAR,
                hours_mission DOUBLE,
                total_hrs_sum DOUBLE,
                field21 DOUBLE,
                eva_hrs_mission DOUBLE,
                total_eva_hrs DOUBLE
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
    
class ArrowTypeHandler(DbTypeHandler):
    '''
        Type Handler to load/handle pandas types by reading/writing parquet 
        files for Trino tables backed by Parquet working against a Hive catalog.
    '''
    def handle_output(
            self, context: OutputContext, table_slice: TableSlice, obj: pyarrow.Table, connection
        ):
        pass

    def load_input(self, context: InputContext, table_slice: TableSlice, connection) -> pyarrow.Table:
        file_paths = FilePathTypeHandler().load_input(context, table_slice, connection)
        #FIXME: gs config should be passed through resource instead
        fs = fsspec.filesystem(protocol='gs', project='trino-catalog', token=os.environ['GCS_TOKEN']) 
        arrow_df = parquet.ParquetDataset(file_paths, filesystem=fs)
        return arrow_df.read()
    
    @property
    def supported_types(self):
        return [pyarrow.Table]
