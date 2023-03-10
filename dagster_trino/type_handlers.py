from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster import InputContext, MetadataValue, OutputContext, TableColumn, TableSchema
from .io_manager import TrinoDbClient

from trino.exceptions import TrinoQueryError


class TrinoQueryTypeHandler(DbTypeHandler):
    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: str, connection
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
    ) -> str:
        """Loads the input as a Trino Query"""
        return TrinoDbClient.get_select_statement(table_slice)
    
    @property
    def supported_types(self):
        return [str]
    
class GCSTrinoTypeHandler(DbTypeHandler):
    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: str, connection
    ):
        """Loads content of a parquet file saved at the given location into a Trino managed table."""
        pass

    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection
    ) -> str:
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
    

        