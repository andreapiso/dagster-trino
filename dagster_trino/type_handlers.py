from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster import InputContext, MetadataValue, OutputContext, TableColumn, TableSchema
from .io_manager import TrinoDbClient

class TrinoQueryTypeHandler(DbTypeHandler):
    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: str, connection
    ):
        """Stores the Trino Query in a Table."""
        connection.execute(
            f"create table if not exists {table_slice.schema}.{table_slice.table} as obj"
        )
        if not connection.fetchall():
            # table was not created, therefore already exists. Insert the data
            connection.execute(
                f"insert into {table_slice.schema}.{table_slice.table} obj"
            )
        context.add_output_metadata(
            {
                "query": obj
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