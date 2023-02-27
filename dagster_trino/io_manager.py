from typing import Mapping, Optional, Sequence, Type, cast
from .configs import define_trino_config
from contextlib import contextmanager

from trino.exceptions import TrinoQueryError

from dagster._core.storage.db_io_manager import (
    DbClient,
    DbIOManager,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)

from dagster import Field, IOManagerDefinition, OutputContext, StringSource, io_manager

from .resources import TrinoConnection

def build_trino_iomanager(
    type_handlers: Sequence[DbTypeHandler], default_load_type: Optional[Type] = None
) -> IOManagerDefinition:
        """
        Builds an IO manager definition that reads inputs from and writes outputs to Trino.
        """
        @io_manager(
                config_schema=define_trino_config()
        )
        def trino_io_manager(init_context):
                pass

class TrinoDbClient(DbClient):
    @staticmethod
    @contextmanager
    def connect(context, table_slice):
        with TrinoConnection(
            context.resource_config,
            context.log
        ).get_connection() as conn:
            yield conn
    
    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"
        if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
            query = (
                f"SELECT {col_str} FROM"
                f" {table_slice.database}.{table_slice.schema}.{table_slice.table} WHERE\n"
            )
            partition_where = " AND\n".join(
                _static_where_clause(partition_dimension)
                if isinstance(partition_dimension.partition, str)
                else _time_window_where_clause(partition_dimension)
                for partition_dimension in table_slice.partition_dimensions
            )

            return query + partition_where
        else:
            return f"""SELECT {col_str} FROM {table_slice.database}.{table_slice.schema}.{table_slice.table}"""
        
    @staticmethod
    def delete_table_slice(context: OutputContext, table_slice: TableSlice, connection) -> None:
        try:
            connection.execute(_get_cleanup_statement(table_slice))
        except TrinoQueryError:
            # table doesn't exist yet, so ignore the error
            pass
