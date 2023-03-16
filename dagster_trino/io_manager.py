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
from dagster._core.definitions.time_window_partitions import TimeWindow

from .resources import TrinoConnection

TRINO_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S" #TODO: check correctness

def build_trino_iomanager(
    type_handlers: Sequence[DbTypeHandler], default_load_type: Optional[Type] = None
) -> IOManagerDefinition:
        """
        Builds an IO manager definition that reads inputs from and writes outputs to Trino.
        """
        required_resource_keys = set()
        if any(type_handler.requires_fsspec for type_handler in type_handlers):
            required_resource_keys.add('fsspec')

        @io_manager(
            config_schema=define_trino_config(),
            required_resource_keys=required_resource_keys
        )
        def trino_io_manager(init_context):
            return DbIOManager(
                type_handlers=type_handlers,
                db_client=TrinoDbClient(),
                io_manager_name="TrinoIoManager",
                database=init_context.resource_config["catalog"],
                schema=init_context.resource_config.get("schema"),
                default_load_type=default_load_type,
        )
        return trino_io_manager

class TrinoDbClient(DbClient):
    @staticmethod
    @contextmanager
    def connect(context, table_slice):
        with TrinoConnection(
            context.resource_config,
            context.log
        ).get_connection() as conn:
            yield conn.cursor()
    
    @staticmethod
    def ensure_schema_exists(context: OutputContext, table_slice: TableSlice, connection) -> None:
        query =f'create schema if not exists {table_slice.schema}'
        connection.execute(query)

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"

        if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
            query = f"SELECT {col_str} FROM {table_slice.schema}.{table_slice.table} WHERE\n"
            return f'({query}{_partition_where_clause(table_slice.partition_dimensions)})'
        else:
            return f'(SELECT {col_str} FROM {table_slice.schema}.{table_slice.table})'
        
    @staticmethod
    def delete_table_slice(context: OutputContext, table_slice: TableSlice, connection) -> None:
        try:
            connection.execute(_get_cleanup_statement(table_slice))
        except TrinoQueryError:
            # table doesn't exist yet, so ignore the error
            pass

def _get_cleanup_statement(table_slice: TableSlice) -> str:
    """
    Returns a SQL statement that deletes data in the given table to make way for the output data
    being written.
    """
    if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
        query = f"DELETE FROM {table_slice.schema}.{table_slice.table} WHERE\n"
        return query + _partition_where_clause(table_slice.partition_dimensions)
    else:
        return f"DELETE FROM {table_slice.schema}.{table_slice.table}"
    
def _partition_where_clause(partition_dimensions: Sequence[TablePartitionDimension]) -> str:
    return " AND\n".join(
        _time_window_where_clause(partition_dimension)
        if isinstance(partition_dimension.partitions, TimeWindow)
        else _static_where_clause(partition_dimension)
        for partition_dimension in partition_dimensions
    )

def _time_window_where_clause(table_partition: TablePartitionDimension) -> str:
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    start_dt_str = start_dt.strftime(TRINO_DATETIME_FORMAT)
    end_dt_str = end_dt.strftime(TRINO_DATETIME_FORMAT)
    return f"""{table_partition.partition_expr} >= '{start_dt_str}' AND {table_partition.partition_expr} < '{end_dt_str}'"""

def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partitions = ", ".join(f"'{partition}'" for partition in table_partition.partitions)
    return f"""{table_partition.partition_expr} in ({partitions})"""