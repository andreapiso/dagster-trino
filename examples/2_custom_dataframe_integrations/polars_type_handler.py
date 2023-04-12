import dagster_trino
from dagster import asset, Definitions, SourceAsset, InputContext, OutputContext
from dagster._core.storage.db_io_manager import TableSlice

from dagster_trino.type_handlers import TrinoBaseTypeHandler, ArrowTypeHandler

import polars as pl
import os

class CustomPolarsTypeHandler(TrinoBaseTypeHandler):
    '''
    A custom extension to the Trino IO Manager to handle Polars DataFrames, leveraging the existing
    Arrow Type Handler and simply converting Arrow Tables <--> Polars DataFrames.
    '''
    def __init__(self):
        self.arrow_handler = ArrowTypeHandler()

    def handle_output(self, context: OutputContext, table_slice: TableSlice, obj: pl.DataFrame, connection):
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pl.DataFrame()
        return self.arrow_handler.handle_output(context, table_slice, obj.to_arrow(), connection)
    
    def load_input(self, context: InputContext, table_slice: TableSlice, connection):
        return pl.from_arrow(self.arrow_handler.load_input(context, table_slice, connection))
    
    @property
    def supported_types(self):
        return [pl.DataFrame]
    
    @property
    def requires_fsspec(self):
        return True

#An existing Trino Table, built from 1_io_manager/arrow_pandas_io.py
trino_iris = SourceAsset(key="trino_iris", io_manager_key='trino_io_manager')

@asset
def trino_iris_as_polars(context, trino_iris: pl.DataFrame):
    context.log.info(f"TRINO IRIS: {trino_iris}")
    return trino_iris