from dagster import asset, SourceAsset
from dagster_trino.types import TrinoQuery
import pandas as pd
import numpy as np

# Quick benchmark of using the Trino IOManager ArrowPandas type handler to read 
# Trino tables vs a simple pandas read_sql

from .write_benchmark import load_with_iomanager

@asset() 
def pandas_from_iomanager(load_with_iomanager:pd.DataFrame) -> pd.DataFrame:
    return load_with_iomanager

@asset(required_resource_keys={'trino'})
def pandas_from_trinoclient(context, load_with_iomanager:TrinoQuery) -> pd.DataFrame:
    query = f"SELECT * FROM {load_with_iomanager}"
    with context.resources.trino.get_connection() as sqlalchemy_conn:
        return pd.read_sql(sql=query, con=sqlalchemy_conn)

