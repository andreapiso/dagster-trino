from dagster import asset
from dagster_trino.types import TableFilePaths, TrinoQuery
import pandas as pd
import numpy as np

# Quick benchmark of using the Trino IOManager ArrowPandas type handler to create 
# Trino tables vs a simple pandas.DataFrame.to_sql 

@asset
def random_data():
    '''
    Generate a dataframe filled with Random Numbers. 
    '''
    n_rows = 2000000
    n_cols = 20
    df = pd.DataFrame(
        np.random.rand(n_rows, n_cols), 
        columns=[f"col_{i}" for i in range(n_cols)]
    )
    return df

@asset(io_manager_key='trino_io_manager') 
def load_with_iomanager(random_data) -> pd.DataFrame:
    return random_data

@asset(required_resource_keys={'trino'})
def load_with_trino_client(context, random_data) -> None:
    table_name = 'load_with_trino_client'
    with context.resources.trino.get_connection() as sqlalchemy_conn:
        random_data.to_sql(
            name=f"{table_name}",
            con=sqlalchemy_conn,
            method=context.resources.trino.pandas_trino_fix,
            index=False
        )

