from dagster import asset
from dagster_trino.types import TableFilePaths, TrinoQuery
import pandas as pd
import pyarrow as pa

@asset(io_manager_key='trino_io_manager') 
def trino_iris() -> pd.DataFrame:
    '''
    Materialising this asset will create a Trino Table `trino_iris` by 
    saving pandas data in arrow parquet format on GCS using fsspec, and creating a 
    Trino Hive Table from those parquet files. 

    Note how by using the Trino IOManager, the user does not have to interface with 
    Trino, nor Arrow, Parquet or fsspec. All that is required from the user is to 
    return a pandas dataframe, which will be automatically handled by the
    `PandasArrowTypeHandler`. It is also not required to define a schema for the Trino
    Table, the IOManager will infer the schema from the Arrow Parquet and map those 
    Arrow Types into appropriate Trino Types.

    '''
    return pd.read_csv(
        "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data",
        names=[
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width",
            "species",
        ],
    )

@asset
def trino_iris_as_parquet(context, trino_iris: TableFilePaths):
    context.log.info(f"TRINO IRIS: {trino_iris}")
    return trino_iris
@asset
def trino_iris_as_arrow(context, trino_iris: pa.Table):
    context.log.info(f"TRINO IRIS: {trino_iris}")
    return trino_iris
@asset
def trino_iris_as_pandas(context, trino_iris: pd.DataFrame):
    context.log.info(f"TRINO IRIS: {trino_iris}")
    return trino_iris
@asset
def trino_iris_as_query(context, trino_iris: TrinoQuery):
    context.log.info(f"TRINO IRIS: {trino_iris}")
    return trino_iris
