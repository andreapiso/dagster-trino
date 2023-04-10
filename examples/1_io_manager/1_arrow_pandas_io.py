import dagster_trino
from dagster import asset, Definitions, SourceAsset
from dagster_trino.type_handlers import FilePathTypeHandler, ArrowTypeHandler, PandasArrowTypeHandler, TrinoQueryTypeHandler
from dagster_trino.types import TableFilePaths, TrinoQuery
import pandas as pd
import os
import pyarrow as pa


# Prepare a dictionary to be passed as `storage_options` for fsspec. 
# The example below sets up Google Cloud Storage, but S3, HDFS etc. will work similarly.
fsspec_params = {
    "protocol": "gs",
    "token": os.environ['GCS_TOKEN'], #These environment variables are managed by Dagster
    "project": os.environ['GCS_PROJECT']
}
# The fsspec resource will be used by dagster-trino to directly access Trino's Hive
# catalog underlying stored parquet files.
fsspec_resource = dagster_trino.resources.build_fsspec_resource(fsspec_params)
trinoquery_io_manager = dagster_trino.io_manager.build_trino_iomanager(
    [TrinoQueryTypeHandler(),
     FilePathTypeHandler(), #This TypeHandler creates a Trino Hive Table from parquet Files, or return the paths of files backing a table.
     ArrowTypeHandler(), #This Typehandler returns/loads a Trino Hive Table as an Arrow Table
     PandasArrowTypeHandler()] #This Typehandler returns/loads a Trino Hive Table as a Pandas Dataframe
)

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

defs = Definitions(
    assets=[trino_iris, trino_iris_as_arrow, trino_iris_as_parquet, trino_iris_as_pandas, trino_iris_as_query],
    resources={
        "trino_io_manager": trinoquery_io_manager.configured(
            {
                "user": {"env": "TRINO_USER"}, 
                "password": {"env": "TRINO_PWD"},
                "host": {"env": "TRINO_HOST"},
                "port": {"env": "TRINO_PORT"},
                "http_scheme": {"env": "TRINO_PROTOCOL"},
                "catalog": {"env": "TRINO_CATALOG"},
                "schema": {"env": "TRINO_SCHEMA"}
            }
        ),"fsspec": fsspec_resource.configured(
            {
                "tmp_path": {"env": "GCS_STAGING_PATH"} #e.g. gs://my_path
            }
        )
    },
)