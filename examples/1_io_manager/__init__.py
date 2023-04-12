from dagster import Definitions, load_assets_from_modules
import dagster_trino
from dagster_trino.type_handlers import FilePathTypeHandler, ArrowTypeHandler, PandasArrowTypeHandler, TrinoQueryTypeHandler
from . import query_io_manager, arrow_pandas_io, write_benchmark, read_benchmark
import os

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
    [TrinoQueryTypeHandler(), #This TypeHandler pushes down compute and storage of assets to Trino.
     FilePathTypeHandler(), #This TypeHandler creates a Trino Hive Table from parquet Files, or return the paths of files backing a table.
     ArrowTypeHandler(), #This Typehandler returns/loads a Trino Hive Table as an Arrow Table
     PandasArrowTypeHandler()] #This Typehandler returns/loads a Trino Hive Table as a Pandas Dataframe
)

defs = Definitions(
    assets=load_assets_from_modules([query_io_manager, arrow_pandas_io], group_name="io_manager") +
           load_assets_from_modules([write_benchmark, read_benchmark], group_name='benchmark'),
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
        ), "trino": dagster_trino.resources.trino_resource.configured(
            {
                "user": {"env": "TRINO_USER"}, 
                "password": {"env": "TRINO_PWD"},
                "host": {"env": "TRINO_HOST"},
                "port": {"env": "TRINO_PORT"},
                "http_scheme": {"env": "TRINO_PROTOCOL"},
                "connector": "sqlalchemy",
                "catalog": {"env": "TRINO_CATALOG"},
                "schema": {"env": "TRINO_SCHEMA"}
            }
        )
    },
)