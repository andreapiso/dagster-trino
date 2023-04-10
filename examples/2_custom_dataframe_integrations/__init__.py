from dagster import Definitions, load_assets_from_modules, SourceAsset
import dagster_trino
from dagster_trino.type_handlers import FilePathTypeHandler, ArrowTypeHandler, PandasArrowTypeHandler, TrinoQueryTypeHandler

from . import polars_type_handler

import os

fsspec_params = {
    "protocol": "gs",
    "token": os.environ['GCS_TOKEN'], #These environment variables are managed by Dagster
    "project": os.environ['GCS_PROJECT']
}
# The fsspec resource will be used by dagster-trino to directly access Trino's Hive
# catalog underlying stored parquet files.
fsspec_resource = dagster_trino.resources.build_fsspec_resource(fsspec_params)
trinoquery_io_manager = dagster_trino.io_manager.build_trino_iomanager([polars_type_handler.CustomPolarsTypeHandler()])

defs = Definitions(
    assets= load_assets_from_modules([polars_type_handler], group_name="custom_type_handlers"),
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