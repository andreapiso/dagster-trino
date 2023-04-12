import os

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import dbt_cli_resource
from dagster_trino.io_manager import build_trino_iomanager
from dagster_trino.resources import build_fsspec_resource
from dagster_trino.type_handlers import PandasArrowTypeHandler

from . import assets
from .assets import DBT_PROFILES, DBT_PROJECT_PATH

fsspec_params = {
    "protocol": "gs",
    "token": os.environ['GCS_TOKEN'],
    "project": os.environ['GCS_PROJECT']
}

fsspec_resource = build_fsspec_resource(fsspec_params)
trino_io_manager = build_trino_iomanager([PandasArrowTypeHandler()])

resources = {
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),
    "fsspec": fsspec_resource.configured(
        {
            "tmp_path": {"env": "GCS_STAGING_PATH"} #e.g. gs://my_path
        }
    ),
    "io_manager": trino_io_manager.configured(
        {
            "user": {"env": "TRINO_USER"}, 
            "password": {"env": "TRINO_PWD"},
            "host": {"env": "TRINO_HOST"},
            "port": {"env": "TRINO_PORT"},
            "http_scheme": {"env": "TRINO_PROTOCOL"},
            "catalog": {"env": "TRINO_CATALOG"},
            # "schema": {"env": "TRINO_SCHEMA"}
        }
    )
}

defs = Definitions(assets=load_assets_from_modules([assets]), resources=resources)
