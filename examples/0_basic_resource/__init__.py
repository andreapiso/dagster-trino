from dagster import Definitions, load_assets_from_modules
import dagster_trino

from . import trino_pandas_sqlalchemy

defs = Definitions(
    assets=load_assets_from_modules([trino_pandas_sqlalchemy], group_name="basic_resource"),
    resources={
        "trino": dagster_trino.resources.trino_resource.configured(
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