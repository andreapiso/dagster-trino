from dagster import Definitions, load_assets_from_modules
import dagster_trino

from . import trino_ibis

defs = Definitions(
    assets=load_assets_from_modules([trino_ibis], group_name="ibis"),
    resources={
        "trino": dagster_trino.resources.trino_resource.configured(
            {
                "user": {"env": "TRINO_USER"}, 
                "password": {"env": "TRINO_PWD"},
                "host": {"env": "TRINO_HOST"},
                "port": {"env": "TRINO_PORT"},
                "http_scheme": {"env": "TRINO_PROTOCOL"},
                "connector": "ibis",
                "catalog": {"env": "TRINO_CATALOG"},
                "schema": {"env": "TRINO_SCHEMA"}
            }
        )
    },
)