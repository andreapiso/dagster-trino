import dagster_trino
import pandas as pd

from dagster import asset, Definitions

@asset(required_resource_keys={"trino"})
def iris(context):
    '''
    Basic Asset obtained from starburst galaxy sample cluster.
    '''
    query = "SELECT * FROM iris"
    with context.resources.trino.get_connection() as sqlalchemy_conn:
        iris = pd.read_sql(sql=query, con=sqlalchemy_conn)
    print(iris)
    return iris

defs = Definitions(
    assets=[iris],
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