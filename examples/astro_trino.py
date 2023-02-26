import sys

#TODO remove path append when moving to dagster-trino as module
sys.path.append('../')

import dagster_trino
print(dagster_trino.__version__)

from dagster import asset, Definitions

@asset(required_resource_keys={"trino"})
def astronauts(context):
    '''
    Basic Asset obtained from starburst galaxy sample cluster.
    '''
    query = "SELECT * FROM sample.demo.astronauts"
    astronauts = context.resources.trino.execute_query(
        sql=query,
        fetch_results=True,
        return_pandas=True
    )
    print(astronauts)
    return astronauts

defs = Definitions(
    assets=[astronauts],
    resources={
        "trino": dagster_trino.resources.trino_resource.configured(
            {
                "user": {"env": "TRINO_USER"}, 
                "password": {"env": "TRINO_PWD"},
                "host": {"env": "TRINO_HOST"},
                "port": {"env": "TRINO_PORT"},
                "http_scheme": {"env": "TRINO_PROTOCOL"},
            }
        )
    },
)