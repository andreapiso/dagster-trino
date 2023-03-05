import sys

#TODO remove path append when moving to dagster-trino as module
sys.path.append('../')

import dagster_trino
print(dagster_trino.__version__)

from dagster import asset, Definitions
from dagster_trino.type_handlers import TrinoQueryTypeHandler

trinoquery_io_manager = dagster_trino.io_manager.build_trino_iomanager([TrinoQueryTypeHandler()])

# @asset(required_resource_keys={"trino"})
# def astronauts(context):
#     '''
#     Basic Asset obtained from starburst galaxy sample cluster.
#     '''
#     query = 'select "$path", * from andrea_object_storage.demo.astronauts LIMIT 10'
#     astronauts = context.resources.trino.execute_query(
#         sql=query,
#         fetch_results=True,
#         return_pandas=True
#     )
#     print(astronauts)
#     return astronauts

@asset(io_manager_key="trino_io_manager")
def astronauts_limited():
    '''
    Basic Asset using the Trino IOManager
    '''
    return 'SELECT * from andrea_object_storage.demo.astronauts LIMIT 20'

@asset(io_manager_key="trino_io_manager")
def astronauts_missions(astronauts_limited):
    query = f'''
    SELECT
        m.company_name,
        a.nationality,
        count() as number_trips
    FROM
        ({astronauts_limited}) a
    JOIN andrea_object_storage.demo.missions m ON m.detail LIKE format('%%%s%%', a.mission_title)
    GROUP BY
        a.nationality,
        m.company_name
    ORDER BY
        m.company_name,
        a.nationality
    '''
    print("EXECUTING QUERY")
    print(query)
    return query

defs = Definitions(
    assets=[astronauts_limited, astronauts_missions],
    resources={
        "trino": dagster_trino.resources.trino_resource.configured(
            {
                "user": {"env": "TRINO_USER"}, 
                "password": {"env": "TRINO_PWD"},
                "host": {"env": "TRINO_HOST"},
                "port": {"env": "TRINO_PORT"},
                "http_scheme": {"env": "TRINO_PROTOCOL"},
            }
        ),
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
        )
    },
)