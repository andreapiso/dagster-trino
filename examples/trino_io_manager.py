import sys

#TODO remove path append when moving to dagster-trino as module
sys.path.append('../')

import dagster_trino
print(dagster_trino.__version__)

from dagster import asset, Definitions
from dagster_trino.type_handlers import TrinoQueryTypeHandler

from dagster import SourceAsset


trinoquery_io_manager = dagster_trino.io_manager.build_trino_iomanager([TrinoQueryTypeHandler()])

# Refer to existing Trino tables even though they are not managed by Dagster!
astronauts = SourceAsset(key="astronauts", io_manager_key='trino_io_manager')
missions = SourceAsset(key="missions", io_manager_key='trino_io_manager')

@asset(io_manager_key="trino_io_manager")
def astronauts_limited(astronauts):
    '''
    Basic Asset using the Trino IOManager
    '''
    return f'SELECT * from {astronauts} LIMIT 5'

@asset(io_manager_key="trino_io_manager")
def astronauts_missions(astronauts_limited, missions):
    query = f'''
    SELECT
        m.company_name,
        a.nationality,
        count() as number_trips
    FROM
        ({astronauts_limited}) a
    JOIN {missions} m ON m.detail LIKE format('%%%s%%', a.mission_title)
    GROUP BY
        a.nationality,
        m.company_name
    ORDER BY
        m.company_name,
        a.nationality
    '''
    return query

defs = Definitions(
    assets=[astronauts, missions, astronauts_limited, astronauts_missions],
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
        )
    },
)