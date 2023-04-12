
import dagster_trino

from dagster import asset, Definitions
from dagster_trino.type_handlers import TrinoQueryTypeHandler
from dagster_trino.types import TrinoQuery

from dagster import SourceAsset

'''
Refer to existing Trino tables even though they are not managed by Dagster!
The iris table below was created from a Trino resource in the example 
0_basic_resource/trino_pandas_sqlalchemy.py
'''
iris = SourceAsset(key="iris", io_manager_key='trino_io_manager')

@asset(io_manager_key="trino_io_manager")
def iris_unique(iris: TrinoQuery) -> TrinoQuery:
    '''
    When using the Trino IOManager, unlike the basic Trino resource,
    it is not necessary to explicitly retrieve the Trino connection 
    or execute a query. 

    When using the `TrinoQueryTypeHandler`, assets are created and loaded
    as trino queries. 

    In the example below, the `iris` parameter of type `TrinoQuery` contains
    the query to be used to select the `iris` table. A new Trino Table named 
    `iris_unique` is created using the logic in `iris_unique_query`. 
    '''
    iris_unique_query =f'''SELECT DISTINCT * FROM {iris}'''
    return iris_unique_query