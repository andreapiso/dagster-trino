import dagster_trino
import pandas as pd

from dagster import op, graph_asset, Definitions, In, Nothing

table_name = 'iris'

@op(required_resource_keys={"trino"})
def drop_table(context):
    '''
    This Op showcases how to run a raw Trino Query using 
    the resource `execute_query` method.
    '''
    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    context.resources.trino.execute_query(
        sql=drop_query,
        fetch_results=False
    )

@op(required_resource_keys={"trino"}, ins={"drop_if_exist": In(Nothing)})
def create_table_from_pandas(context):
    '''
    This Op showcases how to load pandas data into Trino
    using sqlalchemy. Trino does not support trailing semicolons
    which are generated by `pandas.to_sql`, therefore the resource
    provides a `pandas_trino_fix` method to remove them. 
    '''
    iris = pd.read_csv(
        "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data",
        names=[
            "Sepal length (cm)",
            "Sepal width (cm)",
            "Petal length (cm)",
            "Petal width (cm)",
            "Species",
        ],
    )
    with context.resources.trino.get_connection() as sqlalchemy_conn:
        iris.to_sql(
            name=f"{table_name}",
            con=sqlalchemy_conn,
            method=context.resources.trino.pandas_trino_fix,
            index=False
        )

@op(required_resource_keys={"trino"}, ins={"create_table": In(Nothing)})
def query_trino_to_pandas(context):
    '''
    This Op showcases how to load Trino data in Pandas using the 
    native `pandas.read_sql` method. 
    '''
    query = f"SELECT * FROM {table_name}"
    with context.resources.trino.get_connection() as sqlalchemy_conn:
        iris = pd.read_sql(sql=query, con=sqlalchemy_conn)
    print(iris)
    return iris

@graph_asset
def iris():
    '''
    Basic Asset obtained from starburst galaxy sample cluster.
    '''
    return query_trino_to_pandas(create_table_from_pandas(drop_table()))

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