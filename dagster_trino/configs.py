from dagster import Bool, Field, IntSource, StringSource

def define_trino_config():
    '''
    Trino configuration, see https://github.com/trinodb/trino-python-client for reference.
    '''
    host = Field(
        StringSource,
        description="The address of your Trino Cluster.",
        is_required=False,
    )

    user = Field(
        StringSource, 
        description="User login name.", 
        is_required=True
    )

    password = Field(
        StringSource, 
        description="User password.", 
        is_required=False
    )

    port = Field(
        IntSource, 
        description="The port used to connect to the Trino Cluster", 
        is_required=False
    )

    http_scheme = Field(
        StringSource, 
        description="The http scheme used to connect to the Trino Cluster.", 
        is_required=False
    )

    schema = Field(
        StringSource, 
        description="The schema to be used by Trino if not provided by the query.", 
        is_required=False
    )

    catalog = Field(
        StringSource, 
        description="The catalog to be used by Trino if not provided by the query.", 
        is_required=False
    )

    connector = Field(
        StringSource,
        description='Whether to Use SQLAlchemy, ibis, or a raw Trino connection',
        is_required=False
    )

    return {
        "host": host,
        "user": user,
        "password": password,
        "port": port,
        "http_scheme": http_scheme,
        "schema": schema,
        "catalog": catalog,
        "connector": connector
    }