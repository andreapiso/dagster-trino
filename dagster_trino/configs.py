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
    return {
        "host": host,
        "user": user,
        "password": password,
        "port": port,
        "http_scheme": http_scheme
    }

    
