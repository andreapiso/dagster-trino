# dagster-trino 

This repository contains an integration between Dagster and Trino that enables users to run Trino queries as part of their Dagster pipelines.

## Installation

To install the integration, run:

```shell
pip install dagster-trino
```

## Configuration

To configure the integration, you'll need to provide some basic information about your Trino cluster. You can do this by creating a `trino` resource in your Dagster `Definitions` object.

```python
defs = Definitions(
    assets=[my_asset],
    resources={
        "trino": dagster_trino.resources.trino_resource.configured(
            {
                "user": trino_user, 
                "password": trino_pwd,
                "host": trino_host,
                "port": trino_port,
                "http_scheme": "https",
            }
        )
    },
)
```

## Usage

Once you've configured your `trino` resource, you can use it to run queries as part of your Dagster pipelines. Here's an example:

```python
@asset(required_resource_keys={"trino"})
def my_asset():
    '''
    Basic Asset obtained from starburst galaxy sample cluster.
    '''
    query = "SELECT * FROM my_table"
    return context.resources.trino.execute_query(
        sql=query,
        fetch_results=True,
        return_pandas=True
    )
```
In this example, we define an asset that runs a Trino query and returns the results as a pandas DataFrame.

## Trino IOManager

## Contributing

If you'd like to contribute to this integration, please fork the repository and submit a pull request. We welcome bug reports, feature requests, and other contributions.

## License

This integration is provided under the MIT License. See LICENSE for more information.