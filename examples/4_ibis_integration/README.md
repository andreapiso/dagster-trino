# Using dagster-trino with ibis

## What is ibis

[Ibis](https://ibis-project.org/) is a Python library that provides a high-level, composable interface for working with SQL databases. It allows you to express database queries and transformations in Python code, rather than writing raw SQL statements.

Ibis also supports a wide variety of backends, including SQLite, MySQL, PostgreSQL, Apache Impala, and of course Trino. This makes it easy to write portable code that can work with different databases, without having to worry about the differences in syntax and behavior between them.

## Usage with dagster-trino

`dagster-trino` is able to provide an ibis connection, on top of a raw Trino connection and a SqlAlchemy connection, when using the trino [resource](../0_basic_resource/).

In order to make the trino resource return an ibis connetcion, all that needs to be done is to set the `connector` property of the resource, to `ibis`, as follows:

```python
defs = Definitions(
    assets=[my_dagster_assets],
    resources={
        "trino": dagster_trino.resources.trino_resource.configured(
            {
                "<other trino configs>",
                "connector": "ibis",
            }
        )
    },
)
```

When the resource connector is set to `ibis`, the function `trino.get_connection()` will return an ibis Trino backend, that can be used to query and manipulate Trino data.
 
```python
# using interactive mode within an asset ensures that the asset is materialised
# if using lazy mode, run `.execute()` beforre returning the asset.
ibis.option.interactive = True 
with context.resources.trino.get_connection() as ibis_conn:
    #ibis_conn is of type `ibis.backends.trino.Backend`
    ibis_conn.list_tables()
```
