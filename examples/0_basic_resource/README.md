# Using a Trino Resource

The module shows the simplest way `dagster-trino` can be used, that is, leveraging a raw `trino` resource. Using the resource, Dagster manages the Trino connection and configuration, but the user needs to explicitly get hold of the connection and execute queries against the Trino engine, for example:

```python
with context.resources.trino.get_connection() as trino_conn:
    #execute operations against Trino using the connection context manager.
```
When working with `pandas`, it is recommended to configure the `connector` property to `sqlalchemy`, in order to leverage the provided method `TrinoConnection.pandas_trino_fix` which enables Trino tables to be created from `pandas.DataFrame.to_sql()` removing the incompatible trailing semicolumns generated by pandas:

```python
df.to_sql(
    name="my_table",
    con=sqlalchemy_conn,
    method=context.resources.trino.pandas_trino_fix,
    index=False
)
```
On the other hand, reading a Trino table into a Pandas DataFrame using a `sqlalchemy` connection does not require any special fix, and can be done normally obtaining the connection as a context manager:

```python
with context.resources.trino.get_connection() as sqlalchemy_conn:
    pd.read_sql(sql=my_query, con=sqlalchemy_conn)
```