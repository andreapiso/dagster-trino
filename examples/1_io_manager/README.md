# Working with the Trino IOManager

While the Trino Resource still requires the user to explicitly interact with a Trino or SQLAlchemy connection, The IOManager is responsible for reading and writing data to and from Trino. It acts as a bridge between Trino and the user Dagster pipeline, and is responsible for ensuring that the data is properly formatted and compatible with the pipeline.

The dagster-trino IOManager has several `type_handlers` that allow the IOManager to interact with data in different formats. For example:

* `TrinoQueryTypeHandler` creates a Trino Table as a Dagster asset from a user query, or returns a query a user can execute to load an asset.
* `FilePathTypeHandler` creates a Trino Table as a Dagster asset from parquet files stored on fsspec-compatible storage (e.g. S3, GCS, HDFS), or returns the paths of parquet files backing a Trino Table on a Hive catalog.
* `ArrowTypeHandler` creates/returns Trino Table assets from/into Pyarrow Tables.

To use the IOManager, the user just needs to specify the format of the data that they want to read or write, simply by using type hints. The IOManager will then handle the conversion to and from the format specified.

## Asset I/O as Trino Queries

The [query_io_manager](./query_io_manager.py) example shows how to create and load Trino Table assets as Trino Queries. 

The IOManager can load pre-existing Trino Tables as Dagster assets using Dagster `SourceAsset` class. These tables can then be referenced in queries used by Software Defined Assets. 

```python
'''
Refer to existing Trino tables even though they were created 
outside of dagster pipelines
'''
my_table = SourceAsset(key="my_table", io_manager_key='trino_io_manager')

@asset(io_manager_key="trino_io_manager")
def my_table_distinct(my_table: TrinoQuery) -> TrinoQuery:
    return f'''SELECT DISTINCT * FROM {my_table}'''
```

## Loading Trino data directly from underlying storage

When dealing with large datasets, moving large amounts of data through the Trino client can be highly inefficient, as the data is serialized and transfered as JSON. This is especially the case when trying to load Trino data into a distributed system (e.g. Spark/Ray) as one Trino client connection will only send data to one node. 

In these situations, it can be several orders of magnitude faster to directly access the data from the underlying storage. The IOManager abstracts this process, letting the user focus on their business logic rather than cumbersome I/O operations. 

In particular, when writing a large dataframe into Trino using pandas `to_sql`, the Trino Client might reject the query as "too large", making accessing the underlying storage a better choice for Trino I/O. Example showing comparisons between the two methods are available in this folder (see [write](./write_benchmark.py) and [read](./read_benchmark.py) benchmarks).

![Write Benchmark](../_static/benchmark_write.png "Write Benchmark")
*IOManager using `ArrowPandasTypeHandler` completes write succesfully, Trino Client gives up after 400 seconds*

![Read Benchmark](../_static/benchmark_read.png "Read Benchmark")
*IOManager using `ArrowPandasTypeHandler` reads succesfully in 28s, while the Trino Client using `pandas.read_sql` takes 218s.*

**Note:** right now, Hive is the only Trino Object Storage catalog where the IOManager supports direct storage access. Delta/Iceberg support is WIP, as the capability to write Delta/Iceberg data from Pyhton is still a work in progress from those respective projects. In terms of file format, Parquet-backed tables are supported, with ORC support WIP.

The [arrow_pandas_io](./arrow_pandas_io.py) shows how to create and load Trino Table assets by leveraging the underlying storage. First, in order to obtain a consistent interface while accessing different types of storage, [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) is used. dagster-trino provides a fsspec resource that can be instantiated with a dictionary representing the parameters to pass to [fsspec.filesystem](https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.filesystem) such as the storage protocol to use, or the authentication method to access said storage.

For example, an fsspec resource accessing Google Cloud Storage might look like this:

```python
fsspec_params = {
    "protocol": "gs",
    "token": os.environ['GCS_TOKEN'],
    "project": os.environ['GCS_PROJECT']
}
# The fsspec resource will be used by dagster-trino to directly access Trino's Hive
# catalog underlying stored parquet files.
fsspec_resource = dagster_trino.resources.build_fsspec_resource(fsspec_params)
```

The resource needs to be provided as part of Dagster `Definitions` under the `fsspec` keyword:

```python
defs = Definitions(
    assets=my_assets,
    resources={
        "trino_io_manager": trinoquery_io_manager.configured(
            {...}
        ),"fsspec": fsspec_resource.configured({
                "tmp_path": {"env": "GCS_STAGING_PATH"} #e.g. gs://my_path
            }
        )
    },
)
```

When using the Dagster Trino IOManager to access object storage, data is serialized and deserialized using Arrow, a high-performance in-memory data structure that enables efficient data processing and manipulation. This allows data to be transferred between the Dagster pipeline and Trino quickly and with minimal overhead, while at the same time abstracting all I/O operations away from the user. 

For example, the code below shows the IOManager `PandasArrowTypeHandler` creating a new `trino_iris` Table in Trino from a Pandas dataframe, by saving data as Parquet using Pyarrow on Trino's Hive underlying object storage.

```python
@asset(io_manager_key='trino_io_manager') 
def trino_iris() -> pd.DataFrame:
    return pd.read_csv(
        "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data",
        names=[
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width",
            "species",
        ],
    )
```
Note that it is not necessary to specify a schema for the Trino table, as it will be automatically inferred from the arrow schema resulting from saving the dataframe as parquet.