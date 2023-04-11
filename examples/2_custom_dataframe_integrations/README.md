# Custom Type Handlers

It is straightfoward to extend the type handlers provided, to handle other dataframe formats, providing two methods and two properties:

* `load_input`: A function describing how to load a Trino table into the desired format. 
* `handle_output`: A function describing how to create/populate a Trino table with data in the provided format. 
* `supported_types`: A list of types supported by the type handler. The type handler will be used automatically when using those types as hints in the asset definition.
* `requires_fsspec`: Whether the type handler requires direct access to the underlying Trino storage.

For example, the following code creates a type handler for [Polars](https://www.pola.rs/):

```python
class CustomPolarsTypeHandler(TrinoBaseTypeHandler):
    def __init__(self):
        self.arrow_handler = ArrowTypeHandler()

    def handle_output(self, context: OutputContext, table_slice: TableSlice, obj: pl.DataFrame, connection):
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pl.DataFrame()
        return self.arrow_handler.handle_output(context, table_slice, obj.to_arrow(), connection)
    
    def load_input(self, context: InputContext, table_slice: TableSlice, connection):
        return pl.from_arrow(self.arrow_handler.load_input(context, table_slice, connection))
    
    @property
    def supported_types(self):
        return [pl.DataFrame]
    
    @property
    def requires_fsspec(self):
        return True
```

Since Polars supports converting from/to arrow natively, the existing `ArrowTypeHandler` can be used to simplify the creation of the custom Polars type handler. An end-to-end example using the polars type handler can be found [here](./polars_type_handler.py)

Similarly, custom type handlers for distributed systems such as Spark/Dask/Ray which support distributed reads of parquet files, can be made simpler by leveraging the existing `FilePathTypeHandler`.