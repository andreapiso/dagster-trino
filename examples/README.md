# dagster-trino Usage Examples

# Instructions

The examples contained in this folder showcase several usage patterns for dagster-trino. 

The examples can be run from the dagit UI by executing the following command from the `examples` folder:

```shell
dagit -w workspace.yaml
```

To load only selected examples, comment or remove examples that should not be loaded from the `worskpace.yaml` file. For example the following file will instruct dagit to only load examples from the `examples/0_basic_resource` folder:

```yaml
load_from:
  - python_module: 0_basic_resource
  #- python_module: 1_io_manager
  #- python_module: 2_custom_dataframe_integrations
  ```

# Examples

The modules listed in this folder showcase different ways you can leverage `dagster-trino`:

* [Using a Trino Resource](0_basic_resource/) The most basic way to interact with dagster-trino. For example, using Pandas or SQLAlchemy methods.
* [Working with the dagster-trino IOManager](1_io_manager/) Including pushing storage and compute of Dagster assets as Trino Queries, as well as improving I/O performance over the Trino client by several orders of magnitude by letting the IOManager access the underlying Trino storage directly. 
* [Extending the IOManager to support custom DataFrames](2_custom_dataframe_integrations/) with an example of how to extend the IOManager to support [Polars](https://www.pola.rs/) with 20 lines of code.
* [Integration with dbt](3_dbt_integration/) showcasing how `dagster-trino` can work complementarily with `dbt-trino`, for exmaple, to automate fast Trino I/O from object storage upstream or downstream of a dbt project.