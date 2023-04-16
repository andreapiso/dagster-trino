# dagster-trino Usage Examples

# Instructions

The examples contained in this folder showcase several usage patterns for dagster-trino. 

The Trino connection setup depends on some environment variables indicating the trino host, schema, etc. It is recommended to set up a dagster `.env` [environment file](https://docs.dagster.io/guides/dagster/using-environment-variables-and-secrets) with the following fields

```bash
TRINO_PWD=
TRINO_USER=
TRINO_HOST=
TRINO_PORT=
TRINO_PROTOCOL=
TRINO_CATALOG=
TRINO_SCHEMA=
DBT_TRINO_SCHEMA=jaffle_shop #if wanting to run jaffle shop in its own schema
```
For examples accessing the object storage, more variables are needed to build the `fsspec` filesystem.

```bash
GCS_TOKEN=
GCS_PROJECT=
GCS_STAGING_PATH=
```

The examples are set to work with Google Cloud Storage, but can easily be modified to work with S3, HDFS, or others.

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
* [Integration with ibis](4_ibis_integration/) using the Trino resource to obtain an [Ibis](https://ibis-project.org/) handle connected to a Trino backend