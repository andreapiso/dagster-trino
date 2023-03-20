import dagster_trino
import gcsfs
from dagster import asset, Definitions, SourceAsset, EnvVar
from dagster_trino.type_handlers import TrinoQueryTypeHandler,FilePathTypeHandler, ArrowTypeHandler, PandasArrowTypeHandler
from dagster_trino.types import TableFilePaths
from pyarrow import parquet
import pyarrow
import pandas as pd
import os

fsspec_params = {
    "protocol": "gs",
    "token": os.environ['GCS_TOKEN'],
    "project":"trino-catalog"
}

tmp_path = "gs://trino-object-storage/trino-hive/"

fsspec_resource = dagster_trino.resources.build_fsspec_resource(fsspec_params)
trinoquery_io_manager = dagster_trino.io_manager.build_trino_iomanager([TrinoQueryTypeHandler(), FilePathTypeHandler(), ArrowTypeHandler(), PandasArrowTypeHandler()])

# Refer to existing Trino tables even though they are not managed by Dagster!
astronauts_parquet = SourceAsset(key="astronauts_parquet", io_manager_key='trino_io_manager')

@asset()
def astronauts_pandas(context, astronauts_parquet: pd.DataFrame) -> pd.DataFrame:
    '''
    Pandas representation of the astronauts_parquet table.
    '''
    return astronauts_parquet

@asset()
def astronauts_arrow(context, astronauts_parquet: pyarrow.Table) -> pyarrow.Table:
    '''
    Pyarrow representation of the astronauts_parquet table.
    '''
    return astronauts_parquet

@asset(io_manager_key='trino_io_manager')
def astro_from_files(context) -> TableFilePaths:
    '''
    Create a Trino table from a list of parquet files
    '''
    files:TableFilePaths
    files = ['gs://trino-object-storage/trino-hive/tmp/20230311_073231_58277_imuw7_361b9230-76f8-42cf-8e81-7adbdb923466',
            'gs://trino-object-storage/trino-hive/tmp/20230311_073231_58277_imuw7_582a82c2-34a7-4463-9291-5fcc75264db6',
            'gs://trino-object-storage/trino-hive/tmp/20230311_073231_58277_imuw7_61a67b7e-d503-4b35-9959-cec66e88594b',
            'gs://trino-object-storage/trino-hive/tmp/20230311_073231_58277_imuw7_6d707f9d-708b-4a8c-b529-f905827f0c32']
    context.log.info(type(files))
    return files

@asset(io_manager_key='trino_io_manager')
def trino_from_arrow(context) -> pyarrow.Table:
    pylist = [{'n_legs': 2, 'animals': 'Flamingo'}, {'year': 2021, 'animals': 'Centipede'}]
    return pyarrow.Table.from_pylist(pylist)

@asset(io_manager_key='trino_io_manager')
def iris(context) -> pd.DataFrame:
    iris = pd.read_csv('https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv')
    return iris


defs = Definitions(
    assets=[astronauts_parquet, astronauts_pandas, astronauts_arrow, astro_from_files, trino_from_arrow, iris],
    resources={
        "trino_io_manager": trinoquery_io_manager.configured(
            {
                "user": {"env": "TRINO_USER"}, 
                "password": {"env": "TRINO_PWD"},
                "host": {"env": "TRINO_HOST"},
                "port": {"env": "TRINO_PORT"},
                "http_scheme": {"env": "TRINO_PROTOCOL"},
                "catalog": {"env": "TRINO_CATALOG"},
                "schema": {"env": "TRINO_SCHEMA"}
            }
        ),"fsspec": fsspec_resource.configured(
            {
                "tmp_path": tmp_path
            }
        )
    },
)