import pyarrow
from pyarrow import DataType
import dagster_trino.types as ttypes

map_arrow_trino_types = {
    ttypes.arrow_string: ttypes.trino_string,
}

def get_trino_columns_from_arrow_schema(schema:pyarrow.Schema) -> str:
    return ','.join([f"{n} {t}" for n,t in zip(
        schema.names, [map_arrow_trino_types.get(str(t), str(t)) for t in schema.types]
    )])
