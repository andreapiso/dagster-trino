import pyarrow
from pyarrow import DataType
import dagster_trino.types as ttypes

trino_string = "VARCHAR"
trino_bool = "BOOLEAN"
trino_tinyint = "TINYINT"
trino_smallint = "SMALLINT"
trino_int = "INTEGER"
trino_bigint = "BIGINT"
trino_float = "REAL"
trino_double = "DOUBLE"
trino_decimal = "DECIMAL"

map_arrow_trino_types = {
    ttypes.arrow_string: ttypes.trino_string,
    ttypes.arrow_bool:ttypes.trino_bool,
    ttypes.arrow_tinyint:ttypes.trino_tinyint,
    ttypes.arrow_smallint:ttypes.trino_smallint,
    ttypes.arrow_int:ttypes.trino_int,
    ttypes.arrow_float:ttypes.trino_float,
    ttypes.arrow_double:ttypes.trino_double,
    ttypes.arrow_decimal:ttypes.trino_decimal,
    ttypes.arrow_date:ttypes.trino_date,
    ttypes.arrow_time:ttypes.trino_time
}

def get_trino_columns_from_arrow_schema(schema:pyarrow.Schema) -> str:
    return ','.join([f"{n} {t}" for n,t in zip(
        schema.names, [map_arrow_trino_types.get(str(t), str(t)) for t in schema.types]
    )])
