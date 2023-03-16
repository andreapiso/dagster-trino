from dagster_trino import types as t
from dagster_trino.utils.arrow import _get_trino_columns_from_arrow_schema
import pyarrow as pa

def test_basic_schema():
    schema = pa.schema([
        ("col1", pa.int8()),
        ("col2", pa.string()),
        ("col3", pa.float64())
    ])
    expected_table_schema = 'col1 TINYINT,col2 VARCHAR,col3 DOUBLE'                       
    assert(_get_trino_columns_from_arrow_schema(schema)==expected_table_schema)

def test_empty_schema():
    schema = pa.schema([])
    assert(_get_trino_columns_from_arrow_schema(schema)=='')

def test_precision_types():
    schema = pa.schema([
        ("col1", pa.decimal128(2,3)),
        ("col2", pa.decimal128(4,5))
    ])
    expected_table_schema= 'col1 DECIMAL(2, 3),col2 DECIMAL(4, 5)'
    assert(_get_trino_columns_from_arrow_schema(schema)==expected_table_schema)
