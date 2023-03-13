from typing import NewType, List
from dagster import usable_as_dagster_type

TrinoQuery = str
TableFilePaths = List[str]

trino_string = "VARCHAR"
trino_bool = "BOOLEAN"
trino_tinyint = "TINYINT"
trino_smallint = "SMALLINT"
trino_int = "INTEGER"
trino_bigint = "BIGINT"
trino_float = "REAL"
trino_double = "DOUBLE"
trino_decimal = "DECIMAL"

arrow_string= "string"
arrow_bool = "bool"
arrow_tinyint = "int8"
arrow_smallint = "int16"
arrow_int = "int32"
arrow_bigint = "int64"
arrow_float = "float"
arrow_double="double"
arrow_decimal="decimal128"

