from typing import NewType, List
from dagster import usable_as_dagster_type

TrinoQuery = str
TableFilePaths = List[str]

trino_string = "VARCHAR"

arrow_string= "string"

