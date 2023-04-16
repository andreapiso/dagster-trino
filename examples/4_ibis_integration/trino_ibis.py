import pandas as pd

from dagster import asset
import ibis

table_name = '<TABLE_NAME>'

@asset(required_resource_keys={"trino"})
def trino_ibis_table(context):
    '''
    Create a Dagster from an ibis dataframe connecting to 
    a trino backend.
    '''
    ibis.options.interactive = True
    with context.resources.trino.get_connection() as ibis_connection:
        # log the list of Trino tables obtained by ibis
        context.log.info(ibis_connection.list_tables())
        my_table = ibis_connection.table(table_name).head(5)
    return my_table
