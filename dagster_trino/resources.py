from typing import Mapping, Iterator, Optional, Any
from contextlib import closing, contextmanager
import sys

from dagster import resource
from dagster._core.storage.event_log.sql_event_log import SqlDbConnection
from dagster._annotations import public
import dagster._check as check

from .configs import define_trino_config

import pandas as pd

import trino

class TrinoConnection:
    """A connection to Trino that can execute queries. In general this class should not be
    directly instantiated, but rather used as a resource in an op or asset.
    """
    def __init__(self, config: Mapping[str, str], log):  # pylint: disable=too-many-locals
        # Extract parameters from resource config.

        auths_set = 0
        auths_set += 1 if config.get("password", None) is not None else 0

        check.invariant(
            auths_set > 0,
            ("Missing config: Password required for Trino resource.")
        )

        auth = trino.auth.BasicAuthentication(config['user'], config['password'])

        self.conn_args = {
            k: config.get(k)
            for k in (
                "host",
                "user",
                "auth",
                "port",
                "http_scheme",
                "catalog",
                "schema"
            )
            if config.get(k) is not None
        }
        self.conn_args['auth'] = auth

        self.log = log

    @public
    @contextmanager
    def get_connection(self) -> Iterator[trino.dbapi.Connection]:
        """Gets a connection to Trino as a context manager."""
        conn = trino.dbapi.connect(**self.conn_args)
        yield conn
        conn.close()

    @public
    def execute_query(
        self,
        sql: str,
        parameters: Optional[Mapping[Any, Any]] = None,
        fetch_results: bool = False,
        return_pandas: bool = False
    ):
        """Execute a query in Trino.
        Args:
            sql (str): the query to be executed
            parameters (Optional[Mapping[Any, Any]]): Parameters to be passed to the query. 
            fetch_results (bool): If True, will return the result of the query. Defaults to False
        Returns:
            The result of the query if fetch_results or use_pandas_result is True, otherwise returns None
        Examples:
            .. code-block:: python
                @op(required_resource_keys={"trino"})
                def drop_database(context):
                    context.resources.trino.execute_query(
                        "DROP TABLE IF EXISTS MY_TABLE"
                    )
        """
        check.str_param(sql, "sql")
        check.opt_inst_param(parameters, "parameters", (dict))
        check.bool_param(fetch_results, "fetch_results")

        with self.get_connection() as conn:
            with closing(conn.cursor()) as cursor:
                if sys.version_info[0] < 3:
                    sql = sql.encode("utf-8")
                self.log.info("Executing query: " + sql)
                parameters = dict(parameters) if isinstance(parameters, Mapping) else parameters
                cursor.execute(sql, parameters)
                if fetch_results:
                    result = cursor.fetchall()
                    if return_pandas:
                        return pd.DataFrame(result, columns=[i[0] for i in cursor.description])
                    else:
                        return result


@resource(
    config_schema=define_trino_config(),
    description="This resource is for connecting to a Trino Cluster",
)
def trino_resource(context):
    """#FIXME DOCS"""
    return TrinoConnection(context.resource_config, context.log)

def _filter_password(args):
    """Remove password from connection args for logging."""
    return {k: v for k, v in args.items() if k != "password"}