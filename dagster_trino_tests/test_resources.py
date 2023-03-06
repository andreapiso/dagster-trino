from unittest import mock

import pytest
from dagster import DagsterResourceFunctionError, job, op
from dagster._core.test_utils import environ
from dagster_trino import trino_resource

from trino.auth import BasicAuthentication

from .utils import create_mock_connector

@mock.patch("trino.dbapi.connect", new_callable=create_mock_connector)
def test_trino_resource(trino_connect):
    @op(required_resource_keys={"trino"})
    def trino_op(context):
        assert context.resources.trino
        with context.resources.trino.get_connection() as _:
            pass

    @job(resource_defs={"trino": trino_resource})
    def trino_job():
        trino_op()

    result = trino_job.execute_in_process(
        run_config={
            "resources": {
                "trino": {
                    "config": {
                        "host": "foo",
                        "user": "bar",
                        "password": "baz",
                        "catalog": "TESTCATALOG",
                        "schema": "TESTSCHEMA",
                    }
                }
            }
        }
    )
    assert result.success
    trino_connect.assert_called_once_with(
        host='foo',
        user='bar',
        auth = BasicAuthentication ('bar','baz'),
        catalog='TESTCATALOG',
        schema='TESTSCHEMA'
    )

@mock.patch("trino.dbapi.connect", new_callable=create_mock_connector)
def test_trino_resource_from_envvars(trino_connect):
    @op(required_resource_keys={"trino"})
    def trino_op(context):
        assert context.resources.trino
        with context.resources.trino.get_connection() as _:
            pass
    @job(resource_defs={"trino": trino_resource})
    def trino_job():
        trino_op()

    env_vars = {
        "TRINO_HOST": "foo",
        "TRINO_USER": "bar",
        "TRINO_PWD": "baz",
        "TRINO_CATALOG": "TESTCATALOG",
        "TRINO_SCHEMA": "TESTSCHEMA",
    }

    with environ(env_vars):
        result = trino_job.execute_in_process(
            run_config={
                "resources": {
                    "trino": {
                        "config": {
                            "host": {"env": "TRINO_HOST"},
                            "user": {"env": "TRINO_USER"},
                            "password": {"env": "TRINO_PWD"},
                            "catalog": {"env": "TRINO_CATALOG"},
                            "schema": {"env": "TRINO_SCHEMA"},
                        }
                    }
                }
            },
        )
        assert result.success
        trino_connect.assert_called_once_with(
            host='foo',
            user='bar',
            auth = BasicAuthentication ('bar','baz'),
            catalog='TESTCATALOG',
            schema='TESTSCHEMA'
        )


