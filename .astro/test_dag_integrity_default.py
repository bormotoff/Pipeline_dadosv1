from contextlib import contextmanager
import logging
import os

import pytest

from airflow.models import DagBag, Variable, Connection
from airflow.hooks.base import BaseHook
from airflow.utils.db import initdb

initdb()

def basehook_get_connection_monkeypatch(key: str, *args, **kwargs):
    print(
        f"Attempted to fetch connection during parse returning an empty Connection object for {key}"
    )
    return Connection(key)


BaseHook.get_connection = basehook_get_connection_monkeypatch

def os_getenv_monkeypatch(key: str, *args, **kwargs):
    default = None
    if args:
        default = args[0]  
        default = kwargs.get(
            "default", None
        )  

    env_value = os.environ.get(key, None)

    if env_value:
        return env_value  
    if (
        key == "JENKINS_HOME" and default is None
    ):  
        return None
    if default:
        return default 
    return f"MOCKED_{key.upper()}_VALUE"  


os.getenv = os_getenv_monkeypatch

class magic_dict(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)

    def __getitem__(self, key):
        return {}.get(key, "MOCKED_KEY_VALUE")


_no_default = object()  


def variable_get_monkeypatch(key: str, default_var=_no_default, deserialize_json=False):
    print(
        f"Attempted to get Variable value during parse, returning a mocked value for {key}"
    )

    if default_var is not _no_default:
        return default_var
    if deserialize_json:
        return magic_dict()
    return "NON_DEFAULT_MOCKED_VARIABLE_VALUE"


Variable.get = variable_get_monkeypatch

@contextmanager
def suppress_logging(namespace):
    """
    Suppress logging within a specific namespace to keep tests "clean" during build
    """
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag, and include DAGs without errors.
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        result = []

        for k, v in dag_bag.import_errors.items():
            result.append((strip_path_prefix(k), v.strip()))

        for file_path in dag_bag.dags:
            if file_path not in dag_bag.import_errors:
                result.append((strip_path_prefix(file_path), "No import errors"))

        return result


@pytest.mark.parametrize(
    "rel_path, rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rv != "No import errors":
        raise Exception(f"{rel_path} failed to import with message \n {rv}")
    else:
        print(f"{rel_path} passed the import test")
