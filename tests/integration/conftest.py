
from os import environ

from logging import getLogger
from pytest import fixture

from sqlalchemy import create_engine
from sqlalchemy.dialects import registry

import firebolt as firebolt_sdk

LOGGER = getLogger(__name__)

ENGINE_NAME_ENV = "engine_name"
DATABASE_NAME_ENV = "database_name"
USERNAME_ENV = "username"
PASSWORD_ENV = "password"


def must_env(var_name: str) -> str:
    assert var_name in environ, f"Expected {var_name} to be provided in environment"
    LOGGER.info(f"{var_name}: {environ[var_name]}")
    return environ[var_name]


@fixture(scope="session")
def engine_name() -> str:
    return must_env(ENGINE_NAME_ENV)


@fixture(scope="session")
def database_name() -> str:
    return must_env(DATABASE_NAME_ENV)


@fixture(scope="session")
def username() -> str:
    return must_env(USERNAME_ENV)


@fixture(scope="session")
def password() -> str:
    return must_env(PASSWORD_ENV)


@fixture(scope="session")
def engine(username, password, database_name, engine_name):
    registry.register(
        "firebolt", "src.firebolt_db.firebolt_dialect", "FireboltDialect")
    return create_engine(
        f"firebolt://{username}:{password}@{database_name}/{engine_name}"
    )


@fixture(scope="session")
def connection(engine):
    engine = engine
    if hasattr(firebolt_sdk.db.connection.Connection, "commit"):
        return engine.connect()
    else:
        # Disabling autocommit allows for table creation/destruction without
        # trying to call non-existing parameters
        return engine.connect().execution_options(autocommit=False)
