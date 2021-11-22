from logging import getLogger
from os import environ

import firebolt as firebolt_sdk
from pytest import fixture
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.engine.base import Connection, Engine

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
def engine(
    username: str, password: str, database_name: str, engine_name: str
) -> Engine:
    registry.register("firebolt", "src.firebolt_db.firebolt_dialect", "FireboltDialect")
    return create_engine(
        f"firebolt://{username}:{password}@{database_name}/{engine_name}"
    )


@fixture(scope="session")
def connection(engine: Engine) -> Connection:
    if hasattr(firebolt_sdk.db.connection.Connection, "commit"):
        return engine.connect()
    else:
        # Disabling autocommit allows for table creation/destruction without
        # trying to call non-existing parameters
        return engine.connect().execution_options(autocommit=False)
