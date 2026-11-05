import os
import re
from pathlib import Path

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from src.database.connection import get_engine


PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")


def create_test_database_if_needed(test_db_name):
    if not re.fullmatch(
        r"[A-Za-z0-9_]+",
        test_db_name,
    ):
        raise RuntimeError(
            "Invalid test database name."
        )

    admin_url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT")),
        database="postgres",
    )

    admin_engine = create_engine(
        admin_url,
        isolation_level="AUTOCOMMIT", 
    )

    with admin_engine.connect() as connection:
        exists = connection.execute(
            text(
                    """
                    SELECT 1
                    FROM pg_database
                    WHERE datname = :database_name
                    """
                ),
                {
                    "database_name": test_db_name,
                },
        ).scalar()

        if exists is None:
            connection.exec_driver_sql(
                f'CREATE DATABASE "{test_db_name}"'
            )

    admin_engine.dispose()


def rebuild_test_schema():
    engine = get_engine()

    with engine.connect() as connection:
        connection.execution_options(
            isolation_level="AUTOCOMMIT",
        )

        connection.exec_driver_sql(
            "DROP SCHEMA IF EXISTS ops CASCADE"
        )

        connection.exec_driver_sql(
            "DROP SCHEMA IF EXISTS staging CASCADE"
        )

        sql_files = [
            PROJECT_ROOT / "sql" / "create_staging.sql",
            PROJECT_ROOT / "sql" / "004_pipeline_runs.sql",
            PROJECT_ROOT / "sql" / "005_pipeline_run_attempts.sql",
        ]

        for sql_file in sql_files:
            connection.exec_driver_sql(
                sql_file.read_text(
                    encoding="utf-8"
                )
            )

    engine.dispose()


@pytest.fixture(
    scope="session",
    autouse=True,
)
def use_isolated_test_database():
    test_db_name = os.getenv(
        "POSTGRES_TEST_DB",
        "sports_warehouse_test",
    )

    if not test_db_name.endswith("_test"):
        raise RuntimeError(
            "Refusing to run tests because "
            "POSTGRES_TEST_DB must end in '_test'."
        )

    create_test_database_if_needed(
        test_db_name
    )

    original_database = os.getenv(
        "POSTGRES_DB"
    )

    os.environ["POSTGRES_DB"] = (
        test_db_name
    )

    rebuild_test_schema()

    yield

    if original_database is not None:
        os.environ["POSTGRES_DB"] = (
            original_database
        )
        

