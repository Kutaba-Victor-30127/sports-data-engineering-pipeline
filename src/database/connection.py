import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL


PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(PROJECT_ROOT / ".env")


_ENGINES: dict[tuple, Engine] = {}


def get_connection_config():
    return(
        os.getenv("POSTGRES_USER"),
        os.getenv("POSTGRES_PASSWORD"),
        os.getenv("POSTGRES_HOST"),
        int(os.getenv("POSTGRES_PORT")),
        os.getenv("POSTGRES_DB"),
    )


def get_engine():
    config = get_connection_config()

    if config not in _ENGINES:
        (
            username,
            password,
            host,
            port,
            database,
        ) = config

        database_url = URL.create(
            drivername="postgresql+psycopg2",
            username=username,
            password=password,
            host=host,
            port=port,
            database=database,
        )

        _ENGINES[config] = create_engine(
            database_url,
            pool_pre_ping=True,
        )

    return _ENGINES[config]


def dispose_engines():
    for engine in _ENGINES.values():
        engine.dispose()

    _ENGINES.clear()