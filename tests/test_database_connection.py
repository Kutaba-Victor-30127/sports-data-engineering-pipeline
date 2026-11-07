from src.database.connection import (
    dispose_engines,
    get_engine
)


def test_get_engine_reuses_engine():
    dispose_engines()

    first_engine = get_engine()
    second_engine = get_engine()

    assert first_engine is second_engine

    dispose_engines()


def test_get_engine_changes_with_database(
    monkeypatch,
):
    dispose_engines()

    monkeypatch.setenv(
        "POSTGRES_DB",
        "sports_warehouse_test",
    )

    first_engine = get_engine()

    monkeypatch.setenv(
        "POSTGRES_DB",
        "another_warehouse_test",
    )

    second_engine = get_engine()

    assert first_engine is not second_engine

    dispose_engines()