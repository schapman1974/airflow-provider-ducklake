from types import SimpleNamespace

import pytest

from ducklake_provider.hooks import ducklake_hook
from ducklake_provider.hooks.ducklake_hook import DuckLakeHook


def _option_row(name, value):
    return (name, f"description for {name}", value, "GLOBAL", None)


class FakeDuckDBConnection:
    def __init__(self, option_rows=None):
        self.option_rows = option_rows or []
        self.executed = []
        self.installed_extensions = []
        self.loaded_extensions = []
        self.closed = False
        self._last_result = []

    def execute(self, query):
        self.executed.append(query)
        if query.strip() == "FROM ducklake_options()":
            self._last_result = self.option_rows
        else:
            self._last_result = []
        return self

    def fetchall(self):
        return self._last_result

    def install_extension(self, extension):
        self.installed_extensions.append(extension)

    def load_extension(self, extension):
        self.loaded_extensions.append(extension)

    def close(self):
        self.closed = True


@pytest.fixture
def airflow_connection():
    return SimpleNamespace(
        host="postgres.internal",
        login="ducklake_user",
        password="ducklake_password",
        schema="demomart_catalog",
        extra_dejson={
            "engine": "postgres",
            "dbname": "cobdata",
            "pgdbname": "demomart",
            "storage_type": "s3",
            "s3_bucket": "arul-mdlx-dev",
            "s3_path": "arul_mdlx_cobdata/",
        },
    )


def test_get_conn_uses_encrypted_attach_and_sets_missing_defaults(monkeypatch, airflow_connection):
    fake_conn = FakeDuckDBConnection(
        option_rows=[
            _option_row("created_by", "DuckDB test"),
            _option_row("data_path", "s3://arul-mdlx-dev/arul_mdlx_cobdata/"),
            _option_row("encrypted", "true"),
        ]
    )
    monkeypatch.setattr(ducklake_hook.duckdb, "connect", lambda: fake_conn)
    monkeypatch.setattr(ducklake_hook, "_determine_memory_limit_bytes", lambda _plan: 1024**3)
    monkeypatch.setattr(DuckLakeHook, "get_connection", lambda self, conn_id: airflow_connection)

    hook = DuckLakeHook(ducklake_conn_id="ducklake_test")
    conn = hook.get_conn()

    attach_statement = next(query for query in fake_conn.executed if query.startswith("ATTACH "))
    assert "ENCRYPTED" in attach_statement
    assert "METADATA_SCHEMA 'demomart_catalog'" in attach_statement

    assert (
        "CALL cobdata.set_option('expire_older_than', '1 day');" in fake_conn.executed
    )
    assert (
        "CALL cobdata.set_option('delete_older_than', '1 day');" in fake_conn.executed
    )
    assert "CALL cobdata.set_option('parquet_version', 2);" in fake_conn.executed
    assert conn is fake_conn


def test_get_conn_preserves_existing_ducklake_options(monkeypatch, airflow_connection):
    airflow_connection.extra_dejson.update(
        {
            "expire_older_than": "7 days",
            "delete_older_than": "14 days",
            "parquet_version": 1,
            "encrypted": True,
        }
    )
    fake_conn = FakeDuckDBConnection(
        option_rows=[
            _option_row("created_by", "DuckDB test"),
            _option_row("data_path", "s3://arul-mdlx-dev/arul_mdlx_cobdata/"),
            _option_row("encrypted", "true"),
            _option_row("expire_older_than", "1 day"),
            _option_row("delete_older_than", "1 day"),
            _option_row("parquet_version", "V2"),
        ]
    )
    monkeypatch.setattr(ducklake_hook.duckdb, "connect", lambda: fake_conn)
    monkeypatch.setattr(ducklake_hook, "_determine_memory_limit_bytes", lambda _plan: 1024**3)
    monkeypatch.setattr(DuckLakeHook, "get_connection", lambda self, conn_id: airflow_connection)

    hook = DuckLakeHook(ducklake_conn_id="ducklake_test")
    hook.get_conn()

    assert not any("set_option('expire_older_than'" in query for query in fake_conn.executed)
    assert not any("set_option('delete_older_than'" in query for query in fake_conn.executed)
    assert not any("set_option('parquet_version'" in query for query in fake_conn.executed)


def test_fetch_ducklake_options_raises_when_all_queries_fail():
    class FailingConnection:
        def execute(self, query):
            raise RuntimeError(f"boom: {query}")

    with pytest.raises(ducklake_hook.AirflowException, match="Unable to read current DuckLake options"):
        ducklake_hook._fetch_ducklake_options(FailingConnection(), "cobdata")
