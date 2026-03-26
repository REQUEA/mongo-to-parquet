"""Unit tests for MariaDBExtractor (mocked pymysql)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch, PropertyMock
import pytest
import pyarrow as pa
import pymysql.constants.FIELD_TYPE as FT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def extractor():
    from mongodb_to_parquet.mariadb_extractor import MariaDBExtractor
    ext = MariaDBExtractor(host="localhost", user="root", password="test")
    ext.connection = MagicMock()
    ext.connection.open = True
    return ext


# ---------------------------------------------------------------------------
# Tests — Connection
# ---------------------------------------------------------------------------

class TestMariaDBConnect:
    def test_connect_success(self):
        from mongodb_to_parquet.mariadb_extractor import MariaDBExtractor
        ext = MariaDBExtractor(host="localhost", user="root", password="")

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("mongodb_to_parquet.mariadb_extractor.pymysql.connect", return_value=mock_conn):
            ext.connect()

        assert ext.connection is mock_conn
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_connect_retries_on_failure(self):
        from mongodb_to_parquet.mariadb_extractor import MariaDBExtractor
        ext = MariaDBExtractor(host="localhost", user="root", password="")

        with patch("mongodb_to_parquet.mariadb_extractor.pymysql.connect",
                    side_effect=pymysql.err.OperationalError("connection refused")):
            with pytest.raises(pymysql.err.OperationalError):
                ext.connect()


class TestMariaDBClose:
    def test_close_idempotent(self, extractor):
        extractor.close()
        extractor.connection.close.assert_called_once()

    def test_close_when_no_connection(self):
        from mongodb_to_parquet.mariadb_extractor import MariaDBExtractor
        ext = MariaDBExtractor(host="localhost", user="root", password="")
        ext.close()  # should not raise


# ---------------------------------------------------------------------------
# Tests — Discovery
# ---------------------------------------------------------------------------

class TestListDatabases:
    def test_excludes_system_dbs(self, extractor):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"Database": "mydb"},
            {"Database": "information_schema"},
            {"Database": "mysql"},
            {"Database": "performance_schema"},
            {"Database": "sys"},
            {"Database": "app_db"},
        ]
        extractor.connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        extractor.connection.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = extractor.list_databases()
        assert result == ["mydb", "app_db"]

    def test_include_filter(self, extractor):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"Database": "mydb"},
            {"Database": "other"},
        ]
        extractor.connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        extractor.connection.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = extractor.list_databases(include=["mydb"])
        assert result == ["mydb"]


class TestListTables:
    def test_lists_tables(self, extractor):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"Tables_in_mydb": "users"},
            {"Tables_in_mydb": "orders"},
        ]
        extractor.connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        extractor.connection.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = extractor.list_tables("mydb")
        assert result == ["users", "orders"]

    def test_include_filter(self, extractor):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {"Tables_in_mydb": "users"},
            {"Tables_in_mydb": "orders"},
        ]
        extractor.connection.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        extractor.connection.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = extractor.list_tables("mydb", include=["users"])
        assert result == ["users"]


# ---------------------------------------------------------------------------
# Tests — Query building
# ---------------------------------------------------------------------------

class TestBuildQuery:
    def test_no_filters(self):
        from mongodb_to_parquet.mariadb_extractor import MariaDBExtractor
        sql, params = MariaDBExtractor._build_query("db", "tbl", None, None, None)
        assert sql == "SELECT * FROM `db`.`tbl`"
        assert params == []

    def test_date_range(self):
        from mongodb_to_parquet.mariadb_extractor import MariaDBExtractor
        sql, params = MariaDBExtractor._build_query("db", "tbl", "created_at", "2024-01-01", "2024-12-31")
        assert "`created_at` >= %s" in sql
        assert "`created_at` <= %s" in sql
        assert "ORDER BY `created_at`" in sql
        assert params == ["2024-01-01", "2024-12-31"]

    def test_only_start_date(self):
        from mongodb_to_parquet.mariadb_extractor import MariaDBExtractor
        sql, params = MariaDBExtractor._build_query("db", "tbl", "updated_at", "2024-06-01", None)
        assert "`updated_at` >= %s" in sql
        assert "<=" not in sql
        assert params == ["2024-06-01"]


# ---------------------------------------------------------------------------
# Tests — Streaming
# ---------------------------------------------------------------------------

class TestStream:
    def test_yields_arrow_tables(self, extractor):
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("id", FT.LONG, None, None, None, None, None),
            ("name", FT.VARCHAR, None, None, None, None, None),
        ]
        mock_cursor.fetchmany.side_effect = [
            [(1, "Alice"), (2, "Bob")],
            [],  # end
        ]
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("mongodb_to_parquet.mariadb_extractor.pymysql.connect", return_value=mock_conn):
            batches = list(extractor.stream("mydb", "users", batch_size=10))

        assert len(batches) == 1
        assert isinstance(batches[0], pa.Table)
        assert batches[0].num_rows == 2
        assert batches[0].column_names == ["id", "name"]
        assert batches[0].column("id").to_pylist() == [1, 2]

    def test_multiple_batches(self, extractor):
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("id", FT.LONG, None, None, None, None, None),
        ]
        mock_cursor.fetchmany.side_effect = [
            [(1,), (2,)],
            [(3,)],
            [],
        ]
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("mongodb_to_parquet.mariadb_extractor.pymysql.connect", return_value=mock_conn):
            batches = list(extractor.stream("mydb", "users", batch_size=2))

        assert len(batches) == 2
        assert batches[0].num_rows == 2
        assert batches[1].num_rows == 1


import pymysql.err
