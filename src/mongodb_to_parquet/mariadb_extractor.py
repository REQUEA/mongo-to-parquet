"""MariaDB extraction: connect, list databases/tables, stream rows as Arrow tables."""
from __future__ import annotations

from typing import Iterator, List, Optional

import pyarrow as pa
import pymysql
import pymysql.cursors
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

log = structlog.get_logger()

# System databases that should never be exported
_SYSTEM_DBS = frozenset({"information_schema", "mysql", "performance_schema", "sys"})

# MySQL field type codes → PyArrow types
_MYSQL_TYPE_MAP: dict[int, pa.DataType] = {
    pymysql.constants.FIELD_TYPE.TINY: pa.int8(),
    pymysql.constants.FIELD_TYPE.SHORT: pa.int16(),
    pymysql.constants.FIELD_TYPE.INT24: pa.int32(),
    pymysql.constants.FIELD_TYPE.LONG: pa.int32(),
    pymysql.constants.FIELD_TYPE.LONGLONG: pa.int64(),
    pymysql.constants.FIELD_TYPE.FLOAT: pa.float32(),
    pymysql.constants.FIELD_TYPE.DOUBLE: pa.float64(),
    pymysql.constants.FIELD_TYPE.DECIMAL: pa.string(),
    pymysql.constants.FIELD_TYPE.NEWDECIMAL: pa.string(),
    pymysql.constants.FIELD_TYPE.TIMESTAMP: pa.timestamp("us"),
    pymysql.constants.FIELD_TYPE.DATETIME: pa.timestamp("us"),
    pymysql.constants.FIELD_TYPE.DATE: pa.date32(),
    pymysql.constants.FIELD_TYPE.TIME: pa.string(),
    pymysql.constants.FIELD_TYPE.YEAR: pa.int16(),
    pymysql.constants.FIELD_TYPE.VARCHAR: pa.string(),
    pymysql.constants.FIELD_TYPE.VAR_STRING: pa.string(),
    pymysql.constants.FIELD_TYPE.STRING: pa.string(),
    pymysql.constants.FIELD_TYPE.BLOB: pa.large_binary(),
    pymysql.constants.FIELD_TYPE.TINY_BLOB: pa.binary(),
    pymysql.constants.FIELD_TYPE.MEDIUM_BLOB: pa.large_binary(),
    pymysql.constants.FIELD_TYPE.LONG_BLOB: pa.large_binary(),
    pymysql.constants.FIELD_TYPE.BIT: pa.bool_(),
    pymysql.constants.FIELD_TYPE.JSON: pa.string(),
    pymysql.constants.FIELD_TYPE.ENUM: pa.string(),
    pymysql.constants.FIELD_TYPE.SET: pa.string(),
}


class MariaDBExtractor:
    """Thin wrapper around pymysql for streaming-safe MariaDB extraction."""

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        port: int = 3306,
        connection_timeout: int = 30,
        read_timeout: int = 60,
    ) -> None:
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.connection_timeout = connection_timeout
        self.read_timeout = read_timeout
        self.connection: Optional[pymysql.Connection] = None
        self.log = log.bind(component="mariadb_extractor")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def connect(self) -> None:
        """Establish and verify a MariaDB connection (retries up to 3x)."""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                connect_timeout=self.connection_timeout,
                read_timeout=self.read_timeout,
                cursorclass=pymysql.cursors.DictCursor,
            )
            with self.connection.cursor() as cur:
                cur.execute("SELECT 1")
            self.log.info("mariadb_connected", host=self.host, port=self.port)
        except pymysql.err.OperationalError as exc:
            self.log.error("mariadb_connect_error", error=str(exc))
            raise
        except Exception as exc:
            self.log.error("mariadb_connect_error", error=str(exc))
            raise

    def close(self) -> None:
        """Close the MariaDB connection (idempotent)."""
        if self.connection and self.connection.open:
            self.connection.close()
            self.log.info("mariadb_disconnected")

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def list_databases(
        self,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> List[str]:
        """Return database names, excluding system DBs."""
        assert self.connection is not None, "Call connect() first."
        with self.connection.cursor() as cur:
            cur.execute("SHOW DATABASES")
            all_dbs = [row["Database"] for row in cur.fetchall()]

        all_dbs = [d for d in all_dbs if d not in _SYSTEM_DBS]
        excluded = set(exclude or [])
        if include:
            dbs = [d for d in include if d in all_dbs and d not in excluded]
        else:
            dbs = [d for d in all_dbs if d not in excluded]
        self.log.info("databases_selected", count=len(dbs), databases=dbs)
        return dbs

    def list_tables(
        self,
        database: str,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> List[str]:
        """Return table names for *database*, optionally filtered."""
        assert self.connection is not None, "Call connect() first."
        with self.connection.cursor() as cur:
            cur.execute("SHOW TABLES FROM `%s`" % database)
            key = f"Tables_in_{database}"
            all_tables = [row[key] for row in cur.fetchall()]

        excluded = set(exclude or [])
        if include:
            tables = [t for t in include if t in all_tables and t not in excluded]
        else:
            tables = [t for t in all_tables if t not in excluded]
        self.log.info("tables_selected", database=database, count=len(tables))
        return tables

    def check_date_index(
        self, database: str, table: str, date_field: str
    ) -> bool:
        """Return True if *date_field* is indexed; log a WARNING otherwise."""
        assert self.connection is not None
        try:
            with self.connection.cursor() as cur:
                cur.execute(
                    "SHOW INDEX FROM `%s`.`%s` WHERE Column_name = %%s"
                    % (database, table),
                    (date_field,),
                )
                indexed = cur.fetchone() is not None
            if not indexed:
                self.log.warning(
                    "date_field_not_indexed",
                    database=database,
                    table=table,
                    date_field=date_field,
                    hint="Create an index to avoid full-table scans.",
                )
            return indexed
        except pymysql.err.OperationalError as exc:
            self.log.warning("index_check_failed", error=str(exc))
            return False

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------

    def stream(
        self,
        database: str,
        table: str,
        date_field: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.Table]:
        """Yield Arrow tables from *table* using a server-side cursor.

        Each yielded table contains up to *batch_size* rows.
        """
        assert self.connection is not None, "Call connect() first."

        sql, params = self._build_query(database, table, date_field, start_date, end_date)

        # Use SSCursor (server-side) to avoid loading all rows into memory
        conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=database,
            connect_timeout=self.connection_timeout,
            read_timeout=self.read_timeout,
            cursorclass=pymysql.cursors.SSCursor,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)

                # Build Arrow schema from cursor description
                col_names = [desc[0] for desc in cur.description]
                arrow_types = [
                    _MYSQL_TYPE_MAP.get(desc[1], pa.string())
                    for desc in cur.description
                ]
                schema = pa.schema(list(zip(col_names, arrow_types)))

                self.log.info(
                    "stream_started",
                    database=database,
                    table=table,
                    batch_size=batch_size,
                    columns=len(col_names),
                )

                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break

                    # Convert rows (list of tuples) to columnar Arrow table
                    columns = list(zip(*rows)) if rows else []
                    arrays = []
                    for i, (col_type, col_data) in enumerate(zip(arrow_types, columns)):
                        try:
                            arrays.append(pa.array(col_data, type=col_type))
                        except (pa.ArrowInvalid, pa.ArrowTypeError):
                            # Fallback: let PyArrow infer the type
                            arrays.append(pa.array(col_data))

                    yield pa.table(
                        {name: arr for name, arr in zip(col_names, arrays)},
                    )
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_query(
        database: str,
        table: str,
        date_field: Optional[str],
        start_date: Optional[str],
        end_date: Optional[str],
    ) -> tuple[str, list]:
        """Build a parameterized SELECT with optional date range WHERE clause."""
        sql = f"SELECT * FROM `{database}`.`{table}`"
        params: list = []

        conditions = []
        if date_field and start_date:
            conditions.append(f"`{date_field}` >= %s")
            params.append(start_date)
        if date_field and end_date:
            conditions.append(f"`{date_field}` <= %s")
            params.append(end_date)

        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        if date_field:
            sql += f" ORDER BY `{date_field}`"

        return sql, params
