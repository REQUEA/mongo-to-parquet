"""MongoDB extraction: connect, list databases/collections, stream documents."""
from __future__ import annotations

from typing import Iterator, List, Optional

import structlog
from pymongo import MongoClient, ReadPreference
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential

log = structlog.get_logger()

_READ_PREFS = {
    "primary": ReadPreference.PRIMARY,
    "primaryPreferred": ReadPreference.PRIMARY_PREFERRED,
    "secondary": ReadPreference.SECONDARY,
    "secondaryPreferred": ReadPreference.SECONDARY_PREFERRED,
    "nearest": ReadPreference.NEAREST,
}

# MongoDB system databases that should never be exported
_SYSTEM_DBS = frozenset({"admin", "config", "local"})


class MongoExtractor:
    """Thin wrapper around pymongo for streaming-safe MongoDB extraction."""

    def __init__(
        self,
        uri: str,
        connection_timeout_ms: int = 30_000,
        socket_timeout_ms: int = 60_000,
        read_preference: str = "secondaryPreferred",
        max_pool_size: int = 10,
    ) -> None:
        self.uri = uri
        self.connection_timeout_ms = connection_timeout_ms
        self.socket_timeout_ms = socket_timeout_ms
        self.read_preference = read_preference
        self.max_pool_size = max_pool_size
        self.client: Optional[MongoClient] = None
        self.log = log.bind(component="extractor")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True,
    )
    def connect(self) -> None:
        """Establish and verify a MongoDB connection (retries up to 3×)."""
        try:
            self.client = MongoClient(
                self.uri,
                replicaSet="rs0",
                serverSelectionTimeoutMS=self.connection_timeout_ms,
                socketTimeoutMS=self.socket_timeout_ms,
                maxPoolSize=self.max_pool_size,
                readPreference = self.read_preference,
                #readPreference=_READ_PREFS.get(
                #    self.read_preference, ReadPreference.SECONDARY_PREFERRED
                #),
            )
            self.client.admin.command("ping")
            self.log.info("mongodb_connected", uri=self.uri)
        except ServerSelectionTimeoutError as exc:
            self.log.error("mongodb_timeout", error=str(exc))
            raise
        except Exception as exc:
            self.log.error("mongodb_connect_error", error=str(exc))
            raise

    def close(self) -> None:
        """Close the MongoDB connection (idempotent)."""
        if self.client:
            self.client.close()
            self.log.info("mongodb_disconnected")

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def list_databases(
        self,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> List[str]:
        """Return database names, excluding system DBs and any *exclude* entries.

        When *include* is given only those databases (that exist and are not
        excluded) are returned; otherwise all non-system databases are returned.
        """
        assert self.client is not None, "Call connect() before list_databases()."
        all_dbs = [
            d
            for d in self.client.list_database_names()
            if d not in _SYSTEM_DBS
        ]
        excluded = set(exclude or [])
        if include:
            dbs = [d for d in include if d in all_dbs and d not in excluded]
        else:
            dbs = [d for d in all_dbs if d not in excluded]
        self.log.info("databases_selected", count=len(dbs), databases=dbs)
        return dbs

    def list_collections(
        self,
        database: str,
        include: Optional[List[str]] = None,
        exclude: Optional[List[str]] = None,
    ) -> List[str]:
        """Return collection names for *database*, optionally filtered."""
        assert self.client is not None, "Call connect() before list_collections()."
        all_cols = self.client[database].list_collection_names()
        excluded = set(exclude or [])
        if include:
            cols = [c for c in include if c in all_cols and c not in excluded]
        else:
            cols = [c for c in all_cols if c not in excluded]
        self.log.info("collections_selected", database=database, count=len(cols))
        return cols

    def check_date_index(
        self, database: str, collection: str, date_field: str
    ) -> bool:
        """Return True if *date_field* is indexed; log a WARNING otherwise."""
        assert self.client is not None
        try:
            info = self.client[database][collection].index_information()
            indexed = any(
                date_field in {k for k, _ in idx["key"]}
                for idx in info.values()
            )
            if not indexed:
                self.log.warning(
                    "date_field_not_indexed",
                    database=database,
                    collection=collection,
                    date_field=date_field,
                    hint="Create an index to avoid full-collection scans.",
                )
            return indexed
        except OperationFailure as exc:
            self.log.warning("index_check_failed", error=str(exc))
            return False

    # ------------------------------------------------------------------
    # Streaming
    # ------------------------------------------------------------------

    def stream(
        self,
        database: str,
        collection: str,
        query: dict,
        batch_size: int = 1000,
    ) -> Iterator[dict]:
        """Yield documents from *collection* without loading all into memory.

        Always uses a cursor with explicit *batch_size*; never calls
        ``to_list()`` or accumulates the full result set.
        """
        assert self.client is not None, "Call connect() before stream()."
        coll = self.client[database][collection]
        cursor = coll.find(query)
        cursor.batch_size(batch_size)
        self.log.info(
            "stream_started",
            database=database,
            collection=collection,
            batch_size=batch_size,
        )
        yield from cursor
