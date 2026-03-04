"""Utilities: date parsing, path building, logging setup."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import structlog

_DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S",
]


def setup_logging(level: str = "INFO", fmt: str = "text") -> None:
    """Configure structlog. Call once at process startup."""
    import logging

    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    if fmt == "json":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def parse_date(date_str: str) -> datetime:
    """Parse an ISO-like date string into a timezone-aware datetime (UTC).

    Raises ``ValueError`` if the string matches none of the supported formats.
    """
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except ValueError:
            continue
    raise ValueError(
        f"Cannot parse date {date_str!r}. "
        "Expected ISO 8601 format, e.g. '2024-01-15' or '2024-01-15T00:00:00'."
    )


def build_partition_path(
    base: Path,
    database: str,
    collection: str,
    date: Optional[datetime] = None,
    pattern: Optional[str] = None,
) -> Path:
    """Return the directory that should hold a partition's Parquet files.

    Default layout: ``{base}/{database}/{collection}/year={Y}/month={MM}/day={DD}``

    Pass *pattern* to override, using any subset of the available variables:
    ``{database}``, ``{collection}``, ``{year}``, ``{month}``, ``{day}``, ``{hour}``.
    """
    if date is None:
        date = datetime.now(timezone.utc)

    variables = {
        "database": database,
        "collection": collection,
        "year": str(date.year),
        "month": f"{date.month:02d}",
        "day": f"{date.day:02d}",
        "hour": f"{date.hour:02d}",
    }

    if pattern:
        return base / pattern.format(**variables)

    return (
        base
        / database
        / collection
        / f"year={date.year}"
        / f"month={date.month:02d}"
        / f"day={date.day:02d}"
    )


def format_bytes(n: int) -> str:
    """Return a human-readable representation of *n* bytes."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n //= 1024
    return f"{n:.1f} PB"
