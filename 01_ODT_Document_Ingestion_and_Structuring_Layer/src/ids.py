"""ids.py — Identifier and timestamp derivation for the ODT V7.5 pipeline.

Responsible for:
- UTC timestamp generation and validation
- SYSTEM_GLOBAL_ID, document_id, and timestamp derivation from an ODT file
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from src.utils import sha256_file

DEFAULT_REPRODUCIBLE_TIMESTAMP = "2000-01-01T00:00:00Z"


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string with Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_timestamp(value: str) -> str:
    """Validate and return a UTC ISO 8601 timestamp (YYYY-MM-DDTHH:MM:SSZ).

    Raises ValueError if the format does not match exactly.
    """
    if not re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", value):
        raise ValueError(f"Timestamp must be UTC ISO 8601 with Z suffix, got: {value}")
    return value


def build_ids(
    odt_path: Path,
    reproducible: bool = False,
    fixed_timestamp: str | None = None,
    system_global_id_override: str | None = None,
):
    """Derive the three core identifiers for a pipeline run.

    Returns (system_global_id, document_id, timestamp).
    In reproducible mode the timestamp is fixed and the SYSTEM_GLOBAL_ID is
    deterministic; otherwise both are derived from the current wall clock.
    """
    raw = sha256_file(odt_path)
    document_id = f"DOC_{raw[12:28].upper()}"
    if system_global_id_override:
        system_global_id = system_global_id_override
    elif reproducible:
        system_global_id = f"SYSTEM_GLOBAL_ID__REPRO__{raw[:12].upper()}"
    else:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        system_global_id = f"SYSTEM_GLOBAL_ID__{ts}__{raw[:12].upper()}"
    if reproducible:
        timestamp = normalize_timestamp(fixed_timestamp or DEFAULT_REPRODUCIBLE_TIMESTAMP)
    else:
        timestamp = utc_now_iso()
    return system_global_id, document_id, timestamp
