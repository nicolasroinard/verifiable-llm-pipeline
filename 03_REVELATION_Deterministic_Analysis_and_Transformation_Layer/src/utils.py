"""
Utility functions for REVELATION V8.2 pipeline.

Provides:
- JSON I/O with deterministic formatting
- Cryptographic hashing for integrity verification
- File/directory manipulation
- Timestamp and validation helpers
- Error handling (ProtocolError exception)
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Regex pattern for ISO 8601 UTC timestamps
ISO_UTC_RE: re.Pattern[str] = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

# Allowed META layer status values
ALLOWED_META_STATUS: set[str] = {"VALID", "DEGRADED"}

# Allowed REVELATION operation types (closed set, protocol mandated)
ALLOWED_REVELATION_OPERATION_TYPES: set[str] = {
    "EXTRACT",
    "GROUP",
    "LINK",
    "COMPARE",
    "COUNT",
    "CONFLICT_DETECT",
}

# Allowed REVELATION unit status values
ALLOWED_UNIT_STATUS: set[str] = {"VALID", "INCOMPLETE"}

# Required input artifacts for REVELATION (contract)
REQUIRED_INPUT_FILES: list[str] = [
    "META_GLOBAL_GRAPH.json",
    "GLOBAL_DOCUMENT_CORPUS.json",
    "GLOBAL_SEMANTIC_INDEX.json",
    "META_LAYER_STATUS.json",
    "CHAIN_INTEGRITY_MANIFEST.json",
]


class ProtocolError(RuntimeError):
    """
    Exception raised when REVELATION protocol constraints are violated.
    
    Used to distinguish protocol violations from other runtime errors.
    Results in REJECTED status rather than crash.
    """
    pass


def ensure(condition: bool, message: str) -> None:
    """
    Assert a protocol condition; raise ProtocolError if false.
    
    Args:
        condition: Boolean condition to check
        message: Error message if condition fails
    
    Raises:
        ProtocolError: if condition is False
    """
    if not condition:
        raise ProtocolError(message)


def ensure_dir(path: Path) -> None:
    """
    Create directory and all parents if not exist.
    
    Args:
        path: Path to directory
    """
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> Any:
    """
    Read and parse JSON file.
    
    Args:
        path: Path to JSON file
    
    Returns:
        Parsed JSON object
    
    Raises:
        FileNotFoundError: if file doesn't exist
        json.JSONDecodeError: if JSON is malformed
    """
    try:
        with path.open('r', encoding='utf-8') as handle:
            return json.load(handle)
    except FileNotFoundError:
        raise ProtocolError(f'Required artifact not found: {path}')
    except json.JSONDecodeError as exc:
        raise ProtocolError(f'Malformed JSON in {path.name}: {exc.msg} at line {exc.lineno}')
    except (IOError, OSError) as exc:
        raise ProtocolError(f'I/O error reading {path}: {exc}')


def write_json(path: Path, payload: Any) -> None:
    """
    Write object to JSON file with deterministic formatting.
    
    Ensures:
    - Sorted keys (for reproducibility)
    - Proper escaping (no raw unicode)
    - Consistent indentation (2 spaces)
    
    Args:
        path: Path to output JSON file
        payload: Object to serialize
    
    Raises:
        ProtocolError: if write fails or JSON serialization fails
    """
    try:
        ensure_dir(path.parent)
        with path.open('w', encoding='utf-8') as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False, sort_keys=True)
    except TypeError as exc:
        raise ProtocolError(f'JSON serialization error for {path.name}: {exc}')
    except (IOError, OSError) as exc:
        raise ProtocolError(f'I/O error writing {path}: {exc}')


def copy_tree(src: Path, dst: Path) -> None:
    """
    Recursively copy directory tree, replacing destination if exists.
    
    Args:
        src: Source directory
        dst: Destination directory
    
    Raises:
        ProtocolError: if source doesn't exist or copy fails
    """
    try:
        if not src.exists():
            raise ProtocolError(f'Source directory not found: {src}')
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)
    except ProtocolError:
        raise
    except (IOError, OSError) as exc:
        raise ProtocolError(f'Error copying {src} to {dst}: {exc}')


def sha256_bytes(data: bytes) -> str:
    """
    Compute SHA-256 hash of bytes.
    
    Args:
        data: Bytes to hash
    
    Returns:
        Hex-encoded SHA-256 digest
    """
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def sha256_file(path: Path) -> str:
    """
    Compute SHA-256 hash of file (streaming for large files).
    
    Streams 1MB chunks to avoid loading large files into memory.
    
    Args:
        path: Path to file
    
    Returns:
        Hex-encoded SHA-256 digest
    
    Raises:
        ProtocolError: if file doesn't exist or can't be read
    """
    try:
        if not path.exists():
            raise ProtocolError(f'File not found for hashing: {path}')
        digest = hashlib.sha256()
        with path.open('rb') as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                digest.update(chunk)
        return digest.hexdigest()
    except ProtocolError:
        raise
    except (IOError, OSError) as exc:
        raise ProtocolError(f'I/O error hashing {path}: {exc}')


def stable(value: Any) -> str:
    """
    Convert value to stable string representation (for sorting/hashing).
    
    Args:
        value: Value to stringify
    
    Returns:
        String representation, empty string if None
    """
    if value is None:
        return ''
    if isinstance(value, (int, float)):
        return f"{value}"
    return str(value)


def current_utc_timestamp() -> str:
    """
    Get current UTC time as ISO 8601 string.
    
    Returns:
        ISO 8601 UTC timestamp (YYYY-MM-DDTHH:MM:SSZ)
    """
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def validate_iso_utc(value: str | None) -> bool:
    """
    Check if string is valid ISO 8601 UTC timestamp.
    
    Args:
        value: String to validate
    
    Returns:
        True if valid ISO UTC format
    """
    return bool(value) and bool(ISO_UTC_RE.match(value))


def normalize_timestamp(value: str | None) -> str:
    """
    Normalize timestamp: use provided value if valid, else current UTC time.
    
    Args:
        value: Optional ISO 8601 UTC timestamp
    
    Returns:
        Valid ISO 8601 UTC timestamp
    
    Raises:
        ProtocolError: if value provided but invalid format
    """
    if value is None:
        return current_utc_timestamp()
    ensure(validate_iso_utc(value), f'Invalid UTC ISO 8601 timestamp: {value}')
    return value


def prefixed_refs(prefix: str, values: list[str]) -> list[str]:
    """
    Add typed prefix to reference strings for input_refs.
    
    Args:
        prefix: Prefix to add (e.g., 'node_id', 'segment_id', 'document_id')
        values: List of reference IDs
    
    Returns:
        List of prefixed references (e.g., ['node_id:X', 'node_id:Y'])
    """
    return [f'{prefix}:{value}' for value in values]


def all_files_recursive(root: Path) -> list[Path]:
    """
    Recursively list all files under root directory.
    
    Args:
        root: Root directory to search
    
    Returns:
        Sorted list of file paths
    """
    return sorted([path for path in root.rglob('*') if path.is_file()])
