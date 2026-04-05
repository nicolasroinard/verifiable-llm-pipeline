from __future__ import annotations

import hashlib
import json
from pathlib import Path


def ensure_dir(path: Path) -> None:
    """Create *path* and all missing parents. No-op if it already exists."""
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path):
    """Read and parse a UTF-8 JSON file. Returns the decoded Python object."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data) -> None:
    """Serialise *data* to *path* as indented UTF-8 JSON. Creates parent dirs."""
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def write_text(path: Path, text: str) -> None:
    """Write *text* to *path* as UTF-8. Creates parent dirs."""
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def sha256_bytes(data: bytes) -> str:
    """Return the hex SHA-256 digest of *data*."""
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def sha256_file(path: Path) -> str:
    """Return the hex SHA-256 digest of the file at *path*, read in 8 KiB chunks."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
