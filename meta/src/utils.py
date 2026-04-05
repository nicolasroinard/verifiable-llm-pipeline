#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import pathlib
import re
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable

TOKEN_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9][A-Za-zÀ-ÖØ-öø-ÿ0-9_\-']+")
ISO_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
STOPWORDS = {
    "le", "la", "les", "de", "des", "du", "un", "une", "et", "en", "à", "a", "au", "aux",
    "pour", "par", "sur", "dans", "que", "qui", "ne", "pas", "plus", "ou", "où", "ce", "cet",
    "cette", "ces", "il", "elle", "ils", "elles", "on", "je", "tu", "nous", "vous", "leur",
    "leurs", "se", "sa", "son", "ses", "est", "sont", "été", "être", "comme", "avec", "the",
    "and", "for", "with", "from", "that", "this", "are", "was", "were", "has", "have", "had", "not",
}

ACCEPT_MANIFEST_VERDICTS = {
    "PASS",
    "ARCHIVE_VALIDE",
    "ARCHIVE_VALIDE_AUDIT_PROOF",
    "VALID_COMPLETE_ARCHIVE",
    "VALID_COMPLETE_ARCHIVE_AUDIT_PROOF",
}
ACCEPT_CHAIN_STATUS = {"COMPLETE", "COMPLETE_AUDIT_PROOF", "PASS"}
ACCEPT_ODT_STATUS = {
    "PASS",
    "VALID",
    "ARCHIVE_VALIDE",
    "ARCHIVE_VALIDE_AUDIT_PROOF",
    "VALID_COMPLETE_ARCHIVE",
    "VALID_COMPLETE_ARCHIVE_AUDIT_PROOF",
}
DOCUMENT_NODE_SEGMENT_SENTINEL = "__NA_DOCUMENT_NODE__"


class ProtocolError(RuntimeError):
    """Raised when a protocol-critical contract is violated."""


def stable(value: Any) -> str:
    return "" if value is None else str(value)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: pathlib.Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def tokenize(text: str) -> list[str]:
    tokens = [token.lower() for token in TOKEN_RE.findall(text or "")]
    return [token for token in tokens if len(token) >= 3 and token not in STOPWORDS and not token.isdigit()]


def count_terms(text: str) -> Counter:
    return Counter(tokenize(text))


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise ProtocolError(message)


def sort_records(records: Iterable[dict], *keys: str) -> list[dict]:
    return sorted(records, key=lambda item: tuple(stable(item.get(key)) for key in keys))


def validate_iso_utc(value: str | None) -> bool:
    return bool(value) and ISO_UTC_RE.match(value) is not None


def current_utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
