from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Check:
    archive_id: str
    check: str
    result: str
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = {"archive_id": self.archive_id, "check": self.check, "result": self.result}
        payload.update(self.details)
        return payload


@dataclass
class SourceArchive:
    archive_path: Path
    archive_id: str
    source_archive: str
    source_document: str | None
    doc_id: str
    source_file_sha256: str | None
    ingest_timestamp_utc: str | None
    corpus: dict[str, Any]
    segments_payload: list[dict[str, Any]]
    source_metadata: dict[str, Any]
    archive_manifest: dict[str, Any]
    chain_manifest: dict[str, Any]
    odt_layer_status: dict[str, Any]


@dataclass
class SourceSegment:
    archive_id: str
    doc_id: str
    source_archive: str
    source_document: str | None
    segment_id: str
    node_id: str
    text: str
    content_hash: str
    ingest_timestamp_utc: str | None


@dataclass
class FusionContext:
    run_id: str
    input_dir: Path
    output_dir: Path
    strict: bool = False
    reproducible: bool = False
    fixed_timestamp: str | None = None
    source_archives: list[SourceArchive] = field(default_factory=list)
    source_segments: list[SourceSegment] = field(default_factory=list)
    checks: list[Check] = field(default_factory=list)
    unresolved_items: list[dict[str, Any]] = field(default_factory=list)
