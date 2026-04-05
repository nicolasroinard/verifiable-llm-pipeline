from __future__ import annotations

import json
import zipfile

from src.models import Check, SourceArchive, SourceSegment
from src.utils import (
    ACCEPT_CHAIN_STATUS,
    ACCEPT_MANIFEST_VERDICTS,
    ACCEPT_ODT_STATUS,
    ProtocolError,
    ensure,
    sha256_bytes,
    validate_iso_utc,
)

REQUIRED_SOURCE_SUFFIXES = [
    "GLOBAL_DOCUMENT_CORPUS.json",
    "ARTEFACT_segments.json",
    "CHAIN_INTEGRITY_MANIFEST.json",
    "SOURCE_DOCUMENT_METADATA.json",
    "ARCHIVE_MANIFEST.json",
    "MASTER_SHA256.txt",
    "ODT_LAYER_STATUS.json",
]


def _read_json_from_zip(archive_name: str, zf: zipfile.ZipFile, names: list[str], suffix: str) -> dict:
    matches = [name for name in names if name.endswith(suffix)]
    if not matches:
        raise ProtocolError(f"{archive_name}: missing required artefact {suffix}")
    return json.loads(zf.read(matches[0]))


def _read_text_from_zip(archive_name: str, zf: zipfile.ZipFile, names: list[str], suffix: str) -> str:
    matches = [name for name in names if name.endswith(suffix)]
    if not matches:
        raise ProtocolError(f"{archive_name}: missing required artefact {suffix}")
    return zf.read(matches[0]).decode("utf-8")


def _parse_master_lines(master_text: str) -> list[tuple[str, str]]:
    entries = []
    for raw_line in master_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "  " not in line:
            raise ProtocolError(f"Invalid MASTER_SHA256 line: {raw_line}")
        sha, path = line.split("  ", 1)
        entries.append((sha.strip(), path.strip()))
    return entries


def load_and_validate_inputs(context) -> None:
    zip_paths = sorted(context.input_dir.glob("*.zip"))
    ensure(zip_paths, f"No zip archives found in {context.input_dir}")

    for archive_path in zip_paths:
        with zipfile.ZipFile(archive_path) as zf:
            names = zf.namelist()
            file_entries = {name for name in names if not name.endswith("/")}
            for suffix in REQUIRED_SOURCE_SUFFIXES:
                result = "PASS" if any(name.endswith(suffix) for name in names) else "FAIL"
                context.checks.append(Check(archive_id=archive_path.stem, check=f"presence::{suffix}", result=result))
                ensure(result == "PASS", f"{archive_path.name}: missing required artefact {suffix}")

            corpus = _read_json_from_zip(archive_path.name, zf, names, "GLOBAL_DOCUMENT_CORPUS.json")
            segments = _read_json_from_zip(archive_path.name, zf, names, "ARTEFACT_segments.json")
            source_metadata = _read_json_from_zip(archive_path.name, zf, names, "SOURCE_DOCUMENT_METADATA.json")
            archive_manifest = _read_json_from_zip(archive_path.name, zf, names, "ARCHIVE_MANIFEST.json")
            chain_manifest = _read_json_from_zip(archive_path.name, zf, names, "CHAIN_INTEGRITY_MANIFEST.json")
            odt_layer_status = _read_json_from_zip(archive_path.name, zf, names, "ODT_LAYER_STATUS.json")
            master_text = _read_text_from_zip(archive_path.name, zf, names, "MASTER_SHA256.txt")

            context.checks.extend([
                Check(archive_id=archive_path.stem, check="json_read::GLOBAL_DOCUMENT_CORPUS.json", result="PASS"),
                Check(archive_id=archive_path.stem, check="json_read::ARTEFACT_segments.json", result="PASS"),
                Check(archive_id=archive_path.stem, check="json_read::SOURCE_DOCUMENT_METADATA.json", result="PASS"),
                Check(archive_id=archive_path.stem, check="json_read::ARCHIVE_MANIFEST.json", result="PASS"),
                Check(archive_id=archive_path.stem, check="json_read::CHAIN_INTEGRITY_MANIFEST.json", result="PASS"),
                Check(archive_id=archive_path.stem, check="json_read::ODT_LAYER_STATUS.json", result="PASS"),
                Check(archive_id=archive_path.stem, check="read::MASTER_SHA256.txt", result="PASS"),
            ])

            documents = corpus.get("documents") or []
            ensure(bool(documents), f"{archive_path.name}: source corpus has no documents")
            ensure(len(documents) == 1, f"{archive_path.name}: multi-document source corpus not supported without explicit handling")
            document = documents[0]
            doc_id = document.get("document_id") or document.get("doc_id") or corpus.get("document_id")
            ensure(bool(doc_id), f"{archive_path.name}: missing doc_id/document_id in source corpus")

            source_name = source_metadata.get("source_file_name") or document.get("source_file_name")
            source_sha = (
                source_metadata.get("source_file_sha256")
                or document.get("source_file_sha256")
                or archive_manifest.get("source", {}).get("sha256")
            )
            ingest_timestamp_utc = source_metadata.get("extraction_timestamp_utc") or archive_manifest.get("generated_at_utc")
            if context.fixed_timestamp:
                ingest_timestamp_utc = context.fixed_timestamp

            manifest_verdict = archive_manifest.get("validation_finale_ultra", {}).get("verdict") or archive_manifest.get("validation_finale_ultra", {}).get("status")
            chain_status = chain_manifest.get("status") or chain_manifest.get("integrity_result")
            odt_status = odt_layer_status.get("status") or odt_layer_status.get("verdict")
            expected_segment_count = document.get("segment_count") or document.get("segment_count_total") or len(segments)

            context.checks.extend([
                Check(archive_id=archive_path.stem, check="archive_manifest_verdict", result="PASS" if manifest_verdict in ACCEPT_MANIFEST_VERDICTS else "FAIL", details={"value": manifest_verdict}),
                Check(archive_id=archive_path.stem, check="source_chain_status", result="PASS" if chain_status in ACCEPT_CHAIN_STATUS else "FAIL", details={"value": chain_status}),
                Check(archive_id=archive_path.stem, check="odt_layer_status", result="PASS" if odt_status in ACCEPT_ODT_STATUS else "FAIL", details={"value": odt_status}),
                Check(archive_id=archive_path.stem, check="segment_count_match", result="PASS" if int(expected_segment_count) == len(segments) else "FAIL", details={"expected": int(expected_segment_count), "actual": len(segments)}),
                Check(archive_id=archive_path.stem, check="source_file_sha256_present", result="PASS" if bool(source_sha) else "FAIL"),
                Check(archive_id=archive_path.stem, check="ingest_timestamp_format", result="PASS" if validate_iso_utc(ingest_timestamp_utc) else "FAIL", details={"value": ingest_timestamp_utc}),
            ])

            master_entries = _parse_master_lines(master_text)
            master_missing = 0
            master_hash_mismatch = 0
            for expected_sha, inner_path in master_entries:
                matches = [name for name in file_entries if name.endswith(inner_path)]
                if not matches:
                    master_missing += 1
                    continue
                actual_sha = sha256_bytes(zf.read(matches[0]))
                if actual_sha != expected_sha:
                    master_hash_mismatch += 1
            context.checks.extend([
                Check(archive_id=archive_path.stem, check="source_master_entries_present", result="PASS" if master_missing == 0 else "FAIL", details={"missing": master_missing}),
                Check(archive_id=archive_path.stem, check="source_master_hash_alignment", result="PASS" if master_hash_mismatch == 0 else "FAIL", details={"mismatch": master_hash_mismatch}),
            ])

            chain_items = chain_manifest.get("items", [])
            chain_missing = 0
            chain_hash_mismatch = 0
            for item in chain_items:
                path = item.get("path")
                sha = item.get("sha256")
                matches = [name for name in file_entries if name.endswith(path or "")]
                if not matches:
                    chain_missing += 1
                    continue
                actual_sha = sha256_bytes(zf.read(matches[0]))
                if sha and actual_sha != sha:
                    chain_hash_mismatch += 1
            context.checks.extend([
                Check(archive_id=archive_path.stem, check="source_chain_items_present", result="PASS" if chain_missing == 0 else "FAIL", details={"missing": chain_missing}),
                Check(archive_id=archive_path.stem, check="source_chain_hash_alignment", result="PASS" if chain_hash_mismatch == 0 else "FAIL", details={"mismatch": chain_hash_mismatch}),
            ])

            source_archive = SourceArchive(
                archive_path=archive_path,
                archive_id=archive_path.stem,
                source_archive=archive_path.name,
                source_document=source_name,
                doc_id=str(doc_id),
                source_file_sha256=source_sha,
                ingest_timestamp_utc=ingest_timestamp_utc,
                corpus=corpus,
                segments_payload=segments,
                source_metadata=source_metadata,
                archive_manifest=archive_manifest,
                chain_manifest=chain_manifest,
                odt_layer_status=odt_layer_status,
            )
            context.source_archives.append(source_archive)

            seen_segment_ids: set[str] = set()
            for payload in segments:
                text = payload.get("text", "")
                raw_segment_id = str(payload.get("source_segment_id") or payload.get("segment_id"))
                ensure(raw_segment_id not in seen_segment_ids, f"{archive_path.name}: duplicate source segment_id detected: {raw_segment_id}")
                seen_segment_ids.add(raw_segment_id)
                node_id = f"NODE::SEGMENT::{doc_id}::{raw_segment_id}"
                segment = SourceSegment(
                    archive_id=archive_path.stem,
                    doc_id=str(doc_id),
                    source_archive=archive_path.name,
                    source_document=source_name,
                    segment_id=raw_segment_id,
                    node_id=node_id,
                    text=text,
                    content_hash=payload.get("hash_segment") or payload.get("content_hash") or sha256_bytes(text.encode("utf-8")),
                    ingest_timestamp_utc=ingest_timestamp_utc,
                )
                context.source_segments.append(segment)

    if any(check.result == "FAIL" for check in context.checks if check.archive_id != "META"):
        raise ProtocolError("Source validation failed on at least one required ODT archive artefact, integrity check, or status contract.")
