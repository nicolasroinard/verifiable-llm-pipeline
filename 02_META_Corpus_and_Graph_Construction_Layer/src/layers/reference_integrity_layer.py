"""META-03 reference integrity / orphan detection layer.

This layer is strictly read-only. It validates that META references resolve to
real objects already produced by previous layers and reports failures without
repairing, inferring, scoring or prioritising content.
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter
from typing import Any

from src.utils import current_utc_timestamp, stable

REFERENCE_INTEGRITY_SCHEMA_VERSION = "META_REFERENCE_INTEGRITY_V1"
REFERENCE_INTEGRITY_ARTIFACT_NAME = "META_REFERENCE_INTEGRITY_REPORT.json"
REPORT_ONLY = "REPORT_ONLY"

EXPECTED_EDGE_RULES = {
    "REFERENCE": "SEGMENT_BELONGS_TO_DOCUMENT",
    "DUPLICATION": "IDENTICAL_CONTENT_HASH",
}
EXPECTED_SOURCE_SEGMENTS_LENGTH = {
    "REFERENCE": 1,
    "DUPLICATION": 2,
}

FAILURE_POLICIES: dict[str, dict[str, Any]] = {
    "MISSING_SEGMENT": {"severity": "ERROR", "blocking": True},
    "MISSING_NODE": {"severity": "ERROR", "blocking": True},
    "MISSING_EDGE_SOURCE": {"severity": "ERROR", "blocking": True},
    "MISSING_EDGE_TARGET": {"severity": "ERROR", "blocking": True},
    "INVALID_SOURCE_SEGMENTS": {"severity": "ERROR", "blocking": True},
    "INVALID_EDGE_RULE": {"severity": "ERROR", "blocking": True},
    "INVALID_EVIDENCE_HASH": {"severity": "ERROR", "blocking": True},
    "MISSING_DOCUMENT": {"severity": "ERROR", "blocking": True},
    "MISSING_ARCHIVE": {"severity": "ERROR", "blocking": True},
    "ARCHIVE_NOT_TRACEABLE": {"severity": "ERROR", "blocking": True},
    "INVALID_DEDUP_CLUSTER": {"severity": "ERROR", "blocking": True},
    "BROKEN_LINEAGE_LINK": {"severity": "ERROR", "blocking": True},
    "MISSING_ARTIFACT_EXPOSURE": {"severity": "ERROR", "blocking": True},
    "ABSOLUTE_LOCAL_PATH": {"severity": "ERROR", "blocking": True},
    "FORBIDDEN_FIELD": {"severity": "ERROR", "blocking": True},
    "INVALID_SCHEMA_VERSION": {"severity": "ERROR", "blocking": True},
    "INVALID_ARTIFACT_NAME": {"severity": "ERROR", "blocking": True},
    "INVALID_REFERENCE_INTEGRITY_REPORT": {"severity": "ERROR", "blocking": True},
}

FORBIDDEN_FIELDS = {
    "score",
    "priority",
    "recommendation",
    "decision",
    "causalité",
    "causality",
    "confidence",
    "probability",
    "ranking",
    "insight",
    "validated_entity",
    "truth_fact",
    "is_entity",
    "is_fact",
}

DEFAULT_EXPECTED_ARTIFACT_PATHS = {
    "META/GLOBAL_DOCUMENT_CORPUS.json",
    "META/META_GLOBAL_GRAPH.json",
    "META/GLOBAL_SEMANTIC_INDEX.json",
    "META/LINEAGE/META_LINEAGE_MAP.json",
    "META/LINEAGE/META_REFERENCE_INTEGRITY_REPORT.json",
    "DELTA/META_DELTA_REPORT.json",
    "ODT_SOURCE/",
}

PATH_KEYS = {
    "artifact_path",
    "corpus_path",
    "graph_path",
    "path",
    "odt_source_package_path",
    "exposed_in",
}

ABSOLUTE_WINDOWS_RE = re.compile(r"^[A-Za-z]:[\\/]")
HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
RUN_ID_TS_RE = re.compile(r"(\d{8}T\d{6}Z)")


def _timestamp_from_run_id(run_id: str) -> str | None:
    match = RUN_ID_TS_RE.search(run_id or "")
    if not match:
        return None
    raw = match.group(1)
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}T{raw[9:11]}:{raw[11:13]}:{raw[13:15]}Z"


def _generated_at(context) -> str:
    return context.fixed_timestamp or _timestamp_from_run_id(context.run_id) or current_utc_timestamp()


def _is_empty(value: Any) -> bool:
    return value in (None, "", "UNKNOWN", "?")


def _is_absolute_local_path(value: str) -> bool:
    if not isinstance(value, str):
        return False
    return (
        value.startswith("/mnt/")
        or value.startswith("/home/")
        or value.startswith("/tmp/")
        or value.startswith("file://")
        or value.startswith("/")
        or ABSOLUTE_WINDOWS_RE.match(value) is not None
    )


def _is_package_safe_path(value: str) -> bool:
    if not isinstance(value, str) or not value:
        return False
    if _is_absolute_local_path(value):
        return False
    if ".." in value.split("/"):
        return False
    return True


def _node_doc_segment(node_id: Any) -> tuple[str | None, str | None]:
    if not isinstance(node_id, str):
        return None, None
    parts = node_id.split("::")
    if len(parts) == 3 and parts[:2] == ["NODE", "DOCUMENT"]:
        return parts[2] or None, None
    if len(parts) == 4 and parts[:2] == ["NODE", "SEGMENT"]:
        return parts[2] or None, parts[3] or None
    return None, None


def _edge_id(edge: dict[str, Any], index: int) -> str:
    material = "|".join(
        [
            stable(edge.get("type")),
            stable(edge.get("source")),
            stable(edge.get("target")),
            stable(edge.get("rule")),
            stable(edge.get("evidence_hash")),
        ]
    )
    return f"EDGE::{index:06d}::{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


def _failure_hash(material: dict[str, Any]) -> str:
    text = "|".join(
        stable(material.get(key))
        for key in (
            "failure_type",
            "object_kind",
            "object_id",
            "referenced_object_kind",
            "referenced_object_id",
            "path",
            "message",
        )
    )
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _make_failure(
    failures: list[dict[str, Any]],
    failure_type: str,
    *,
    object_kind: str,
    object_id: Any,
    referenced_object_kind: str | None,
    referenced_object_id: Any,
    path: str,
    message: str,
    evidence: dict[str, Any] | None = None,
    blocking_override: bool | None = None,
) -> None:
    policy = FAILURE_POLICIES[failure_type]
    blocking = policy["blocking"] if blocking_override is None else blocking_override
    failures.append(
        {
            "failure_type": failure_type,
            "severity": policy["severity"],
            "blocking": bool(blocking),
            "object_kind": object_kind,
            "object_id": stable(object_id),
            "referenced_object_kind": stable(referenced_object_kind),
            "referenced_object_id": stable(referenced_object_id),
            "path": path,
            "message": message,
            "evidence": evidence or {},
            "suggested_action": REPORT_ONLY,
        }
    )


def _walk_forbidden_fields(value: Any, failures: list[dict[str, Any]], *, base_path: str, object_kind: str, object_id: str) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{base_path}.{key}" if base_path else str(key)
            if key in FORBIDDEN_FIELDS:
                _make_failure(
                    failures,
                    "FORBIDDEN_FIELD",
                    object_kind=object_kind,
                    object_id=object_id,
                    referenced_object_kind="field",
                    referenced_object_id=key,
                    path=child_path,
                    message="forbidden META-03 / canonical field detected",
                    evidence={"field": key},
                )
            _walk_forbidden_fields(child, failures, base_path=child_path, object_kind=object_kind, object_id=object_id)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _walk_forbidden_fields(child, failures, base_path=f"{base_path}[{index}]", object_kind=object_kind, object_id=object_id)


def _walk_path_safety(value: Any, failures: list[dict[str, Any]], *, base_path: str, object_kind: str, object_id: str, active_path_context: bool = False) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{base_path}.{key}" if base_path else str(key)
            next_active = active_path_context or key in PATH_KEYS or key.endswith("_path") or key.endswith("_paths")
            _walk_path_safety(child, failures, base_path=child_path, object_kind=object_kind, object_id=object_id, active_path_context=next_active)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _walk_path_safety(child, failures, base_path=f"{base_path}[{index}]", object_kind=object_kind, object_id=object_id, active_path_context=active_path_context)
    elif active_path_context and isinstance(value, str) and _is_absolute_local_path(value):
        _make_failure(
            failures,
            "ABSOLUTE_LOCAL_PATH",
            object_kind=object_kind,
            object_id=object_id,
            referenced_object_kind="path",
            referenced_object_id=value,
            path=base_path,
            message="absolute local path detected in canonical META artefact",
            evidence={"value": value},
        )


def _sort_failures(failures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(
        failures,
        key=lambda item: (
            not item.get("blocking", False),
            stable(item.get("failure_type")),
            stable(item.get("object_kind")),
            stable(item.get("object_id")),
            stable(item.get("referenced_object_kind")),
            stable(item.get("referenced_object_id")),
            stable(item.get("path")),
            stable(item.get("message")),
        ),
    )
    for index, failure in enumerate(ordered, start=1):
        failure["failure_id"] = f"REFINT::{index:06d}::{failure['failure_type']}::{_failure_hash(failure)}"
    return ordered


def _failure_type_summary(failures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_type = Counter(failure["failure_type"] for failure in failures)
    rows: list[dict[str, Any]] = []
    for failure_type in sorted(by_type):
        typed = [failure for failure in failures if failure["failure_type"] == failure_type]
        rows.append(
            {
                "type": failure_type,
                "count": len(typed),
                "blocking_count": sum(1 for failure in typed if failure.get("blocking") is True),
                "non_blocking_count": sum(1 for failure in typed if failure.get("blocking") is not True),
            }
        )
    return rows


def _corpus_indexes(corpus_payload: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    documents_by_id: dict[str, dict[str, Any]] = {}
    segments_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for document in corpus_payload.get("documents", []):
        doc_id = document.get("doc_id")
        if doc_id is not None:
            documents_by_id[stable(doc_id)] = document
        for segment in document.get("segments", []):
            segment_id = segment.get("segment_id")
            if doc_id is not None and segment_id is not None:
                segments_by_key[(stable(doc_id), stable(segment_id))] = segment
    return documents_by_id, segments_by_key


def build_reference_integrity_report(
    context,
    corpus_payload: dict[str, Any],
    graph_payload: dict[str, Any],
    index_payload: dict[str, Any],
    delta_payload: dict[str, Any],
    lineage_payload: dict[str, Any],
    package_expected_paths: set[str] | None = None,
) -> dict[str, Any]:
    """Build a deterministic META-03 reference integrity report.

    This function is read-only: it does not mutate inputs and does not repair any
    failure it observes. All findings are reported with suggested_action=REPORT_ONLY.
    """
    failures: list[dict[str, Any]] = []
    expected_paths = set(package_expected_paths or DEFAULT_EXPECTED_ARTIFACT_PATHS)

    documents_by_id, segments_by_key = _corpus_indexes(corpus_payload)
    graph_nodes = list(graph_payload.get("nodes", []))
    graph_edges = list(graph_payload.get("edges", []))
    node_by_id = {stable(node.get("id")): node for node in graph_nodes if node.get("id") is not None}
    node_ids = set(node_by_id)
    source_archive_ids = {stable(archive.archive_id) for archive in context.source_archives}
    source_archive_names = {stable(archive.source_archive) for archive in context.source_archives}
    archive_by_doc = {stable(archive.doc_id): archive for archive in context.source_archives}
    lineage_archives = list(lineage_payload.get("archives", []))
    lineage_documents = list(lineage_payload.get("documents", []))
    lineage_segments = list(lineage_payload.get("segments", []))
    lineage_nodes = list(lineage_payload.get("nodes", []))
    lineage_edges = list(lineage_payload.get("edges", []))
    lineage_dedup_events = list(lineage_payload.get("dedup_events", []))
    lineage_artifacts = list(lineage_payload.get("artifact_exposure", []))
    lineage_node_ids = {stable(node.get("node_id")) for node in lineage_nodes if node.get("node_id") is not None}
    lineage_edge_ids = {stable(edge.get("edge_id")) for edge in lineage_edges if edge.get("edge_id") is not None}
    lineage_dedup_ids = {stable(event.get("dedup_event_id")) for event in lineage_dedup_events if event.get("dedup_event_id") is not None}

    # Schema / artifact checks on META_LINEAGE_MAP.
    if lineage_payload.get("schema_version") != "META_LINEAGE_MAP_V1":
        _make_failure(
            failures,
            "INVALID_SCHEMA_VERSION",
            object_kind="artifact",
            object_id="META_LINEAGE_MAP.json",
            referenced_object_kind="schema_version",
            referenced_object_id=lineage_payload.get("schema_version"),
            path="lineage.schema_version",
            message="META_LINEAGE_MAP schema_version is not META_LINEAGE_MAP_V1",
            evidence={"expected": "META_LINEAGE_MAP_V1", "actual": lineage_payload.get("schema_version")},
        )
    if lineage_payload.get("artifact_name") != "META_LINEAGE_MAP.json":
        _make_failure(
            failures,
            "INVALID_ARTIFACT_NAME",
            object_kind="artifact",
            object_id="META_LINEAGE_MAP.json",
            referenced_object_kind="artifact_name",
            referenced_object_id=lineage_payload.get("artifact_name"),
            path="lineage.artifact_name",
            message="META_LINEAGE_MAP artifact_name is invalid",
            evidence={"expected": "META_LINEAGE_MAP.json", "actual": lineage_payload.get("artifact_name")},
        )

    # Archives.
    for index, archive in enumerate(lineage_archives):
        archive_id = stable(archive.get("archive_id"))
        source_archive = stable(archive.get("source_archive"))
        package_path = stable(archive.get("odt_source_package_path"))
        if _is_empty(archive_id) or archive_id not in source_archive_ids:
            _make_failure(
                failures,
                "MISSING_ARCHIVE",
                object_kind="archive",
                object_id=archive_id,
                referenced_object_kind="archive",
                referenced_object_id=archive_id,
                path=f"lineage.archives[{index}].archive_id",
                message="lineage archive_id is not resolved in source archives",
                evidence={"source_archive_ids": sorted(source_archive_ids)},
            )
        if _is_empty(source_archive) or source_archive not in source_archive_names:
            _make_failure(
                failures,
                "ARCHIVE_NOT_TRACEABLE",
                object_kind="archive",
                object_id=archive_id,
                referenced_object_kind="source_archive",
                referenced_object_id=source_archive,
                path=f"lineage.archives[{index}].source_archive",
                message="lineage archive source_archive is not traceable",
                evidence={"source_archive_names": sorted(source_archive_names)},
            )
        if _is_empty(package_path) or not package_path.startswith("ODT_SOURCE/") or not _is_package_safe_path(package_path):
            _make_failure(
                failures,
                "ARCHIVE_NOT_TRACEABLE",
                object_kind="archive",
                object_id=archive_id,
                referenced_object_kind="path",
                referenced_object_id=package_path,
                path=f"lineage.archives[{index}].odt_source_package_path",
                message="lineage archive package path is not package-safe or not under ODT_SOURCE/",
                evidence={"path": package_path},
            )

    # Documents and segments in corpus.
    for doc_index, document in enumerate(corpus_payload.get("documents", [])):
        doc_id = stable(document.get("doc_id"))
        archive = archive_by_doc.get(doc_id)
        source_archive = stable(document.get("source_archive"))
        if _is_empty(doc_id):
            _make_failure(
                failures,
                "MISSING_DOCUMENT",
                object_kind="document",
                object_id=doc_id,
                referenced_object_kind="document",
                referenced_object_id=doc_id,
                path=f"corpus.documents[{doc_index}].doc_id",
                message="document doc_id is empty or unresolved",
                evidence={},
            )
        if archive is None:
            _make_failure(
                failures,
                "MISSING_ARCHIVE",
                object_kind="document",
                object_id=doc_id,
                referenced_object_kind="archive",
                referenced_object_id="",
                path=f"corpus.documents[{doc_index}]",
                message="document has no resolved source archive",
                evidence={"doc_id": doc_id},
            )
        if source_archive and source_archive not in source_archive_names:
            _make_failure(
                failures,
                "ARCHIVE_NOT_TRACEABLE",
                object_kind="document",
                object_id=doc_id,
                referenced_object_kind="source_archive",
                referenced_object_id=source_archive,
                path=f"corpus.documents[{doc_index}].source_archive",
                message="document source_archive is not traceable",
                evidence={"source_archive": source_archive},
            )
        for seg_index, segment in enumerate(document.get("segments", [])):
            segment_id = stable(segment.get("segment_id"))
            if _is_empty(segment_id):
                _make_failure(
                    failures,
                    "MISSING_SEGMENT",
                    object_kind="segment",
                    object_id=f"{doc_id}::{segment_id}",
                    referenced_object_kind="segment",
                    referenced_object_id=segment_id,
                    path=f"corpus.documents[{doc_index}].segments[{seg_index}].segment_id",
                    message="segment_id is empty or unresolved",
                    evidence={"doc_id": doc_id},
                )
            if doc_id not in documents_by_id:
                _make_failure(
                    failures,
                    "MISSING_DOCUMENT",
                    object_kind="segment",
                    object_id=f"{doc_id}::{segment_id}",
                    referenced_object_kind="document",
                    referenced_object_id=doc_id,
                    path=f"corpus.documents[{doc_index}].segments[{seg_index}]",
                    message="segment parent document is not resolved",
                    evidence={"doc_id": doc_id, "segment_id": segment_id},
                )

    # Nodes.
    for index, node in enumerate(graph_nodes):
        node_id = stable(node.get("id"))
        node_type = node.get("type")
        provenance = node.get("provenance") or {}
        doc_id_from_node, segment_id_from_node = _node_doc_segment(node_id)
        doc_id = stable(provenance.get("document_id") or doc_id_from_node)
        segment_id = stable(provenance.get("segment_id") or segment_id_from_node)
        if _is_empty(node_id):
            _make_failure(
                failures,
                "MISSING_NODE",
                object_kind="node",
                object_id=node_id,
                referenced_object_kind="node",
                referenced_object_id=node_id,
                path=f"graph.nodes[{index}].id",
                message="node id is empty or unresolved",
                evidence={"node_type": node_type},
            )
        if node_type == "SEGMENT":
            if (doc_id, segment_id) not in segments_by_key:
                _make_failure(
                    failures,
                    "MISSING_SEGMENT",
                    object_kind="node",
                    object_id=node_id,
                    referenced_object_kind="segment",
                    referenced_object_id=f"{doc_id}::{segment_id}",
                    path=f"graph.nodes[{index}].provenance.segment_id",
                    message="SEGMENT node does not resolve to an existing corpus segment",
                    evidence={"doc_id": doc_id, "segment_id": segment_id},
                )
        elif node_type == "DOCUMENT":
            if doc_id not in documents_by_id:
                _make_failure(
                    failures,
                    "MISSING_DOCUMENT",
                    object_kind="node",
                    object_id=node_id,
                    referenced_object_kind="document",
                    referenced_object_id=doc_id,
                    path=f"graph.nodes[{index}].provenance.document_id",
                    message="DOCUMENT node does not resolve to an existing corpus document",
                    evidence={"doc_id": doc_id},
                )
        archive_id = stable(provenance.get("archive_id"))
        if archive_id and archive_id not in source_archive_ids:
            _make_failure(
                failures,
                "MISSING_ARCHIVE",
                object_kind="node",
                object_id=node_id,
                referenced_object_kind="archive",
                referenced_object_id=archive_id,
                path=f"graph.nodes[{index}].provenance.archive_id",
                message="node archive_id is not resolved in source archives",
                evidence={"archive_id": archive_id},
            )

    # Edges.
    for index, edge in enumerate(graph_edges):
        edge_id = _edge_id(edge, index)
        edge_type = edge.get("type")
        source = stable(edge.get("source"))
        target = stable(edge.get("target"))
        if source not in node_ids:
            _make_failure(
                failures,
                "MISSING_EDGE_SOURCE",
                object_kind="edge",
                object_id=edge_id,
                referenced_object_kind="node",
                referenced_object_id=source,
                path=f"graph.edges[{index}].source",
                message="edge.source does not resolve to a graph node",
                evidence={"edge_type": edge_type, "source": source},
            )
        if target not in node_ids:
            _make_failure(
                failures,
                "MISSING_EDGE_TARGET",
                object_kind="edge",
                object_id=edge_id,
                referenced_object_kind="node",
                referenced_object_id=target,
                path=f"graph.edges[{index}].target",
                message="edge.target does not resolve to a graph node",
                evidence={"edge_type": edge_type, "target": target},
            )
        expected_rule = EXPECTED_EDGE_RULES.get(edge_type)
        if edge.get("rule") != expected_rule:
            _make_failure(
                failures,
                "INVALID_EDGE_RULE",
                object_kind="edge",
                object_id=edge_id,
                referenced_object_kind="rule",
                referenced_object_id=edge.get("rule"),
                path=f"graph.edges[{index}].rule",
                message="edge rule is incompatible with edge type",
                evidence={"edge_type": edge_type, "expected_rule": expected_rule, "actual_rule": edge.get("rule")},
            )
        source_segments = edge.get("source_segments")
        expected_len = EXPECTED_SOURCE_SEGMENTS_LENGTH.get(edge_type)
        if not isinstance(source_segments, list) or expected_len is None or len(source_segments) != expected_len:
            _make_failure(
                failures,
                "INVALID_SOURCE_SEGMENTS",
                object_kind="edge",
                object_id=edge_id,
                referenced_object_kind="segment",
                referenced_object_id=stable(source_segments),
                path=f"graph.edges[{index}].source_segments",
                message="edge source_segments is absent, malformed or has invalid cardinality",
                evidence={"edge_type": edge_type, "expected_length": expected_len, "actual": source_segments},
            )
        else:
            source_doc, _ = _node_doc_segment(source)
            target_doc, _ = _node_doc_segment(target)
            segment_pairs: list[tuple[str | None, Any]] = []
            if edge_type == "REFERENCE":
                segment_pairs = [(source_doc, source_segments[0])]
            elif edge_type == "DUPLICATION":
                segment_pairs = [(source_doc, source_segments[0]), (target_doc, source_segments[1])]
            for segment_index, (doc_id, segment_id) in enumerate(segment_pairs):
                if (stable(doc_id), stable(segment_id)) not in segments_by_key:
                    _make_failure(
                        failures,
                        "MISSING_SEGMENT",
                        object_kind="edge",
                        object_id=edge_id,
                        referenced_object_kind="segment",
                        referenced_object_id=f"{stable(doc_id)}::{stable(segment_id)}",
                        path=f"graph.edges[{index}].source_segments[{segment_index}]",
                        message="edge source_segment does not resolve in corpus",
                        evidence={"edge_type": edge_type, "source": source, "target": target},
                    )
        evidence_hash = edge.get("evidence_hash")
        if not isinstance(evidence_hash, str) or HEX64_RE.match(evidence_hash) is None:
            _make_failure(
                failures,
                "INVALID_EVIDENCE_HASH",
                object_kind="edge",
                object_id=edge_id,
                referenced_object_kind="evidence_hash",
                referenced_object_id=evidence_hash,
                path=f"graph.edges[{index}].evidence_hash",
                message="edge evidence_hash is absent or not a 64-character hexadecimal string",
                evidence={"actual": evidence_hash},
            )
        forbidden = sorted(FORBIDDEN_FIELDS.intersection(edge.keys()))
        if forbidden:
            _make_failure(
                failures,
                "FORBIDDEN_FIELD",
                object_kind="edge",
                object_id=edge_id,
                referenced_object_kind="field",
                referenced_object_id=",".join(forbidden),
                path=f"graph.edges[{index}]",
                message="forbidden field detected on edge",
                evidence={"fields": forbidden},
            )

    # Dedup events in lineage.
    for index, event in enumerate(lineage_dedup_events):
        event_id = stable(event.get("dedup_event_id"))
        node_refs = event.get("node_refs")
        edge_refs = event.get("edge_refs") or []
        duplicate_count = event.get("duplicate_count")
        if not isinstance(node_refs, list) or not node_refs:
            _make_failure(
                failures,
                "INVALID_DEDUP_CLUSTER",
                object_kind="dedup_event",
                object_id=event_id,
                referenced_object_kind="node",
                referenced_object_id="",
                path=f"lineage.dedup_events[{index}].node_refs",
                message="dedup_event node_refs is missing or empty",
                evidence={"dedup_event_id": event_id},
            )
        else:
            if isinstance(duplicate_count, int) and duplicate_count != len(node_refs):
                _make_failure(
                    failures,
                    "INVALID_DEDUP_CLUSTER",
                    object_kind="dedup_event",
                    object_id=event_id,
                    referenced_object_kind="duplicate_count",
                    referenced_object_id=duplicate_count,
                    path=f"lineage.dedup_events[{index}].duplicate_count",
                    message="dedup_event duplicate_count does not match node_refs length",
                    evidence={"duplicate_count": duplicate_count, "node_refs_count": len(node_refs)},
                )
            for ref_index, node_ref in enumerate(node_refs):
                if stable(node_ref) not in node_ids and stable(node_ref) not in lineage_node_ids:
                    _make_failure(
                        failures,
                        "MISSING_NODE",
                        object_kind="dedup_event",
                        object_id=event_id,
                        referenced_object_kind="node",
                        referenced_object_id=node_ref,
                        path=f"lineage.dedup_events[{index}].node_refs[{ref_index}]",
                        message="dedup_event node_ref does not resolve to graph/lineage node",
                        evidence={"dedup_event_id": event_id},
                    )
        for ref_index, edge_ref in enumerate(edge_refs):
            if stable(edge_ref) not in lineage_edge_ids:
                _make_failure(
                    failures,
                    "BROKEN_LINEAGE_LINK",
                    object_kind="dedup_event",
                    object_id=event_id,
                    referenced_object_kind="edge",
                    referenced_object_id=edge_ref,
                    path=f"lineage.dedup_events[{index}].edge_refs[{ref_index}]",
                    message="dedup_event edge_ref does not resolve to lineage edge",
                    evidence={"dedup_event_id": event_id},
                )

    # Lineage cross-links.
    lineage_document_ids = {stable(item.get("doc_id")) for item in lineage_documents if item.get("doc_id") is not None}
    lineage_segment_keys = {(stable(item.get("doc_id")), stable(item.get("segment_id"))) for item in lineage_segments}
    graph_edge_keys = {(stable(edge.get("type")), stable(edge.get("source")), stable(edge.get("target"))) for edge in graph_edges}
    lineage_edge_keys = {(stable(edge.get("type")), stable(edge.get("source")), stable(edge.get("target"))) for edge in lineage_edges}
    if lineage_document_ids != set(documents_by_id):
        missing = sorted(set(documents_by_id) - lineage_document_ids)
        extra = sorted(lineage_document_ids - set(documents_by_id))
        _make_failure(
            failures,
            "BROKEN_LINEAGE_LINK",
            object_kind="lineage",
            object_id="documents",
            referenced_object_kind="document",
            referenced_object_id=",".join(missing or extra),
            path="lineage.documents",
            message="lineage document set does not match corpus document set",
            evidence={"missing": missing, "extra": extra},
        )
    if lineage_segment_keys != set(segments_by_key):
        missing = sorted(f"{doc}::{seg}" for doc, seg in set(segments_by_key) - lineage_segment_keys)
        extra = sorted(f"{doc}::{seg}" for doc, seg in lineage_segment_keys - set(segments_by_key))
        _make_failure(
            failures,
            "BROKEN_LINEAGE_LINK",
            object_kind="lineage",
            object_id="segments",
            referenced_object_kind="segment",
            referenced_object_id=",".join(missing[:10] or extra[:10]),
            path="lineage.segments",
            message="lineage segment set does not match corpus segment set",
            evidence={"missing_count": len(missing), "extra_count": len(extra)},
        )
    if lineage_node_ids != node_ids:
        missing = sorted(node_ids - lineage_node_ids)
        extra = sorted(lineage_node_ids - node_ids)
        _make_failure(
            failures,
            "BROKEN_LINEAGE_LINK",
            object_kind="lineage",
            object_id="nodes",
            referenced_object_kind="node",
            referenced_object_id=",".join((missing or extra)[:10]),
            path="lineage.nodes",
            message="lineage node set does not match graph node set",
            evidence={"missing_count": len(missing), "extra_count": len(extra)},
        )
    if lineage_edge_keys != graph_edge_keys:
        missing = sorted(f"{t}|{s}|{g}" for t, s, g in graph_edge_keys - lineage_edge_keys)
        extra = sorted(f"{t}|{s}|{g}" for t, s, g in lineage_edge_keys - graph_edge_keys)
        _make_failure(
            failures,
            "BROKEN_LINEAGE_LINK",
            object_kind="lineage",
            object_id="edges",
            referenced_object_kind="edge",
            referenced_object_id=",".join((missing or extra)[:10]),
            path="lineage.edges",
            message="lineage edge set does not match graph edge set",
            evidence={"missing_count": len(missing), "extra_count": len(extra)},
        )

    # Artifact exposure.
    for index, artifact in enumerate(lineage_artifacts):
        artifact_path = stable(artifact.get("artifact_path"))
        if not _is_package_safe_path(artifact_path):
            _make_failure(
                failures,
                "ABSOLUTE_LOCAL_PATH",
                object_kind="artifact_exposure",
                object_id=artifact_path,
                referenced_object_kind="path",
                referenced_object_id=artifact_path,
                path=f"lineage.artifact_exposure[{index}].artifact_path",
                message="artifact_exposure path is not package-safe",
                evidence={"artifact_path": artifact_path},
            )
        if artifact_path not in expected_paths:
            _make_failure(
                failures,
                "MISSING_ARTIFACT_EXPOSURE",
                object_kind="artifact_exposure",
                object_id=artifact_path,
                referenced_object_kind="artifact",
                referenced_object_id=artifact_path,
                path=f"lineage.artifact_exposure[{index}].artifact_path",
                message="artifact_exposure path is not in expected package artefacts",
                evidence={"expected_paths": sorted(expected_paths)},
            )

    # Canonical forbidden fields and path safety.
    _walk_forbidden_fields(lineage_payload, failures, base_path="lineage", object_kind="artifact", object_id="META_LINEAGE_MAP.json")
    _walk_path_safety(lineage_payload, failures, base_path="lineage", object_kind="artifact", object_id="META_LINEAGE_MAP.json")

    ordered_failures = _sort_failures(failures)
    blocking_failures = sum(1 for failure in ordered_failures if failure.get("blocking") is True)
    non_blocking_failures = len(ordered_failures) - blocking_failures

    report = {
        "artifact_name": REFERENCE_INTEGRITY_ARTIFACT_NAME,
        "schema_version": REFERENCE_INTEGRITY_SCHEMA_VERSION,
        "summary": {
            "integrity_status": "PASS" if blocking_failures == 0 else "FAIL",
            "total_checks": (
                len(lineage_archives)
                + len(corpus_payload.get("documents", []))
                + len(segments_by_key)
                + len(graph_nodes)
                + len(graph_edges)
                + len(lineage_dedup_events)
                + len(lineage_artifacts)
            ),
            "total_failures": len(ordered_failures),
            "blocking_failures": blocking_failures,
            "non_blocking_failures": non_blocking_failures,
            "checked_archives": len(lineage_archives),
            "checked_documents": len(corpus_payload.get("documents", [])),
            "checked_segments": len(segments_by_key),
            "checked_nodes": len(graph_nodes),
            "checked_edges": len(graph_edges),
            "checked_dedup_events": len(lineage_dedup_events),
            "checked_artifact_exposures": len(lineage_artifacts),
        },
        "failures_by_type": _failure_type_summary(ordered_failures),
        "failures_details": ordered_failures,
        "validation_scope": {
            "corpus": True,
            "graph": True,
            "lineage_map": True,
            "dedup_events": True,
            "artifact_exposure": True,
            "forbidden_fields": True,
            "path_safety": True,
        },
        "metadata": {
            "meta_version": "V9.2",
            "meta_reference_integrity_version": REFERENCE_INTEGRITY_SCHEMA_VERSION,
            "run_id": context.run_id,
            "generated_at": _generated_at(context),
            "deterministic": True,
        },
    }
    return report
