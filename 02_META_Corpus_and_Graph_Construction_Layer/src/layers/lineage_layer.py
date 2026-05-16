"""META lineage map construction layer.

Builds a deterministic, forensic lineage map that links transported ODT sources,
documents, segments, graph nodes, enriched edges, deduplication events and META
artefact exposure without adding inference or changing existing consumers.
"""
from __future__ import annotations

import hashlib
from typing import Any

from src.models import FusionContext, SourceArchive, SourceSegment
from src.utils import ProtocolError, stable

FORBIDDEN_LINEAGE_FIELDS = {
    "score",
    "priority",
    "recommendation",
    "decision",
    "causalité",
    "causality",
    "confidence",
    "probability",
    "is_entity",
    "is_fact",
    "ranking",
    "insight",
}


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _extract_node_doc_segment(node_id: str) -> tuple[str, str | None]:
    """Return (doc_id, segment_id) from a canonical node id.

    DOCUMENT node: NODE::DOCUMENT::<doc_id> -> (doc_id, None)
    SEGMENT node:  NODE::SEGMENT::<doc_id>::<segment_id> -> (doc_id, segment_id)
    """
    if not isinstance(node_id, str):
        raise ProtocolError(f"lineage: node_id must be a string, got {node_id!r}")
    parts = node_id.split("::")
    if len(parts) == 3 and parts[0] == "NODE" and parts[1] == "DOCUMENT":
        doc_id = parts[2]
        if not doc_id:
            raise ProtocolError(f"lineage: empty document node id in {node_id!r}")
        return doc_id, None
    if len(parts) == 4 and parts[0] == "NODE" and parts[1] == "SEGMENT":
        doc_id, segment_id = parts[2], parts[3]
        if not doc_id or not segment_id or segment_id in {"UNKNOWN", "?"}:
            raise ProtocolError(f"lineage: malformed segment node id {node_id!r}")
        return doc_id, segment_id
    raise ProtocolError(f"lineage: unsupported node_id format {node_id!r}")


def _edge_id(edge: dict[str, Any]) -> str:
    material = "|".join(
        [
            stable(edge.get("type")),
            stable(edge.get("source")),
            stable(edge.get("target")),
            stable(edge.get("rule")),
            stable(edge.get("evidence_hash")),
        ]
    )
    return f"EDGE::{stable(edge.get('type'))}::{_sha256_text(material)}"


def _dedup_event_id(index: int, event: dict[str, Any]) -> str:
    node_refs = [stable(ref) for ref in event.get("node_refs", [])]
    material = "|".join([str(index), stable(event.get("content_hash")), *node_refs])
    return f"DEDUP_EVENT::{index + 1:06d}::{_sha256_text(material)[:16]}"


def _source_archive_by_id(source_archives: list[SourceArchive]) -> dict[str, SourceArchive]:
    return {archive.archive_id: archive for archive in source_archives}


def _source_segments_by_node(source_segments: list[SourceSegment]) -> dict[str, SourceSegment]:
    return {segment.node_id: segment for segment in source_segments}


def _source_segments_by_doc_segment(source_segments: list[SourceSegment]) -> dict[tuple[str, str], SourceSegment]:
    return {(segment.doc_id, segment.segment_id): segment for segment in source_segments}


def _dedup_lookup(dedup_events: list[dict[str, Any]], edge_id_by_pair: dict[tuple[str, str], str]) -> tuple[list[dict[str, Any]], dict[tuple[str, str], str]]:
    """Build deterministic lineage entries for dedup events and map edge pairs to dedup IDs."""
    dedup_entries: list[dict[str, Any]] = []
    pair_to_dedup_id: dict[tuple[str, str], str] = {}
    for index, event in enumerate(dedup_events):
        node_refs = [stable(ref) for ref in event.get("node_refs", [])]
        event_id = _dedup_event_id(index, event)
        edge_refs: list[str] = []
        for left, right in zip(node_refs, node_refs[1:]):
            pair = (left, right)
            pair_to_dedup_id[pair] = event_id
            if pair in edge_id_by_pair:
                edge_refs.append(edge_id_by_pair[pair])
        dedup_entries.append(
            {
                "content_hash": event.get("content_hash"),
                "dedup_event_id": event_id,
                "dedup_event_index": index,
                "duplicate_count": event.get("duplicate_count"),
                "edge_refs": sorted(edge_refs),
                "exposed_in": ["DELTA/META_DELTA_REPORT.json"],
                "node_refs": node_refs,
            }
        )
    return dedup_entries, pair_to_dedup_id


def _artifact_exposure() -> list[dict[str, Any]]:
    return [
        {
            "artifact_path": "META/GLOBAL_DOCUMENT_CORPUS.json",
            "exposes": ["documents", "segments", "segment_content_hash", "segment_node_refs"],
        },
        {
            "artifact_path": "META/META_GLOBAL_GRAPH.json",
            "exposes": ["nodes", "edges", "rule", "source_segments", "evidence_hash"],
        },
        {
            "artifact_path": "META/GLOBAL_SEMANTIC_INDEX.json",
            "exposes": ["semantic_terms", "references"],
        },
        {
            "artifact_path": "DELTA/META_DELTA_REPORT.json",
            "exposes": ["dedup_events", "added_links"],
        },
        {
            "artifact_path": "ODT_SOURCE/",
            "exposes": ["transported_odt_archives"],
        },
    ]


def build_lineage_map(
    context: FusionContext,
    corpus_payload: dict[str, Any],
    graph_payload: dict[str, Any],
    index_payload: dict[str, Any],
    delta_payload: dict[str, Any],
    dedup_events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build deterministic META_LINEAGE_MAP payload.

    The lineage map is additive and uses already-built payloads only. It does not
    change graph/corpus/index/delta semantics and does not copy segment text.
    """
    archives_by_id = _source_archive_by_id(context.source_archives)
    segments_by_node = _source_segments_by_node(context.source_segments)
    segments_by_doc_segment = _source_segments_by_doc_segment(context.source_segments)

    nodes = list(graph_payload.get("nodes", []))
    edges = list(graph_payload.get("edges", []))
    node_by_id = {node.get("id"): node for node in nodes}

    archives_entries: list[dict[str, Any]] = []
    for archive in sorted(context.source_archives, key=lambda item: stable(item.archive_id)):
        if not archive.source_archive or not archive.source_archive.strip():
            raise ProtocolError(f"lineage: source_archive missing for archive_id={archive.archive_id!r}")
        archives_entries.append(
            {
                "archive_id": archive.archive_id,
                "doc_id": archive.doc_id,
                "odt_chain_status": (archive.odt_layer_status or {}).get("status"),
                "odt_source_package_path": f"ODT_SOURCE/{archive.source_archive}",
                "source_archive": archive.source_archive,
                "source_document": archive.source_document,
                "source_file_sha256": archive.source_file_sha256,
                "transport_verified_by": "odt_transport_sha256_verified",
            }
        )

    documents_entries: list[dict[str, Any]] = []
    for document in sorted(corpus_payload.get("documents", []), key=lambda item: stable(item.get("doc_id"))):
        doc_id = document.get("doc_id")
        archive = next((item for item in context.source_archives if item.doc_id == doc_id), None)
        document_node_id = f"NODE::DOCUMENT::{doc_id}"
        segments = list(document.get("segments", []))
        documents_entries.append(
            {
                "archive_id": archive.archive_id if archive else None,
                "content_hash": (node_by_id.get(document_node_id) or {}).get("content_hash"),
                "doc_id": doc_id,
                "document_node_id": document_node_id,
                "segment_count": len(segments),
                "segment_ids": [segment.get("segment_id") for segment in segments],
                "source_archive": document.get("source_archive"),
                "source_document": document.get("source_document"),
            }
        )

    segments_entries: list[dict[str, Any]] = []
    for segment in sorted(context.source_segments, key=lambda item: (stable(item.doc_id), stable(item.segment_id))):
        segments_entries.append(
            {
                "archive_id": segment.archive_id,
                "content_hash": segment.content_hash,
                "corpus_path": "META/GLOBAL_DOCUMENT_CORPUS.json",
                "doc_id": segment.doc_id,
                "graph_path": "META/META_GLOBAL_GRAPH.json",
                "segment_id": segment.segment_id,
                "segment_key": f"{segment.doc_id}::{segment.segment_id}",
                "segment_node_id": segment.node_id,
                "source_archive": segment.source_archive,
                "text_hash": segment.content_hash,
            }
        )

    nodes_entries: list[dict[str, Any]] = []
    for node in sorted(nodes, key=lambda item: (stable(item.get("type")), stable(item.get("id")))):
        provenance = node.get("provenance") or {}
        node_type = node.get("type")
        nodes_entries.append(
            {
                "archive_id": provenance.get("archive_id"),
                "content_hash": node.get("content_hash"),
                "doc_id": provenance.get("document_id"),
                "exposed_in": ["META/META_GLOBAL_GRAPH.json"],
                "node_id": node.get("id"),
                "node_type": node_type,
                "provenance_run_id": provenance.get("run_id"),
                "segment_id": provenance.get("segment_id") if node_type == "SEGMENT" else None,
            }
        )

    edge_id_by_pair = {(edge.get("source"), edge.get("target")): _edge_id(edge) for edge in edges}
    dedup_entries, pair_to_dedup_id = _dedup_lookup(dedup_events, edge_id_by_pair)

    edges_entries: list[dict[str, Any]] = []
    for index, edge in enumerate(sorted(edges, key=lambda item: (stable(item.get("type")), stable(item.get("source")), stable(item.get("target"))))):
        edge_type = edge.get("type")
        source = edge.get("source")
        target = edge.get("target")
        source_doc_id, _source_segment_id = _extract_node_doc_segment(source)
        target_doc_id, _target_segment_id = _extract_node_doc_segment(target)
        edge_entry: dict[str, Any] = {
            "dedup_event_ref": pair_to_dedup_id.get((source, target)) if edge_type == "DUPLICATION" else None,
            "edge_id": _edge_id(edge),
            "edge_index": index,
            "evidence_hash": edge.get("evidence_hash"),
            "exposed_in": ["META/META_GLOBAL_GRAPH.json"],
            "rule": edge.get("rule"),
            "source": source,
            "source_doc_id": source_doc_id,
            "source_segments": edge.get("source_segments"),
            "strength": edge.get("strength"),
            "target": target,
            "target_doc_id": target_doc_id,
            "type": edge_type,
        }
        if edge_type == "REFERENCE":
            segment = segments_by_node.get(source)
            edge_entry["archive_id"] = segment.archive_id if segment else None
        elif edge_type == "DUPLICATION":
            source_node = node_by_id.get(source) or {}
            target_node = node_by_id.get(target) or {}
            source_hash = source_node.get("content_hash")
            target_hash = target_node.get("content_hash")
            edge_entry["content_hash"] = source_hash
            edge_entry["exposed_in"] = ["META/META_GLOBAL_GRAPH.json", "DELTA/META_DELTA_REPORT.json"]
            if source_hash != target_hash:
                raise ProtocolError(f"lineage: DUPLICATION content_hash mismatch for {source!r} -> {target!r}")
        edges_entries.append(edge_entry)

    reference_edges = sum(1 for edge in edges if edge.get("type") == "REFERENCE")
    duplication_edges = sum(1 for edge in edges if edge.get("type") == "DUPLICATION")
    lineage_payload: dict[str, Any] = {
        "archives": archives_entries,
        "artifact_exposure": _artifact_exposure(),
        "artifact_name": "META_LINEAGE_MAP.json",
        "dedup_events": dedup_entries,
        "documents": documents_entries,
        "edges": edges_entries,
        "nodes": nodes_entries,
        "producer_layer": "META",
        "run_id": context.run_id,
        "schema_version": "META_LINEAGE_MAP_V1",
        "segments": segments_entries,
        "source_contract": {
            "meta_edges_schema": "META-01_V2_1A",
            "source_provenance_relation": "SOURCE_PROVENANCE_MAP_COMPATIBLE",
            "source_transport_barrier": "PHASE_3_0_ODT_TO_META",
            "source_transport_relation": "SOURCE_TRANSPORT_MAP_DISTINCT",
        },
        "totals": {
            "archives": len(archives_entries),
            "dedup_events": len(dedup_entries),
            "documents": len(documents_entries),
            "duplication_edges": duplication_edges,
            "edges": len(edges_entries),
            "nodes": len(nodes_entries),
            "reference_edges": reference_edges,
            "segments": len(segments_entries),
        },
        "validation_summary": {
            "lineage_valid": True,
            "violations": 0,
        },
    }

    # Basic construction-time sanity: every source segment key must resolve.
    for edge in edges_entries:
        if edge["type"] == "REFERENCE":
            for segment_id in edge.get("source_segments") or []:
                if (edge.get("source_doc_id"), segment_id) not in segments_by_doc_segment:
                    raise ProtocolError(
                        f"lineage: REFERENCE edge source_segment not found in corpus: "
                        f"doc_id={edge.get('source_doc_id')!r} segment_id={segment_id!r}"
                    )
        elif edge["type"] == "DUPLICATION":
            source_doc_id = edge.get("source_doc_id")
            target_doc_id = edge.get("target_doc_id")
            source_segments = edge.get("source_segments") or []
            pairs = [(source_doc_id, source_segments[0]), (target_doc_id, source_segments[1])] if len(source_segments) == 2 else []
            for doc_id, segment_id in pairs:
                if (doc_id, segment_id) not in segments_by_doc_segment:
                    raise ProtocolError(
                        f"lineage: DUPLICATION edge source_segment not found in corpus: "
                        f"doc_id={doc_id!r} segment_id={segment_id!r}"
                    )

    return lineage_payload
