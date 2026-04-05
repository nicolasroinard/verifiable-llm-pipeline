from __future__ import annotations

from src.models import Check
from src.utils import DOCUMENT_NODE_SEGMENT_SENTINEL, validate_iso_utc


NODE_TYPES = {"DOCUMENT", "SEGMENT", "ENTITY", "CONCEPT"}
EDGE_TYPES = {"REFERENCE", "DUPLICATION", "CONTRADICTION", "ENRICHMENT"}
REF_TYPES = {"NODE", "SEGMENT", "DOCUMENT"}


def compute_reference_consistency(corpus_payload: dict, graph_payload: dict, index_payload: dict) -> tuple[float, dict]:
    node_ids = {node["id"] for node in graph_payload["nodes"]}
    doc_ids = {document["doc_id"] for document in corpus_payload["documents"]}
    segment_ids = {segment["segment_id"] for document in corpus_payload["documents"] for segment in document["segments"]}

    total_refs = 0
    valid_refs = 0
    orphan_node_refs = 0
    orphan_edge_refs = 0
    orphan_index_refs = 0

    for document in corpus_payload["documents"]:
        for segment in document["segments"]:
            for node_ref in segment.get("node_refs", []):
                total_refs += 1
                if node_ref in node_ids:
                    valid_refs += 1
                else:
                    orphan_node_refs += 1

    for edge in graph_payload["edges"]:
        total_refs += 2
        if edge["source"] in node_ids:
            valid_refs += 1
        else:
            orphan_edge_refs += 1
        if edge["target"] in node_ids:
            valid_refs += 1
        else:
            orphan_edge_refs += 1

    for entry in index_payload["index"]:
        for reference in entry["references"]:
            total_refs += 1
            is_valid = (
                (reference["ref_type"] == "NODE" and reference["ref_id"] in node_ids)
                or (reference["ref_type"] == "SEGMENT" and reference["ref_id"] in segment_ids)
                or (reference["ref_type"] == "DOCUMENT" and reference["ref_id"] in doc_ids)
            )
            if is_valid:
                valid_refs += 1
            else:
                orphan_index_refs += 1

    consistency = (valid_refs / total_refs) if total_refs else 1.0
    return consistency, {
        "valid_references": valid_refs,
        "total_references": total_refs,
        "orphan_node_refs": orphan_node_refs,
        "orphan_edge_refs": orphan_edge_refs,
        "orphan_index_refs": orphan_index_refs,
    }


def validate_payload_integrity(corpus_payload: dict, graph_payload: dict, index_payload: dict) -> tuple[float, dict]:
    invalid_node_types = sum(1 for node in graph_payload["nodes"] if node.get("type") not in NODE_TYPES)
    invalid_edge_types = sum(1 for edge in graph_payload["edges"] if edge.get("type") not in EDGE_TYPES)
    invalid_edge_strengths = sum(
        1 for edge in graph_payload["edges"]
        if not isinstance(edge.get("strength"), (int, float)) or not (0.0 <= float(edge["strength"]) <= 1.0)
    )
    invalid_ref_types = sum(
        1 for entry in index_payload["index"] for ref in entry.get("references", []) if ref.get("ref_type") not in REF_TYPES
    )
    invalid_ref_scores = sum(
        1
        for entry in index_payload["index"]
        for ref in entry.get("references", [])
        if not isinstance(ref.get("score"), (int, float)) or not (0.0 <= float(ref["score"]) <= 1.0)
    )
    missing_node_refs = sum(
        1 for document in corpus_payload["documents"] for segment in document.get("segments", []) if not segment.get("node_refs")
    )
    missing_run_id = sum(1 for node in graph_payload["nodes"] if node.get("provenance", {}).get("run_id") is None)
    missing_ingest_timestamp = sum(
        1 for node in graph_payload["nodes"] if node.get("provenance", {}).get("ingest_timestamp_utc") in (None, "")
    )
    invalid_ingest_timestamp_format = sum(
        1 for node in graph_payload["nodes"] if not validate_iso_utc(node.get("provenance", {}).get("ingest_timestamp_utc"))
    )
    missing_segment_id_for_segment_nodes = sum(
        1
        for node in graph_payload["nodes"]
        if node.get("type") == "SEGMENT" and node.get("provenance", {}).get("segment_id") in (None, "")
    )
    invalid_segment_id_for_document_nodes = sum(
        1
        for node in graph_payload["nodes"]
        if node.get("type") == "DOCUMENT" and node.get("provenance", {}).get("segment_id") != DOCUMENT_NODE_SEGMENT_SENTINEL
    )
    missing_required_document_keys = sum(
        1
        for document in corpus_payload["documents"]
        if not {"doc_id", "source_archive", "source_document", "segments"}.issubset(document.keys())
    )
    missing_required_segment_keys = sum(
        1
        for document in corpus_payload["documents"]
        for segment in document.get("segments", [])
        if not {"segment_id", "text", "content_hash", "node_refs"}.issubset(segment.keys())
    )
    missing_required_node_keys = sum(
        1 for node in graph_payload["nodes"] if not {"id", "type", "content_hash", "provenance"}.issubset(node.keys())
    )
    missing_required_edge_keys = sum(
        1 for edge in graph_payload["edges"] if not {"source", "target", "type", "strength"}.issubset(edge.keys())
    )

    defects = (
        invalid_node_types
        + invalid_edge_types
        + invalid_edge_strengths
        + invalid_ref_types
        + invalid_ref_scores
        + missing_node_refs
        + missing_run_id
        + missing_ingest_timestamp
        + invalid_ingest_timestamp_format
        + missing_segment_id_for_segment_nodes
        + invalid_segment_id_for_document_nodes
        + missing_required_document_keys
        + missing_required_segment_keys
        + missing_required_node_keys
        + missing_required_edge_keys
    )
    integrity = 1.0 if defects == 0 else 0.0
    return integrity, {
        "invalid_node_types": invalid_node_types,
        "invalid_edge_types": invalid_edge_types,
        "invalid_edge_strengths": invalid_edge_strengths,
        "invalid_ref_types": invalid_ref_types,
        "invalid_ref_scores": invalid_ref_scores,
        "missing_node_refs": missing_node_refs,
        "missing_run_id": missing_run_id,
        "missing_ingest_timestamp": missing_ingest_timestamp,
        "invalid_ingest_timestamp_format": invalid_ingest_timestamp_format,
        "missing_segment_id_for_segment_nodes": missing_segment_id_for_segment_nodes,
        "invalid_segment_id_for_document_nodes": invalid_segment_id_for_document_nodes,
        "missing_required_document_keys": missing_required_document_keys,
        "missing_required_segment_keys": missing_required_segment_keys,
        "missing_required_node_keys": missing_required_node_keys,
        "missing_required_edge_keys": missing_required_edge_keys,
    }


def compute_status(coverage_ratio: float, artifact_integrity: float, reference_consistency: float, transition_check_success: float) -> tuple[float, str]:
    score = (
        (0.3 * coverage_ratio)
        + (0.3 * artifact_integrity)
        + (0.2 * reference_consistency)
        + (0.2 * transition_check_success)
    )
    if coverage_ratio < 0.7 or artifact_integrity < 1.0:
        return score, "INCOMPLETE"
    if score >= 0.90:
        return score, "VALID"
    if score >= 0.70:
        return score, "DEGRADED"
    return score, "REJECTED"


def check_pass_ratio(checks: list[Check]) -> float:
    return sum(1 for check in checks if check.result == "PASS") / len(checks) if checks else 1.0
