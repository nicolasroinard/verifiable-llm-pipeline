from __future__ import annotations

from collections import defaultdict

from src.models import FusionContext
from src.utils import DOCUMENT_NODE_SEGMENT_SENTINEL, stable


def build_corpus_and_graph_seed(context: FusionContext) -> tuple[dict, list[dict], list[dict]]:
    nodes: list[dict] = []
    edges: list[dict] = []
    node_ids: set[str] = set()

    def add_node(node: dict) -> None:
        if node["id"] not in node_ids:
            nodes.append(node)
            node_ids.add(node["id"])

    def add_edge(edge: dict) -> None:
        edges.append(edge)

    segments_by_doc = defaultdict(list)
    for segment in context.source_segments:
        segments_by_doc[segment.doc_id].append(segment)

    documents_payload: list[dict] = []
    for source_archive in context.source_archives:
        doc_node_id = f"NODE::DOCUMENT::{source_archive.doc_id}"
        add_node(
            {
                "id": doc_node_id,
                "type": "DOCUMENT",
                "content_hash": source_archive.source_file_sha256,
                "provenance": {
                    "archive_id": source_archive.archive_id,
                    "document_id": source_archive.doc_id,
                    "segment_id": DOCUMENT_NODE_SEGMENT_SENTINEL,
                    "source_hash": source_archive.source_file_sha256,
                    "ingest_timestamp_utc": source_archive.ingest_timestamp_utc,
                    "run_id": context.run_id,
                },
            }
        )

        doc_payload = {
            "doc_id": source_archive.doc_id,
            "source_archive": source_archive.source_archive,
            "source_document": source_archive.source_document,
            "segments": [],
        }
        for segment in sorted(segments_by_doc[source_archive.doc_id], key=lambda item: (stable(item.segment_id), stable(item.content_hash))):
            segment_node_id = segment.node_id
            add_node(
                {
                    "id": segment_node_id,
                    "type": "SEGMENT",
                    "content_hash": segment.content_hash,
                    "provenance": {
                        "archive_id": segment.archive_id,
                        "document_id": segment.doc_id,
                        "segment_id": segment.segment_id,
                        "source_hash": segment.content_hash,
                        "ingest_timestamp_utc": segment.ingest_timestamp_utc,
                        "run_id": context.run_id,
                    },
                }
            )
            add_edge({"source": segment_node_id, "target": doc_node_id, "type": "REFERENCE", "strength": 1.0})
            doc_payload["segments"].append(
                {
                    "segment_id": segment.segment_id,
                    "text": segment.text,
                    "content_hash": segment.content_hash,
                    "node_refs": [segment_node_id],
                }
            )
        documents_payload.append(doc_payload)

    return {"documents": documents_payload}, nodes, edges
