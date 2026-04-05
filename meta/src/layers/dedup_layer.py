from __future__ import annotations

from src.utils import stable


def build_dedup_events(context, hash_groups: dict) -> list[dict]:
    dedup_events: list[dict] = []
    for content_hash, items in hash_groups.items():
        ordered = sorted(items, key=lambda item: (stable(item.source_archive), stable(item.doc_id), stable(item.segment_id), stable(item.node_id)))
        if len(ordered) > 1:
            dedup_events.append(
                {
                    "content_hash": content_hash,
                    "duplicate_count": len(ordered),
                    "node_refs": [item.node_id for item in ordered],
                }
            )
    return dedup_events
