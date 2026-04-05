from __future__ import annotations


def build_delta_report(dedup_events: list[dict], edges: list[dict], run_id: str) -> dict:
    return {
        "added_links": [
            {"type": "REFERENCE", "count": sum(1 for edge in edges if edge["type"] == "REFERENCE")},
            {"type": "DUPLICATION", "count": sum(1 for edge in edges if edge["type"] == "DUPLICATION")},
        ],
        "dedup_events": dedup_events,
        "conflict_sets": [],
        "unresolved_items": [],
        "version_before": "ODT_V7_5",
        "version_after": "META_FUSION_V9_2_FINAL_LOCK",
        "run_id": run_id,
    }
