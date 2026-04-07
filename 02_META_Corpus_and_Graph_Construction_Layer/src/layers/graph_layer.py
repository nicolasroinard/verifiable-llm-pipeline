from __future__ import annotations


def add_dedup_edges(edges: list[dict], dedup_events: list[dict]) -> None:
    for event in dedup_events:
        refs = event["node_refs"]
        for left, right in zip(refs, refs[1:]):
            edges.append({"source": left, "target": right, "type": "DUPLICATION", "strength": 1.0})


def build_graph_payload(nodes: list[dict], edges: list[dict]) -> dict:
    ordered_nodes = sorted(nodes, key=lambda item: (item.get("type") or "", item.get("id") or ""))
    ordered_edges = sorted(edges, key=lambda item: (item.get("type") or "", item.get("source") or "", item.get("target") or ""))
    return {"nodes": ordered_nodes, "edges": ordered_edges}
