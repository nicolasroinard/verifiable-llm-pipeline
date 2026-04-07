from __future__ import annotations

from collections import Counter, defaultdict

from src.utils import stable, count_terms


def build_semantic_index(context) -> dict:
    term_refs: dict[str, Counter] = defaultdict(Counter)
    for segment in context.source_segments:
        counts = count_terms(segment.text)
        for term, count in counts.items():
            term_refs[term][("NODE", segment.node_id)] += count
            term_refs[term][("DOCUMENT", segment.doc_id)] += count

    index_entries: list[dict] = []
    for term in sorted(term_refs):
        counter = term_refs[term]
        max_count = max(counter.values()) if counter else 1
        references = []
        for (ref_type, ref_id), count in sorted(counter.items(), key=lambda kv: (stable(kv[0][0]), stable(kv[0][1]))):
            references.append({"ref_type": ref_type, "ref_id": ref_id, "score": round(count / max_count, 6)})
        index_entries.append({"term": term, "references": references})
    return {"index": index_entries}
