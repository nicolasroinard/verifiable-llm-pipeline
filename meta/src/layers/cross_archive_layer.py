from __future__ import annotations

from collections import defaultdict

from src.models import FusionContext


def build_cross_archive_links(context: FusionContext) -> dict:
    by_hash = defaultdict(list)
    for segment in context.source_segments:
        by_hash[segment.content_hash].append(segment)
    return {content_hash: items for content_hash, items in sorted(by_hash.items())}
