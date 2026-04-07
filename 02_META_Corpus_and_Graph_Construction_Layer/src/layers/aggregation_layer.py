from __future__ import annotations

from src.models import FusionContext
from src.utils import sort_records


def aggregate_sources(context: FusionContext) -> None:
    context.source_archives = sorted(
        context.source_archives,
        key=lambda item: (item.source_archive or "", item.doc_id or ""),
    )
    context.source_segments = sorted(
        context.source_segments,
        key=lambda item: (item.source_archive or "", item.doc_id or "", item.segment_id or ""),
    )
