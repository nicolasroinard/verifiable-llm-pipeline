from __future__ import annotations

from src.models import FusionContext


def normalize_records(context: FusionContext) -> None:
    """Records are already normalized into SourceArchive and SourceSegment dataclasses.

    This layer remains explicit to preserve the canonical META pipeline order and to host
    future additive normalizations without changing the CLI contract.
    """
    _ = context
