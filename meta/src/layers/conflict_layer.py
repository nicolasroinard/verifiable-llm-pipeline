from __future__ import annotations


def build_conflict_sets() -> list[dict]:
    """No implicit conflict inference is allowed by protocol.

    MVP strategy: produce an empty explicit conflict set and leave unresolved items empty.
    """
    return []
