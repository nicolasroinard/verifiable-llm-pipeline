"""protocol.py — Shared protocol constants for the ODT V7.5 pipeline.

These values are authoritative and must not be modified without a formal
protocol version change. All pipeline modules import from here.
"""
from __future__ import annotations

TOP_LEVEL_MANIFEST_FILES: frozenset = frozenset({
    "CHAIN_INTEGRITY_MANIFEST.json",
    "ARCHIVE_MANIFEST.json",
    "MASTER_SHA256.txt",
    "logs/VALIDATION_FINALE.json",
})

FORENSIC_EXTENSION_POLICY: dict = {
    "mode": "ADDITIVE_ONLY",
    "rule": (
        "Standard protocol files and fields remain unchanged; "
        "optional forensic enrichments are additive and non-blocking."
    ),
    "compatibility_target": ["ODT_V7_5", "META_FUSION_V9_2"],
}

FREEZE_POLICY: dict = {
    "mode": "FORENSIC_FINAL_FREEZE",
    "description": (
        "Final manifest and master hash are computed on frozen artefacts "
        "with explicit self-reference exclusions."
    ),
    "auto_referential_exclusions": sorted(TOP_LEVEL_MANIFEST_FILES),
    "archive_manifest_status": "FROZEN",
}
