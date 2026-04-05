"""manifests.py — Archive integrity manifests for the ODT V7.5 pipeline.

Responsible for:
- Computing per-file SHA-256 hashes across the output directory
- Computing the master SHA-256 fingerprint
- Writing CHAIN_INTEGRITY_MANIFEST.json, ARCHIVE_MANIFEST.json, MASTER_SHA256.txt
- Cross-validating the frozen archive (FINAL_VALIDATION)
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from src.utils import sha256_file, write_json, write_text
from src.protocol import FREEZE_POLICY, FORENSIC_EXTENSION_POLICY, TOP_LEVEL_MANIFEST_FILES


def compute_all_relative_files(output_dir: Path) -> list:
    """Return a sorted list of POSIX-relative paths for every file under *output_dir*."""
    return [
        path.relative_to(output_dir).as_posix()
        for path in sorted(output_dir.rglob("*"))
        if path.is_file()
    ]


def compute_chain_file_hashes(output_dir: Path) -> dict:
    """Return a {relative_path: sha256} dict for all files except TOP_LEVEL_MANIFEST_FILES."""
    file_hashes = {}
    for rel in compute_all_relative_files(output_dir):
        if rel in TOP_LEVEL_MANIFEST_FILES:
            continue
        file_hashes[rel] = sha256_file(output_dir / rel)
    return file_hashes


def compute_master_sha(output_dir: Path) -> str:
    """Compute a single SHA-256 over all chained file hashes, sorted by path.

    This is the canonical integrity fingerprint of the whole archive, excluding
    the top-level self-referential manifest files.
    """
    components = []
    for rel, file_hash in sorted(compute_chain_file_hashes(output_dir).items()):
        components.append(f"{rel}:{file_hash}")
    return hashlib.sha256("\n".join(components).encode("utf-8")).hexdigest()


def write_manifests(
    output_dir: Path,
    system_global_id: str,
    document_id: str,
    timestamp: str,
) -> None:
    """Write CHAIN_INTEGRITY_MANIFEST.json, ARCHIVE_MANIFEST.json, and MASTER_SHA256.txt.

    Called twice during the pipeline: once before INTERNAL_EXPERT_AUDIT and once
    after, so that the final manifests incorporate every file including the audit log.
    """
    chain_hashes = compute_chain_file_hashes(output_dir)
    chain_manifest = {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "status": "FROZEN",
        "freeze_policy": FREEZE_POLICY,
        "chain_integrity_manifest": chain_hashes,
        "feeds": ["GLOBAL_SYSTEM_STATUS", "ODT_LAYER_STATUS", "ARCHIVE_MANIFEST"],
        "exclusions": sorted(TOP_LEVEL_MANIFEST_FILES),
        "chain_file_count": len(chain_hashes),
    }
    write_json(output_dir / "CHAIN_INTEGRITY_MANIFEST.json", chain_manifest)
    archive_manifest = {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "status": "FROZEN",
        "freeze_policy": FREEZE_POLICY,
        "auto_referential_exclusions": sorted(TOP_LEVEL_MANIFEST_FILES),
        "archive_complete_v7_5": [],
        "final_inventory": [],
        "extension_policy": FORENSIC_EXTENSION_POLICY,
    }
    write_json(output_dir / "ARCHIVE_MANIFEST.json", archive_manifest)
    master_sha = compute_master_sha(output_dir)
    write_text(output_dir / "MASTER_SHA256.txt", master_sha + "\n")
    final_files = compute_all_relative_files(output_dir)
    archive_manifest["archive_complete_v7_5"] = final_files
    archive_manifest["final_inventory"] = final_files
    archive_manifest["final_inventory_count"] = len(final_files)
    write_json(output_dir / "ARCHIVE_MANIFEST.json", archive_manifest)


def build_final_validation(
    output_dir: Path,
    source_odt_copy: Path,
    source_odt_original: Path,
    system_global_id: str,
    document_id: str,
    timestamp: str,
) -> dict:
    """Cross-validate the frozen archive against its own manifests.

    Checks performed:
      - archive manifest file list matches actual files on disk
      - all chain hashes match the hashed files
      - MASTER_SHA256.txt matches recomputed master SHA
      - GLOBAL_SYSTEM_STATUS is VALID and carries the correct SYSTEM_GLOBAL_ID
      - ODT source copy is byte-identical to the original input
      - timestamp is well-formed UTC ISO 8601

    Returns the validation dict; status is VALID only if all checks pass.
    """
    archive_manifest = json.loads(
        (output_dir / "ARCHIVE_MANIFEST.json").read_text(encoding="utf-8")
    )
    chain_manifest = json.loads(
        (output_dir / "CHAIN_INTEGRITY_MANIFEST.json").read_text(encoding="utf-8")
    )
    actual_files = compute_all_relative_files(output_dir)
    recorded_files = archive_manifest["archive_complete_v7_5"]
    archive_manifest_complete = sorted(recorded_files) == sorted(actual_files)
    chain_mismatches = []
    for rel, expected_hash in chain_manifest["chain_integrity_manifest"].items():
        actual_hash = sha256_file(output_dir / rel)
        if expected_hash != actual_hash:
            chain_mismatches.append({"path": rel, "expected": expected_hash, "actual": actual_hash})
    chain_ok = len(chain_mismatches) == 0
    master_expected = compute_master_sha(output_dir)
    master_actual = (output_dir / "MASTER_SHA256.txt").read_text(encoding="utf-8").strip()
    master_ok = master_expected == master_actual
    global_system_status = json.loads(
        (output_dir / "archive" / "GLOBAL_SYSTEM_STATUS.json").read_text(encoding="utf-8")
    )
    system_core_ok = (
        global_system_status.get("SYSTEM_GLOBAL_ID") == system_global_id
        and global_system_status.get("status") == "VALID"
    )
    odt_preserved = (
        source_odt_copy.exists()
        and sha256_file(source_odt_copy) == sha256_file(source_odt_original)
    )
    validation = {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "freeze_policy": FREEZE_POLICY,
        "checks": {
            "integrite_complete": archive_manifest_complete and chain_ok,
            "tracabilite_complete": bool(recorded_files) and system_core_ok,
            "reconstruction_sha256_valide": master_ok,
            "conformite_system_core": system_core_ok,
            "odt_native_preserved": odt_preserved,
            "utc_iso_8601": bool(
                re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", timestamp)
            ),
        },
        "details": {
            "archive_manifest_complete": archive_manifest_complete,
            "recorded_archive_file_count": len(recorded_files),
            "actual_archive_file_count": len(actual_files),
            "chain_manifest_entries": len(chain_manifest["chain_integrity_manifest"]),
            "chain_manifest_mismatches": chain_mismatches,
            "master_sha_expected": master_expected,
            "master_sha_actual": master_actual,
            "crypto_scope_exclusions": sorted(TOP_LEVEL_MANIFEST_FILES),
            "final_inventory_count": len(actual_files),
            "self_reference_policy_explicit": True,
        },
    }
    validation["status"] = "VALID" if all(validation["checks"].values()) else "REJECTED"
    return validation
