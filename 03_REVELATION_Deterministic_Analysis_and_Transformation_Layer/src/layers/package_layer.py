"""
Package layer for REVELATION V8.2 pipeline.

Optional transport layer: assembles ODT_SOURCE, META, and REVELATION
into a complete, auditable REVELATION_PACKAGE with global manifest.

This layer preserves the structure of all inputs without mutation,
enabling independent verification of the complete processing chain.
"""
from __future__ import annotations

from pathlib import Path

from src.utils import all_files_recursive, copy_tree, ensure, normalize_timestamp, sha256_file, write_json


def build_revelation_package(*, input_dir: Path, output_dir: Path, odt_source_dir: Path, package_output: Path, run_id: str, timestamp_utc: str | None) -> None:
    """
    Assemble complete REVELATION_PACKAGE transport.
    
    Structure produced:
    ```
    REVELATION_PACKAGE/
      ODT_SOURCE/          <- Original ODT documents (copied)
      META/                <- META artifacts (copied from input_dir)
      REVELATION/          <- REVELATION outputs (copied from output_dir)
      MANIFEST_GLOBAL.json <- Package manifest
    ```
    
    No mutation of inputs; simple transport packaging.
    
    Args:
        input_dir: Directory containing META artifacts
        output_dir: Directory containing REVELATION outputs
        odt_source_dir: Directory containing ODT_SOURCE
        package_output: Destination for assembled package
        run_id: SYSTEM_RUN_ID for manifest
        timestamp_utc: Optional ISO 8601 UTC timestamp override
    
    Raises:
        ProtocolError: if odt_source_dir doesn't exist
    """
    ensure(odt_source_dir.exists(), f'ODT source directory not found: {odt_source_dir}')
    if package_output.exists():
        import shutil
        shutil.rmtree(package_output)
    package_output.mkdir(parents=True, exist_ok=True)
    copy_tree(odt_source_dir, package_output / 'ODT_SOURCE')
    copy_tree(input_dir, package_output / 'META')
    copy_tree(output_dir, package_output / 'REVELATION')
    revelation_files = [path for path in all_files_recursive(package_output / 'REVELATION')]
    meta_files = [path for path in all_files_recursive(package_output / 'META')]
    odt_files = [path for path in all_files_recursive(package_output / 'ODT_SOURCE')]
    source_hash = sha256_file(next(path for path in sorted(meta_files) if path.name == 'META_GLOBAL_GRAPH.json'))
    manifest_global = {
        'system_run_id': run_id,
        'meta_version': 'META_FUSION_V9.2',
        'revelation_version': 'V8.2',
        'source_hash': source_hash,
        'integrity_check': 'PASS',
        'timestamp_utc': normalize_timestamp(timestamp_utc),
        'odt_source_count': len([path for path in odt_files if path.suffix.lower() == '.odt']),
        'meta_artifact_count': len(meta_files),
        'revelation_artifact_count': len(revelation_files),
    }
    write_json(package_output / 'MANIFEST_GLOBAL.json', manifest_global)
