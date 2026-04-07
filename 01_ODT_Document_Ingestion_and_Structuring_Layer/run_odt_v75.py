"""run_odt_v75.py — ODT V7.5 deterministic archive pipeline.

Entry point. Orchestrates the pipeline in strict protocol order.
All business logic lives in src/.

Usage:
    python run_odt_v75.py --input path/to/file.odt --output out_dir
    python run_odt_v75.py --input path/to/file.odt --output out_dir --reproducible
    python run_odt_v75.py --input path/to/file.odt --output out_dir \\
        --reproducible --fixed-timestamp 2000-01-01T00:00:00Z
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import zipfile
from pathlib import Path

from src.builders import (
    build_archive_stats,
    build_artefact_exec_meta,
    build_artefact_relations,
    build_boot_summary,
    build_corpus,
    build_extraction_log,
    build_finalization_policy,
    build_global_system_status,
    build_index_master,
    build_input_validation_record,
    build_internal_expert_audit,
    build_odt_layer_status,
    build_paragraph_objects,
    build_pipeline_exec_meta,
    build_preliminary_validation,
    build_query_map,
    build_segment_objects,
    build_source_metadata,
    build_integrity_report,
    build_system_version,
    update_global_status_with_validation,
)
from src.extractor import extract_text_blocks, validate_input
from src.ids import build_ids
from src.manifests import build_final_validation, write_manifests
from src.utils import ensure_dir, sha256_bytes, write_json, write_text

log = logging.getLogger(__name__)


def _setup_logging(verbose: bool) -> None:
    """Configure root logger: DEBUG if verbose, INFO otherwise."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        level=level,
        stream=sys.stderr,
    )


def build_archive(
    odt_path: Path,
    output_dir: Path,
    reproducible: bool = False,
    fixed_timestamp: str | None = None,
    system_global_id_override: str | None = None,
) -> dict:
    """Execute the full ODT V7.5 deterministic pipeline and write the archive.

    Pipeline order (must not be altered):
      1.  BUILD_IDS                - derive SYSTEM_GLOBAL_ID, document_id, timestamp
      2.  INPUT_VALIDATION         - verify ODT ZIP integrity and mandatory entries
      3.  ZIP_EXTRACTION           - read all entries, compute SHA-256 per entry
      4.  TEXT_EXTRACTION          - parse content.xml into ordered paragraph blocks
      5.  ARTEFACT_GENERATION      - build paragraphs, segments, and artefact lists
      6.  STRUCTURE_EXPORT         - source metadata, integrity report, structure tree
      7.  EXECUTION_LOGS           - extraction log, pipeline meta, artefact exec meta
      8.  EXPLOITATION_EXPORT      - corpus, index, query map, relations, policies
      9.  WRITE_ALL                - flush all JSON/TXT artefacts to disk
      10. WRITE_MANIFESTS (pass 1)  - chain manifest, archive manifest, master SHA
      11. FINAL_VALIDATION         - cross-check all hashes and manifest completeness
      12. INTERNAL_EXPERT_AUDIT    - aggregate audit with orphan checks
      13. WRITE_MANIFESTS (pass 2)  - re-freeze manifests including the audit log
      14. FINAL_VALIDATION (pass 2) - final authoritative validation
      15. GLOBAL_STATUS update      - record final validation status

    Returns a summary dict: SYSTEM_GLOBAL_ID, document_id, timestamp, status,
    output_dir, reproducible.
    """
    # --- 1. BUILD_IDS ---
    log.info("Step 1/15  BUILD_IDS")
    system_global_id, document_id, timestamp = build_ids(
        odt_path,
        reproducible=reproducible,
        fixed_timestamp=fixed_timestamp,
        system_global_id_override=system_global_id_override,
    )
    log.debug("  SYSTEM_GLOBAL_ID : %s", system_global_id)
    log.debug("  document_id      : %s", document_id)
    log.debug("  timestamp        : %s", timestamp)

    # --- Repertoires de sortie ---
    source_dir    = output_dir / "SOURCE" / "ODT_NATIVE"
    archive_dir   = output_dir / "archive"
    artefacts_dir = output_dir / "artefacts"
    structure_dir = output_dir / "structure"
    meta_dir      = output_dir / "meta"
    logs_dir      = output_dir / "logs"
    bootstrap_dir = output_dir / "bootstrap"
    for folder in [source_dir, archive_dir, artefacts_dir, structure_dir,
                   meta_dir, logs_dir, bootstrap_dir]:
        ensure_dir(folder)

    # --- 1b. SOURCE - copie native ODT ---
    shutil.copy2(odt_path, source_dir / odt_path.name)
    log.debug("  ODT copied to SOURCE/ODT_NATIVE/")

    # --- 2. INPUT_VALIDATION ---
    log.info("Step 2/15  INPUT_VALIDATION")
    validation = validate_input(odt_path)
    input_validation = build_input_validation_record(
        system_global_id, document_id, timestamp, odt_path, validation
    )
    write_json(logs_dir / "INPUT_VALIDATION.json", input_validation)
    if input_validation["status"] != "VALID":
        log.error("Input validation REJECTED: %s", validation["errors"])
        raise RuntimeError("Input validation failed; ODT unreadable or corrupted.")
    log.debug("  validation OK")

    # --- 3. ZIP_EXTRACTION ---
    log.info("Step 3/15  ZIP_EXTRACTION")
    with zipfile.ZipFile(odt_path, "r") as zf:
        names = zf.namelist()
        entries = []
        archive_map = {}
        extracted = {}
        for name in names:
            info = zf.getinfo(name)
            data = b"" if name.endswith("/") else zf.read(name)
            entries.append({
                "path": name,
                "is_dir": name.endswith("/"),
                "compressed_size": info.compress_size,
                "file_size": info.file_size,
                "CRC": info.CRC,
                "sha256": None if name.endswith("/") else sha256_bytes(data),
            })
            if not name.endswith("/"):
                archive_map[name] = {"sha256": sha256_bytes(data), "size": len(data)}
                extracted[name] = data
    log.debug("  %d entries extracted", len(entries))

    # --- 4. TEXT_EXTRACTION ---
    log.info("Step 4/15  TEXT_EXTRACTION")
    paragraphs_raw = extract_text_blocks(extracted["content.xml"])
    text_length = sum(len(item["text"]) for item in paragraphs_raw)
    log.debug("  %d raw text blocks, %d chars", len(paragraphs_raw), text_length)

    # --- 5. ARTEFACT_GENERATION ---
    log.info("Step 5/15  ARTEFACT_GENERATION")
    paragraphs, artefact_paragraphs, artefact_master = build_paragraph_objects(
        system_global_id, document_id, timestamp, paragraphs_raw
    )
    segments, artefact_segments = build_segment_objects(
        system_global_id, document_id, timestamp, paragraphs
    )
    log.debug("  %d paragraphs  %d segments", len(paragraphs), len(segments))

    # --- 6. STRUCTURE_EXPORT ---
    log.info("Step 6/15  STRUCTURE_EXPORT")
    source_metadata  = build_source_metadata(system_global_id, document_id, timestamp, odt_path)
    integrity_report = build_integrity_report(
        system_global_id, document_id, timestamp, odt_path, archive_map
    )

    # --- 7. EXECUTION_LOGS ---
    log.info("Step 7/15  EXECUTION_LOGS")
    extraction_log     = build_extraction_log(
        system_global_id, document_id, timestamp, entries, paragraphs, segments
    )
    pipeline_exec_meta = build_pipeline_exec_meta(system_global_id, document_id, timestamp)
    artefact_exec_meta = build_artefact_exec_meta(
        system_global_id, document_id, timestamp,
        artefact_master, artefact_paragraphs, artefact_segments,
    )

    # --- 8. EXPLOITATION_EXPORT ---
    log.info("Step 8/15  EXPLOITATION_EXPORT")
    corpus               = build_corpus(
        system_global_id, document_id, timestamp, odt_path, paragraphs, segments
    )
    index_master         = build_index_master(
        system_global_id, document_id, timestamp, paragraphs, segments, archive_map
    )
    query_map            = build_query_map(system_global_id, document_id, timestamp, segments)
    finalization_policy  = build_finalization_policy(system_global_id, document_id, timestamp)
    artefact_relations   = build_artefact_relations(
        system_global_id, document_id, timestamp, paragraphs, segments
    )
    archive_stats        = build_archive_stats(
        system_global_id, document_id, timestamp,
        entries, paragraphs, segments, text_length,
        artefact_master, artefact_paragraphs, artefact_segments,
    )
    boot_summary         = build_boot_summary(
        system_global_id, document_id, timestamp, odt_path, entries, paragraphs, segments
    )
    global_system_status = build_global_system_status(
        system_global_id, document_id, timestamp, entries, paragraphs, segments
    )
    odt_layer_status     = build_odt_layer_status(system_global_id, document_id, timestamp)
    system_version       = build_system_version(timestamp)

    # --- 9. WRITE_ALL - ecriture sur disque ---
    log.info("Step 9/15  WRITE_ALL")
    write_json(archive_dir   / "GLOBAL_SYSTEM_STATUS.json",        global_system_status)
    write_json(artefacts_dir / "ARTEFACT_MASTER.json",             artefact_master)
    write_json(artefacts_dir / "ARTEFACT_paragraphs.json",         artefact_paragraphs)
    write_json(artefacts_dir / "ARTEFACT_segments.json",           artefact_segments)
    write_json(structure_dir / "ARTEFACT_STRUCTURE_TREE.json",     entries)
    write_json(structure_dir / "ARCHIVE_STRUCTURE_MAP.json",       archive_map)
    write_json(structure_dir / "SOURCE_DOCUMENT_METADATA.json",    source_metadata)
    write_json(structure_dir / "ARTEFACT_INTEGRITY_REPORT.json",   integrity_report)
    write_json(meta_dir      / "GLOBAL_ARCHIVE_STATS.json",        archive_stats)
    write_json(meta_dir      / "GLOBAL_DOCUMENT_CORPUS.json",      corpus)
    write_json(meta_dir      / "ARCHIVE_INDEX_MASTER.json",        index_master)
    write_json(meta_dir      / "QUERY_MAP.json",                   query_map)
    write_json(meta_dir      / "FINALIZATION_POLICY.json",         finalization_policy)
    write_json(meta_dir      / "ARTEFACT_RELATIONS.json",          artefact_relations)
    write_json(logs_dir      / "ARTEFACT_EXTRACTION_LOG.json",     extraction_log)
    write_json(logs_dir      / "PIPELINE_EXECUTION_META.json",     pipeline_exec_meta)
    write_json(logs_dir      / "ARTEFACT_EXECUTION_META.json",     artefact_exec_meta)
    write_text(bootstrap_dir / "DOSSIER_BOOT_SUMMARY.txt",         boot_summary)
    write_text(output_dir    / "SYSTEM_GLOBAL_ID.txt",             system_global_id)
    write_json(output_dir    / "ODT_LAYER_STATUS.json",            odt_layer_status)
    write_json(output_dir    / "SYSTEM_VERSION.json",              system_version)

    # --- 10. VALIDATION FINALE - etat preliminaire ---
    log.info("Step 10/15 VALIDATION_FINALE (preliminary)")
    preliminary_validation = build_preliminary_validation(
        system_global_id, document_id, timestamp,
        source_dir, odt_path, entries, paragraphs, segments, reproducible,
    )
    write_json(logs_dir / "VALIDATION_FINALE.json", preliminary_validation)

    # --- 11. WRITE_MANIFESTS (passe 1) ---
    log.info("Step 11/15 WRITE_MANIFESTS (pass 1)")
    write_manifests(output_dir, system_global_id, document_id, timestamp)

    # --- 12. FINAL_VALIDATION (passe 1) ---
    log.info("Step 12/15 FINAL_VALIDATION (pass 1)")
    final_validation = build_final_validation(
        output_dir=output_dir,
        source_odt_copy=source_dir / odt_path.name,
        source_odt_original=odt_path,
        system_global_id=system_global_id,
        document_id=document_id,
        timestamp=timestamp,
    )
    write_json(logs_dir / "VALIDATION_FINALE.json", final_validation)
    update_global_status_with_validation(global_system_status, final_validation)
    write_json(archive_dir / "GLOBAL_SYSTEM_STATUS.json", global_system_status)
    log.debug("  pass-1 status: %s", final_validation["status"])

    # --- 13. INTERNAL_EXPERT_AUDIT ---
    log.info("Step 13/15 INTERNAL_EXPERT_AUDIT")
    internal_expert_audit = build_internal_expert_audit(
        paragraphs=paragraphs,
        segments=segments,
        archive_entries=entries,
        archive_map=archive_map,
        final_validation=final_validation,
        reproducible=reproducible,
        output_dir=output_dir,
    )
    write_json(logs_dir / "INTERNAL_EXPERT_AUDIT.json", internal_expert_audit)
    log.debug("  audit status: %s", internal_expert_audit["status"])

    # --- 14. WRITE_MANIFESTS (passe 2 - integre l'audit) ---
    log.info("Step 14/15 WRITE_MANIFESTS (pass 2)")
    write_manifests(output_dir, system_global_id, document_id, timestamp)

    # --- 15. FINAL_VALIDATION (passe 2 - validation autoritaire finale) ---
    log.info("Step 15/15 FINAL_VALIDATION (pass 2 - authoritative)")
    final_validation = build_final_validation(
        output_dir=output_dir,
        source_odt_copy=source_dir / odt_path.name,
        source_odt_original=odt_path,
        system_global_id=system_global_id,
        document_id=document_id,
        timestamp=timestamp,
    )
    write_json(logs_dir / "VALIDATION_FINALE.json", final_validation)
    update_global_status_with_validation(global_system_status, final_validation)
    write_json(archive_dir / "GLOBAL_SYSTEM_STATUS.json", global_system_status)
    log.debug("  pass-2 status: %s", final_validation["status"])

    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "status": "VALID",
        "output_dir": str(output_dir),
        "reproducible": reproducible,
    }


def main() -> None:
    """CLI entry point for the ODT V7.5 archive builder."""
    parser = argparse.ArgumentParser(
        description="ODT V7.5 - deterministic archive builder",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Standard run:
    python run_odt_v75.py --input doc.odt --output out/

  Reproducible run (deterministic output for identical inputs):
    python run_odt_v75.py --input doc.odt --output out/ --reproducible

  Reproducible with fixed timestamp:
    python run_odt_v75.py --input doc.odt --output out/ \\
        --reproducible --fixed-timestamp 2000-01-01T00:00:00Z
        """,
    )
    parser.add_argument("--input",  required=True, metavar="ODT",
                        help="Path to the source .odt file")
    parser.add_argument("--output", required=True, metavar="DIR",
                        help="Output directory for ARCHIVE_COMPLETE_V7_5")
    parser.add_argument("--reproducible", action="store_true",
                        help="Produce identical outputs for identical inputs")
    parser.add_argument("--fixed-timestamp", metavar="TS",
                        help="UTC ISO 8601 timestamp for --reproducible mode")
    parser.add_argument("--system-global-id", metavar="ID",
                        help="Optional override for SYSTEM_GLOBAL_ID")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable DEBUG-level logging to stderr")
    parser.add_argument("--quiet",   "-q", action="store_true",
                        help="Suppress all logging (stdout JSON result only)")
    args = parser.parse_args()

    if args.quiet:
        logging.disable(logging.CRITICAL)
    else:
        _setup_logging(verbose=args.verbose)

    odt_path = Path(args.input)
    out = Path(args.output)
    ensure_dir(out)

    result = build_archive(
        odt_path,
        out,
        reproducible=args.reproducible,
        fixed_timestamp=args.fixed_timestamp,
        system_global_id_override=args.system_global_id,
    )

    if not args.quiet:
        log.info("DONE  status=%s  output=%s", result["status"], result["output_dir"])

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
