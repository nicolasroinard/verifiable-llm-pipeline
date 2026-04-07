"""builders.py — Pure dict builders for the ODT V7.5 pipeline.

Each function constructs one protocol record without writing anything to disk.
The calling order inside build_archive() must remain unchanged.

Layers covered (in pipeline order):
  INPUT_VALIDATION, ARTEFACT_GENERATION, STRUCTURE_EXPORT,
  EXECUTION, EXPLOITATION, ODT, VALIDATION_FINALE, AUDIT
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path

from src.extractor import deterministic_segments
from src.protocol import (
    FREEZE_POLICY,
    FORENSIC_EXTENSION_POLICY,
    TOP_LEVEL_MANIFEST_FILES,
)
from src.utils import sha256_file


# ---------------------------------------------------------------------------
# INPUT_VALIDATION
# ---------------------------------------------------------------------------

def build_input_validation_record(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    odt_path: Path,
    validation: dict,
) -> dict:
    """Couche INPUT_VALIDATION — enveloppe le résultat de validate_input()."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "odt_path": str(odt_path),
        "validation": validation,
        "status": "VALID"
        if all([
            validation["readable"],
            validation["zip_valid"],
            validation["mandatory_entries_present"],
        ])
        else "REJECTED",
    }


# ---------------------------------------------------------------------------
# ARTEFACT_GENERATION
# ---------------------------------------------------------------------------

def build_paragraph_objects(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    paragraphs_raw: list,
) -> tuple[list, list, list]:
    """Couche ARTEFACT_GENERATION (paragraphes).

    Retourne (paragraphs, artefact_paragraphs, artefact_master) dans cet ordre.
    """
    paragraphs: list = []
    artefact_paragraphs: list = []
    artefact_master: list = []
    for idx, paragraph in enumerate(paragraphs_raw, start=1):
        paragraph_id = f"P_{idx:05d}"
        hash_segment = hashlib.sha256(paragraph["text"].encode("utf-8")).hexdigest()
        paragraph_position = {
            "paragraph_sequence": idx,
            "absolute_sequence": paragraph["absolute_sequence"],
            "xml_path": paragraph["xml_path"],
        }
        paragraphs.append(
            {
                "SYSTEM_GLOBAL_ID": system_global_id,
                "document_id": document_id,
                "paragraph_id": paragraph_id,
                "sequence": idx,
                "kind": paragraph["kind"],
                "xml_tag": paragraph["xml_tag"],
                "style_name": paragraph["style_name"],
                "text": paragraph["text"],
                "hash_segment": hash_segment,
                "timestamp": timestamp,
                "source_position": paragraph_position,
                "provenance_mode": "direct",
                "traceability_level": "high",
                "reconstructibility_path": {
                    "type": "direct_from_content_xml",
                    "source_file": "content.xml",
                    "xml_path": paragraph["xml_path"],
                    "method": "itertext_on_single_node",
                },
            }
        )
        artefact_paragraphs.append(
            {
                "SYSTEM_GLOBAL_ID": system_global_id,
                "document_id": document_id,
                "artefact_id": f"ARTEFACT_PARAGRAPH_{idx:05d}",
                "source_segment_id": paragraph_id,
                "type_transformation": "ODT_PARAGRAPH_EXTRACT",
                "hash_segment": hash_segment,
                "timestamp": timestamp,
                "provenance": {
                    "position_exacte_dans_odt": paragraph_position,
                    "niveau_transformation": "paragraph",
                    "provenance_mode": "direct",
                    "traceability_level": "high",
                    "reconstructibility_path": {
                        "type": "direct_from_content_xml",
                        "source_file": "content.xml",
                        "xml_path": paragraph["xml_path"],
                        "method": "itertext_on_single_node",
                    },
                    "relations": [],
                },
                "content": paragraph["text"],
            }
        )
        artefact_master.append(
            {
                "SYSTEM_GLOBAL_ID": system_global_id,
                "document_id": document_id,
                "artefact_id": f"ARTEFACT_MASTER_{idx:05d}",
                "source_segment_id": paragraph_id,
                "type_transformation": "ODT_MASTER_EXTRACT",
                "hash_segment": hash_segment,
                "timestamp": timestamp,
                "provenance": {
                    "position_exacte_dans_odt": paragraph_position,
                    "niveau_transformation": "master",
                    "provenance_mode": "document_level_direct",
                    "traceability_level": "document_level",
                    "reconstructibility_path": {
                        "type": "document_master_from_ordered_paragraphs",
                        "paragraph_id": paragraph_id,
                        "method": "ordered_single_paragraph_copy",
                    },
                    "relations": {
                        "derived_paragraph_artefact": f"ARTEFACT_PARAGRAPH_{idx:05d}",
                        "related_segment_artefacts": [],
                    },
                },
                "content": paragraph["text"],
            }
        )
    return paragraphs, artefact_paragraphs, artefact_master


def build_segment_objects(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    paragraphs: list,
) -> tuple[list, list]:
    """Couche ARTEFACT_GENERATION (segments).

    Retourne (segments, artefact_segments).
    """
    segments: list = []
    artefact_segments: list = []
    for seg_idx, seg_paragraphs, seg_text in deterministic_segments(paragraphs):
        segment_id = f"S_{seg_idx:05d}"
        seg_hash = hashlib.sha256(seg_text.encode("utf-8")).hexdigest()
        para_ids = [item["paragraph_id"] for item in seg_paragraphs]
        paragraph_xml_paths = [item["source_position"]["xml_path"] for item in seg_paragraphs]
        segment_position = {
            "segment_sequence": seg_idx,
            "paragraph_ids": para_ids,
            "paragraph_xml_paths": paragraph_xml_paths,
            "xml_path_range": {
                "start": paragraph_xml_paths[0],
                "end": paragraph_xml_paths[-1],
            },
            "paragraph_range": {
                "start": para_ids[0],
                "end": para_ids[-1],
            },
        }
        segments.append(
            {
                "SYSTEM_GLOBAL_ID": system_global_id,
                "document_id": document_id,
                "segment_id": segment_id,
                "sequence": seg_idx,
                "paragraph_ids": para_ids,
                "text": seg_text,
                "hash_segment": seg_hash,
                "timestamp": timestamp,
                "position_exacte_dans_odt": segment_position,
                "provenance_mode": "derived",
                "traceability_level": "high_derived",
                "reconstructibility_path": {
                    "type": "derived_from_paragraphs",
                    "source_paragraphs": para_ids,
                    "method": "ordered_concatenation_with_newlines",
                },
            }
        )
        artefact_segments.append(
            {
                "SYSTEM_GLOBAL_ID": system_global_id,
                "document_id": document_id,
                "artefact_id": f"ARTEFACT_SEGMENT_{seg_idx:05d}",
                "source_segment_id": segment_id,
                "type_transformation": "ODT_SEGMENT_EXTRACT",
                "hash_segment": seg_hash,
                "timestamp": timestamp,
                "provenance": {
                    "position_exacte_dans_odt": segment_position,
                    "niveau_transformation": "segment",
                    "provenance_mode": "derived",
                    "traceability_level": "high_derived",
                    "reconstructibility_path": {
                        "type": "derived_from_paragraphs",
                        "source_paragraphs": para_ids,
                        "method": "ordered_concatenation_with_newlines",
                    },
                    "relations": [
                        f"ARTEFACT_PARAGRAPH_{int(pid.split('_')[1]):05d}" for pid in para_ids
                    ],
                },
                "content": seg_text,
            }
        )
    return segments, artefact_segments


# ---------------------------------------------------------------------------
# STRUCTURE_EXPORT
# ---------------------------------------------------------------------------

def build_source_metadata(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    odt_path: Path,
) -> dict:
    """Couche STRUCTURE_EXPORT — metadonnees du fichier source ODT."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "source_file_name": odt_path.name,
        "source_file_size_bytes": odt_path.stat().st_size,
        "source_file_sha256": sha256_file(odt_path),
        "authority": "ODT_NATIVE = source absolue",
        "time_consistency_rule": {"timezone": "UTC", "format": "ISO 8601"},
    }


def build_integrity_report(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    odt_path: Path,
    archive_map: dict,
) -> dict:
    """Couche STRUCTURE_EXPORT — rapport d'integrite des entrees ZIP."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "source_sha256": sha256_file(odt_path),
        "archive_entries_hashed": archive_map,
        "identity_rule": "Identit\u00e9 = contenu + structure + provenance identiques.",
        "status": "VALID",
    }


# ---------------------------------------------------------------------------
# EXECUTION
# ---------------------------------------------------------------------------

def build_extraction_log(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    entries: list,
    paragraphs: list,
    segments: list,
) -> dict:
    """Couche EXECUTION — journal des etapes d'extraction."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "steps": [
            "VALIDATION_ENTREE",
            "ZIP_EXTRACTION",
            "TEXT_EXTRACTION",
            "ARTEFACT_GENERATION",
            "STRUCTURE_EXPORT",
            "EXECUTION_LOGS",
            "EXPLOITATION_EXPORT",
            "FINAL_VALIDATION",
        ],
        "counters": {
            "archive_entries": len(entries),
            "paragraphs": len(paragraphs),
            "segments": len(segments),
        },
        "status": "VALID",
    }


def build_pipeline_exec_meta(
    system_global_id: str,
    document_id: str,
    timestamp: str,
) -> dict:
    """Couche EXECUTION — metadonnees d'execution du pipeline."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "pipeline": (
            "ODT -> VALIDATION -> EXTRACTION -> ARTEFACTS -> STRUCTURE"
            " -> EXECUTION -> EXPLOITATION -> VALIDATION FINALE -> ARCHIVE_COMPLETE_V7_5"
        ),
        "compatibilite_chaine": ["META", "REVELATION", "ENGINE", "SESSION"],
        "degraded_mode_policy_odt": "aucune sortie partielle autoritaire",
        "status": "VALID",
    }


def build_artefact_exec_meta(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    artefact_master: list,
    artefact_paragraphs: list,
    artefact_segments: list,
) -> dict:
    """Couche EXECUTION — compteurs d'artefacts generes."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "artefacts": {
            "ARTEFACT_MASTER": len(artefact_master),
            "ARTEFACT_paragraphs": len(artefact_paragraphs),
            "ARTEFACT_segments": len(artefact_segments),
        },
        "status": "VALID",
    }


# ---------------------------------------------------------------------------
# EXPLOITATION
# ---------------------------------------------------------------------------

def build_corpus(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    odt_path: Path,
    paragraphs: list,
    segments: list,
) -> dict:
    """Couche EXPLOITATION — corpus documentaire complet."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "documents": [
            {
                "document_id": document_id,
                "file_name": odt_path.name,
                "paragraph_ids": [item["paragraph_id"] for item in paragraphs],
                "segment_ids": [item["segment_id"] for item in segments],
                "text": "\n".join(item["text"] for item in paragraphs),
            }
        ],
    }


def build_index_master(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    paragraphs: list,
    segments: list,
    archive_map: dict,
) -> dict:
    """Couche EXPLOITATION — index maitre par paragraph_id, segment_id et entree archive."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "index": {
            "by_paragraph_id": {item["paragraph_id"]: item["hash_segment"] for item in paragraphs},
            "by_segment_id": {item["segment_id"]: item["hash_segment"] for item in segments},
            "by_archive_entry": archive_map,
        },
    }


def build_query_map(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    segments: list,
) -> dict:
    """Couche EXPLOITATION — index inverse mot vers liste de segment_ids (mots >= 4 caracteres)."""
    query_terms: dict[str, set] = {}
    for segment in segments:
        for word in re.findall(r"\w+", segment["text"].lower()):
            if len(word) < 4:
                continue
            query_terms.setdefault(word, set()).add(segment["segment_id"])
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "query_map": {key: sorted(value) for key, value in sorted(query_terms.items())},
    }


def build_finalization_policy(
    system_global_id: str,
    document_id: str,
    timestamp: str,
) -> dict:
    """Couche EXPLOITATION — politique de finalisation et d'extension forensique."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "freeze_policy": FREEZE_POLICY,
        "extension_policy": FORENSIC_EXTENSION_POLICY,
        "closure_exclusions": sorted(TOP_LEVEL_MANIFEST_FILES),
        "note": "Additive-only forensic enrichments do not alter protocol-required files or semantics.",
    }


def build_artefact_relations(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    paragraphs: list,
    segments: list,
) -> dict:
    """Couche EXPLOITATION — graphe de relations entre paragraphes et segments."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "relations": {
            "document_to_paragraphs": [item["paragraph_id"] for item in paragraphs],
            "document_to_segments": [item["segment_id"] for item in segments],
            "segment_to_paragraphs": {
                item["segment_id"]: item["paragraph_ids"] for item in segments
            },
            "paragraph_to_segment_membership": {
                p["paragraph_id"]: [
                    s["segment_id"] for s in segments
                    if p["paragraph_id"] in s["paragraph_ids"]
                ]
                for p in paragraphs
            },
        },
    }


def build_archive_stats(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    entries: list,
    paragraphs: list,
    segments: list,
    text_length: int,
    artefact_master: list,
    artefact_paragraphs: list,
    artefact_segments: list,
) -> dict:
    """Couche EXPLOITATION — statistiques globales de l'archive."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "stats": {
            "source_documents": 1,
            "archive_entries": len(entries),
            "paragraphs": len(paragraphs),
            "segments": len(segments),
            "text_length_chars": text_length,
            "artefacts_total": (
                len(artefact_master) + len(artefact_paragraphs) + len(artefact_segments)
            ),
        },
    }


def build_boot_summary(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    odt_path: Path,
    entries: list,
    paragraphs: list,
    segments: list,
) -> str:
    """Couche EXPLOITATION — texte de resume BOOT pour DOSSIER_BOOT_SUMMARY.txt."""
    return (
        f"ARCHIVE_COMPLETE_V7_5 BOOT SUMMARY\n"
        f"SYSTEM_GLOBAL_ID: {system_global_id}\n"
        f"document_id: {document_id}\n"
        f"timestamp: {timestamp}\n"
        f"source_file: {odt_path.name}\n"
        f"status: VALID\n"
        f"archive_entries: {len(entries)}\n"
        f"paragraphs: {len(paragraphs)}\n"
        f"segments: {len(segments)}\n"
    )


# ---------------------------------------------------------------------------
# ODT layer
# ---------------------------------------------------------------------------

def build_global_system_status(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    entries: list,
    paragraphs: list,
    segments: list,
) -> dict:
    """Couche ODT — statut global du systeme (mis a jour apres validation finale)."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "status": "VALID",
        "freeze_policy": FREEZE_POLICY,
        "layers": {
            "INPUT_VALIDATION": "VALID",
            "ODT": "VALID",
            "ARTEFACTS": "VALID",
            "STRUCTURE": "VALID",
            "EXECUTION": "VALID",
            "EXPLOITATION": "VALID",
            "FINAL_VALIDATION": "PENDING_FINAL_FREEZE",
        },
        "final_counts": {
            "paragraphs": len(paragraphs),
            "segments": len(segments),
            "archive_entries": len(entries),
        },
    }


def build_odt_layer_status(
    system_global_id: str,
    document_id: str,
    timestamp: str,
) -> dict:
    """Couche ODT — statut et politique de la couche source native."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "status": "VALID",
        "authority": "ODT_NATIVE = source absolue",
        "non_retroactivite": True,
        "degraded_mode_policy_odt": "aucune sortie partielle autoritaire",
    }


def build_system_version(timestamp: str) -> dict:
    """Couche ODT — version et profil du systeme."""
    return {
        "layer": "ODT",
        "version": "V7.5",
        "label": "ABSOLUE FINAL \u2014 VERSION 10/10",
        "timestamp": timestamp,
        "profile": "FORENSIC_NATIVE",
    }


# ---------------------------------------------------------------------------
# VALIDATION_FINALE
# ---------------------------------------------------------------------------

def build_preliminary_validation(
    system_global_id: str,
    document_id: str,
    timestamp: str,
    source_dir: Path,
    odt_path: Path,
    entries: list,
    paragraphs: list,
    segments: list,
    reproducible: bool,
) -> dict:
    """Couche VALIDATION FINALE — etat PENDING avant le gel des manifests."""
    return {
        "SYSTEM_GLOBAL_ID": system_global_id,
        "document_id": document_id,
        "timestamp": timestamp,
        "freeze_policy": FREEZE_POLICY,
        "checks": {
            "integrite_complete": False,
            "tracabilite_complete": False,
            "reconstruction_sha256_valide": False,
            "conformite_system_core": False,
            "odt_native_preserved": (source_dir / odt_path.name).exists(),
            "utc_iso_8601": bool(
                re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", timestamp)
            ),
        },
        "details": {
            "paragraphs": len(paragraphs),
            "segments": len(segments),
            "archive_entries": len(entries),
        },
        "status": "PENDING_FINAL_FREEZE",
        "reproducible_mode": reproducible,
    }


# ---------------------------------------------------------------------------
# AUDIT
# ---------------------------------------------------------------------------

def compute_orphan_checks(paragraphs: list, segments: list) -> dict:
    """Detecte les references segment-paragraphe pointant vers des paragraphes inexistants.

    Retourne un dict avec compteurs et liste d'orphelins ; has_orphans est True
    si au moins une reference pendante est trouvee.
    """
    paragraph_ids = {p["paragraph_id"] for p in paragraphs}
    segment_ids = {s["segment_id"] for s in segments}
    orphan_segment_paragraph_refs = []
    for seg in segments:
        for pid in seg["paragraph_ids"]:
            if pid not in paragraph_ids:
                orphan_segment_paragraph_refs.append(
                    {"segment_id": seg["segment_id"], "paragraph_id": pid}
                )
    return {
        "paragraph_count": len(paragraph_ids),
        "segment_count": len(segment_ids),
        "orphan_segment_paragraph_refs": orphan_segment_paragraph_refs,
        "has_orphans": bool(orphan_segment_paragraph_refs),
    }


def build_internal_expert_audit(
    *,
    paragraphs: list,
    segments: list,
    archive_entries: list,
    archive_map: dict,
    final_validation: dict,
    reproducible: bool,
    output_dir: Path,
) -> dict:
    # In reproducible mode the absolute output path must be neutralised so that
    # two runs on the same input but different output directories produce a
    # bit-for-bit identical INTERNAL_EXPERT_AUDIT.json — and therefore an
    # identical MASTER_SHA256.txt and VALIDATION_FINALE.json.
    archive_output_dir_value = "__REPRODUCIBLE_MODE__" if reproducible else str(output_dir)
    """Build the INTERNAL_EXPERT_AUDIT record.

    Aggregates orphan checks, protocol integrity counters, closure model,
    and traceability model into a single audit dict. Status is VALID only
    when final_validation is VALID and no orphan paragraph references exist.
    """
    orphan_checks = compute_orphan_checks(paragraphs, segments)
    return {
        "audit_type": "INTERNAL_EXPERT_AUDIT",
        "status": (
            "VALID"
            if final_validation["status"] == "VALID" and not orphan_checks["has_orphans"]
            else "DEGRADED"
        ),
        "reproducible_mode": reproducible,
        "protocol_integrity": {
            "archive_entries": len(archive_entries),
            "hashed_archive_entries": len(archive_map),
            "paragraphs": len(paragraphs),
            "segments": len(segments),
            "final_validation_status": final_validation["status"],
        },
        "closure_model": {
            "freeze_policy": FREEZE_POLICY,
            "extension_policy": FORENSIC_EXTENSION_POLICY,
            "self_referential_exclusions_documented": True,
        },
        "traceability_model": {
            "paragraph_provenance_mode": "direct",
            "segment_provenance_mode": "derived",
            "reconstructibility_documented": True,
            "orphan_checks": orphan_checks,
        },
        "summary": {
            "archive_manifest_path": "ARCHIVE_MANIFEST.json",
            "chain_manifest_path": "CHAIN_INTEGRITY_MANIFEST.json",
            "master_sha_path": "MASTER_SHA256.txt",
            "boot_summary_path": "bootstrap/DOSSIER_BOOT_SUMMARY.txt",
            "archive_output_dir": archive_output_dir_value,
        },
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def update_global_status_with_validation(
    global_system_status: dict,
    final_validation: dict,
) -> None:
    """Mise a jour en-place du GLOBAL_SYSTEM_STATUS avec le resultat de validation finale."""
    global_system_status["layers"]["FINAL_VALIDATION"] = final_validation["status"]
    global_system_status["final_validation"] = {
        "status": final_validation["status"],
        "checks": final_validation["checks"],
    }
