"""
Metrics and audit functions for REVELATION V8.2 pipeline.

Computes deterministic integrity, traceability, and coverage metrics.
Performs exhaustive protocol validation:
- Operation type domain validation
- Evidence binding structure validation
- Reference orphan detection (cross-checks against META indices)
- Trace graph structural validation

All metrics are binary (1.0/0.0) for clarity; layer status is VALID
only if ALL metrics reach 1.0.
"""
from __future__ import annotations

from src.models import RevelationContext
from src.utils import ALLOWED_REVELATION_OPERATION_TYPES, ALLOWED_UNIT_STATUS


def scan_input_ref_violations(units: list[dict]) -> int:
    """
    Scan all input_refs for protocol violations.
    
    Input refs must be:
    - Strings (not composites)
    - Prefixed with typed prefix: 'node_id:', 'segment_id:', 'document_id:'
    
    Args:
        units: List of analysis units to scan
    
    Returns:
        Count of violations found
    """
    allowed_prefixes = ('node_id:', 'segment_id:', 'document_id:')
    violations = 0
    for unit in units:
        for ref in unit.get('input_refs', []):
            if not isinstance(ref, str) or not ref.startswith(allowed_prefixes):
                violations += 1
    return violations


def scan_evidence_binding_violations(units: list[dict]) -> int:
    """
    Scan all evidence_binding structures for protocol violations.
    
    Evidence binding must be:
    - A dict (not null, not string, not array)
    - Contain only allowed keys: 'node_ids', 'segment_ids', 'document_ids'
    - Have values that are lists (not strings, not scalars)
    
    Args:
        units: List of analysis units to scan
    
    Returns:
        Count of violations found
    """
    allowed_keys = {'node_ids', 'segment_ids', 'document_ids'}
    violations = 0
    for unit in units:
        binding = unit.get('evidence_binding') or {}
        if not isinstance(binding, dict):
            violations += 1
            continue
        for key, values in binding.items():
            if key not in allowed_keys:
                violations += 1
                continue
            if not isinstance(values, list):
                violations += 1
    return violations


def scan_orphans(context: RevelationContext, units: list[dict]) -> int:
    """
    Scan all units for orphaned references (refs to non-existent META entities).
    
    Cross-checks every input_ref and evidence_binding entry against:
    - context.node_ids (from META_GLOBAL_GRAPH)
    - context.document_ids (from GLOBAL_DOCUMENT_CORPUS)
    - context.segment_ids (from graph provenance)
    
    Args:
        context: RevelationContext with populated indices
        units: List of analysis units to scan
    
    Returns:
        Count of orphaned references found
    """
    orphans = 0
    for unit in units:
        for ref in unit.get('input_refs', []):
            if ref.startswith('node_id:') and ref.split(':', 1)[1] not in context.node_ids:
                orphans += 1
            elif ref.startswith('document_id:') and ref.split(':', 1)[1] not in context.document_ids:
                orphans += 1
            elif ref.startswith('segment_id:') and ref.split(':', 1)[1] not in context.segment_ids:
                orphans += 1
        binding = unit.get('evidence_binding') or {}
        for node_id in binding.get('node_ids', []):
            if node_id not in context.node_ids:
                orphans += 1
        for document_id in binding.get('document_ids', []):
            if document_id not in context.document_ids:
                orphans += 1
        for segment_id in binding.get('segment_ids', []):
            if segment_id not in context.segment_ids:
                orphans += 1
    return orphans


def compute_status_metrics(context: RevelationContext, units: list[dict], trace_graph: dict) -> tuple[dict, dict]:
    """
    Compute comprehensive REVELATION layer status metrics and audit checks.
    
    Computes 3 key metrics (each 1.0 or 0.0):
    
    1. **coverage_ratio**: 1.0 if all context.documents are analyzed; else 0.0
    2. **deterministic_integrity**: 1.0 if no operation type/status/replay/graph violations; else 0.0
    3. **traceability_score**: 1.0 if no input_ref/evidence_binding/orphan violations; else 0.0
    
    Layer status is VALID only if all three == 1.0; else DEGRADED.
    
    Args:
        context: RevelationContext with metadata indices
        units: List of generated analysis units
        trace_graph: Generated trace graph structure
    
    Returns:
        Tuple of (metrics_dict, checks_dict):
        - metrics_dict: status, coverage_ratio, deterministic_integrity, 
                       traceability_score, run_id
        - checks_dict: detailed violation counts for each check type
    """
    operation_violations = sum(1 for unit in units if unit.get('operation_type') not in ALLOWED_REVELATION_OPERATION_TYPES)
    unit_status_violations = sum(1 for unit in units if unit.get('status') not in ALLOWED_UNIT_STATUS)
    replay_violations = sum(
        1 for unit in units
        if not isinstance(unit.get('deterministic_proof'), dict) or unit['deterministic_proof'].get('replayable') is not True
    )
    input_ref_violations = scan_input_ref_violations(units)
    evidence_binding_violations = scan_evidence_binding_violations(units)
    orphan_count = scan_orphans(context, units)
    graph_node_ids = {node['analysis_id'] for node in trace_graph.get('nodes', [])}
    graph_edge_violations = 0
    for edge in trace_graph.get('edges', []):
        if edge.get('type') not in {'INPUT_DEPENDENCY', 'DERIVATION'}:
            graph_edge_violations += 1
        if edge.get('source') not in graph_node_ids or edge.get('target') not in graph_node_ids:
            graph_edge_violations += 1
        if not edge.get('justification'):
            graph_edge_violations += 1

    coverage_ratio = 1.0 if units and len({doc['doc_id'] for doc in context.documents}) == len(context.document_ids) else 0.0
    deterministic_integrity = 1.0 if (operation_violations + unit_status_violations + replay_violations + graph_edge_violations) == 0 else 0.0
    traceability_score = 1.0 if (input_ref_violations + evidence_binding_violations + orphan_count) == 0 else 0.0
    status = 'VALID' if coverage_ratio == deterministic_integrity == traceability_score == 1.0 else 'DEGRADED'

    checks = {
        'allowed_operation_types_only': operation_violations,
        'unit_status_domain_valid': unit_status_violations,
        'deterministic_proof_replayable': replay_violations,
        'orphan_reference_scan': orphan_count,
        'input_refs_explicit_meta_only': input_ref_violations,
        'evidence_binding_explicit_meta_only': evidence_binding_violations,
        'trace_graph_structure': graph_edge_violations,
    }
    metrics = {
        'status': status,
        'coverage_ratio': round(coverage_ratio, 6),
        'deterministic_integrity': round(deterministic_integrity, 6),
        'traceability_score': round(traceability_score, 6),
        'run_id': context.run_id,
    }
    return metrics, checks
