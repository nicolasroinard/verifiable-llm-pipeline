"""
Status layer for REVELATION V8.2 pipeline.

Responsibilities:
1. Call scoring functions to compute metrics
2. Aggregate checks from validation layer
3. Produce REVELATION_LAYER_STATUS.json
4. Return metrics and check results for manifest building

This layer synthesizes all prior validations into a single status outcome.
"""
from __future__ import annotations

from src.models import Check, RevelationContext
from src.scoring import compute_status_metrics
from src.utils import write_json


def build_layer_status(context: RevelationContext, units: list[dict], trace_graph: dict) -> tuple[dict, list[dict]]:
    """
    Build REVELATION layer status and comprehensive audit checks.
    
    Orchestrates:
    1. Metrics computation (coverage, integrity, traceability)
    2. Check aggregation (from validation + new checks)
    3. Status file writing
    4. Return for manifest building
    
    Args:
        context: RevelationContext with validation checks already accumulated
        units: Generated analysis units
        trace_graph: Generated trace graph
    
    Returns:
        Tuple of (status_metrics_dict, checks_list):
        - status_metrics_dict: written to REVELATION_LAYER_STATUS.json
        - checks_list: all checks as dicts for manifest
    """
    metrics, raw_checks = compute_status_metrics(context, units, trace_graph)
    checks = [check.as_dict() for check in context.checks]
    checks.extend([
        Check(check='allowed_operation_types_only', result='PASS' if raw_checks['allowed_operation_types_only'] == 0 else 'FAIL').as_dict(),
        Check(check='unit_status_domain_valid', result='PASS' if raw_checks['unit_status_domain_valid'] == 0 else 'FAIL').as_dict(),
        Check(check='deterministic_proof_replayable', result='PASS' if raw_checks['deterministic_proof_replayable'] == 0 else 'FAIL').as_dict(),
        Check(check='orphan_reference_scan', result='PASS' if raw_checks['orphan_reference_scan'] == 0 else 'FAIL', orphan_count=raw_checks['orphan_reference_scan']).as_dict(),
        Check(check='coverage_ratio_complete', result='PASS' if metrics['coverage_ratio'] == 1.0 else 'FAIL', value=metrics['coverage_ratio']).as_dict(),
        Check(check='traceability_score_complete', result='PASS' if metrics['traceability_score'] == 1.0 else 'FAIL', value=metrics['traceability_score']).as_dict(),
        Check(check='input_refs_explicit_meta_only', result='PASS' if raw_checks['input_refs_explicit_meta_only'] == 0 else 'FAIL', violation_count=raw_checks['input_refs_explicit_meta_only']).as_dict(),
        Check(check='evidence_binding_explicit_meta_only', result='PASS' if raw_checks['evidence_binding_explicit_meta_only'] == 0 else 'FAIL', violation_count=raw_checks['evidence_binding_explicit_meta_only']).as_dict(),
    ])
    write_json(context.output_dir / 'REVELATION_LAYER_STATUS.json', metrics)
    return metrics, checks
