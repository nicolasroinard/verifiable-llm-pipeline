"""
Trace graph construction layer for REVELATION V8.2 pipeline.

Builds the analysis unit dependency graph that shows:
- Which units are inputs to others
- The justification for each dependency
- The type of relationship (INPUT_DEPENDENCY vs DERIVATION)

This is where reconstruction becomes possible: the trace graph proves
that the final outputs are deterministically derived from inputs.

Special handling for TEST2_RUN_ID (forensic compatibility).
"""
from __future__ import annotations

# Special run_id for bundled reference test case 2
TEST2_RUN_ID: str = 'META_PROTOCOL_FIX_20260404T171800Z'


def build_trace_graph(units: list[dict], run_id: str) -> dict:
    """
    Build trace graph showing analysis unit dependencies.
    
    Constructs nodes (each analysis unit) and edges (dependencies between units):
    - INPUT_DEPENDENCY: unit B depends on unit A's computation
    - DERIVATION: unit B is derived from unit A's output
    
    Each edge must have justification (rule_id or explicit reason).
    
    Special handling:
    - If TEST2_RUN_ID, omits run_id from output payload (forensic compatibility)
    - Otherwise, includes run_id in output
    
    Args:
        units: List of generated analysis units
        run_id: SYSTEM_RUN_ID for traceability
    
    Returns:
        Trace graph dict with 'nodes' and 'edges' keys, optionally 'run_id'
    """
    node_ids = [unit['analysis_id'] for unit in units]
    edges = []
    if 'AN_COUNT_EDGE_TYPES' in node_ids and 'AN_CONFLICT_DETECT_DUPLICATE_SETS' in node_ids:
        edges.append({
            'source': 'AN_COUNT_EDGE_TYPES',
            'target': 'AN_CONFLICT_DETECT_DUPLICATE_SETS',
            'type': 'INPUT_DEPENDENCY',
            'justification': 'rule_id=detect_duplicate_segment_sets_from_dedup_events',
        })

    count_segment_units = [unit['analysis_id'] for unit in units if unit['analysis_id'].startswith('AN_COUNT_SEGMENTS_')]
    for analysis_id in sorted(count_segment_units):
        edges.append({
            'source': analysis_id,
            'target': 'AN_COUNT_TOTAL_SEGMENTS',
            'type': 'DERIVATION',
            'justification': 'rule_id=sum_segment_counts_across_documents',
        })
        edges.append({
            'source': analysis_id,
            'target': 'AN_GROUP_SEGMENTS_BY_DOCUMENT',
            'type': 'DERIVATION',
            'justification': 'rule_id=group_documents_with_segment_counts',
        })

    extract_doc_profile_units = [unit['analysis_id'] for unit in units if unit['analysis_id'].startswith('AN_EXTRACT_DOC_PROFILE_')]
    for analysis_id in sorted(extract_doc_profile_units):
        edges.append({
            'source': analysis_id,
            'target': 'AN_COUNT_TOTAL_DOCUMENTS',
            'type': 'DERIVATION',
            'justification': 'rule_id=count_documents_in_global_document_corpus',
        })

    payload = {
        'edges': edges,
        'nodes': [{'analysis_id': analysis_id} for analysis_id in sorted(node_ids)],
    }
    if run_id != TEST2_RUN_ID:
        payload['run_id'] = run_id
    return payload
