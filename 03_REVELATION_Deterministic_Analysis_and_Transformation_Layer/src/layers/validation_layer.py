"""
Input validation layer for REVELATION V8.2 pipeline.

Responsibilities:
1. Load and validate 5 required META artifacts
2. Extract and index metadata (nodes, documents, segments)
3. Build lookup tables (node_id → document → segment mappings)
4. Perform integrity checks (run_id consistency, reference validity)
5. Populate context with validated metadata

This layer enforces the input contract: if any check fails, ProtocolError
is raised and pipeline stops immediately.
"""
from __future__ import annotations

from collections import defaultdict

from src.models import Check, RevelationContext
from src.utils import ALLOWED_META_STATUS, REQUIRED_INPUT_FILES, ensure, read_json


def load_and_validate_inputs(context: RevelationContext) -> None:
    """
    Load, validate, and index all required META artifacts.
    
    Performs:
    1. Existence and JSON schema validation
    2. Integrity manifest checks (run_id, integrity_result)
    3. Document-node-segment relationship validation
    4. Orphan reference detection
    5. Index building for fast lookups
    
    Args:
        context: RevelationContext to populate with loaded artifacts
    
    Raises:
        ProtocolError: if any artifact is missing, malformed, or violates
                      protocol (e.g., integrity_result != PASS, orphan refs)
    """
    for name in REQUIRED_INPUT_FILES:
        ensure((context.input_dir / name).exists(), f'Missing required input artefact: {name}')

    context.meta_graph = read_json(context.input_dir / 'META_GLOBAL_GRAPH.json')
    context.document_corpus = read_json(context.input_dir / 'GLOBAL_DOCUMENT_CORPUS.json')
    context.semantic_index = read_json(context.input_dir / 'GLOBAL_SEMANTIC_INDEX.json')
    context.meta_layer_status = read_json(context.input_dir / 'META_LAYER_STATUS.json')
    context.meta_chain_manifest = read_json(context.input_dir / 'CHAIN_INTEGRITY_MANIFEST.json')
    delta_path = context.input_dir / 'META_DELTA_REPORT.json'
    context.meta_delta_report = read_json(delta_path) if delta_path.exists() else {'dedup_events': [], 'unresolved_items': [], 'run_id': context.meta_chain_manifest.get('run_id')}

    context.graph_nodes = sorted(context.meta_graph.get('nodes', []), key=lambda node: (node.get('type') or '', node.get('id') or ''))
    context.graph_edges = sorted(context.meta_graph.get('edges', []), key=lambda edge: (edge.get('type') or '', edge.get('source') or '', edge.get('target') or ''))
    context.documents = sorted(context.document_corpus.get('documents', []), key=lambda doc: doc.get('doc_id') or '')

    context.node_ids = {node['id'] for node in context.graph_nodes}
    context.document_ids = {doc['doc_id'] for doc in context.documents}
    context.run_id = context.meta_chain_manifest.get('run_id')

    ensure(context.meta_layer_status.get('status') in ALLOWED_META_STATUS, f"META status not acceptable for REVELATION: {context.meta_layer_status.get('status')}")
    ensure(context.meta_chain_manifest.get('integrity_result') == 'PASS', 'META chain integrity_result must be PASS')
    ensure(bool(context.run_id), 'Missing run_id in META chain manifest')
    ensure(context.meta_layer_status.get('run_id') == context.run_id, 'META run_id mismatch between META_LAYER_STATUS and CHAIN_INTEGRITY_MANIFEST')
    if context.meta_delta_report:
        delta_run_id = context.meta_delta_report.get('run_id')
        if delta_run_id is not None:
            ensure(delta_run_id == context.run_id, 'META run_id mismatch in META_DELTA_REPORT')

    document_node_ids: dict[str, str] = {}
    segment_node_ids_by_doc = defaultdict(list)
    segment_ids_by_doc = defaultdict(list)
    for node in context.graph_nodes:
        provenance = node.get('provenance', {})
        document_id = provenance.get('document_id')
        segment_id = provenance.get('segment_id')
        if node.get('type') == 'DOCUMENT' and document_id:
            document_node_ids[document_id] = node['id']
        if node.get('type') == 'SEGMENT' and document_id and segment_id:
            segment_node_ids_by_doc[document_id].append(node['id'])
            segment_ids_by_doc[document_id].append(segment_id)
            context.segment_ids.add(segment_id)

    context.document_node_ids = {key: value for key, value in sorted(document_node_ids.items())}
    context.segment_node_ids_by_doc = {key: sorted(value) for key, value in sorted(segment_node_ids_by_doc.items())}
    context.segment_ids_by_doc = {key: sorted(value) for key, value in sorted(segment_ids_by_doc.items())}

    for document in context.documents:
        doc_id = document['doc_id']
        context.source_archive_by_doc[doc_id] = document.get('source_archive')
        context.source_document_by_doc[doc_id] = document.get('source_document')
        ensure(doc_id in context.document_node_ids, f'Missing DOCUMENT node for doc_id={doc_id}')
        for segment in document.get('segments', []):
            segment_id = segment.get('segment_id')
            ensure(bool(segment_id), f'Missing segment_id under doc_id={doc_id}')
            context.segment_ids.add(segment_id)
            for node_ref in segment.get('node_refs', []):
                ensure(node_ref in context.node_ids, f'Orphan node_ref in GLOBAL_DOCUMENT_CORPUS: {node_ref}')

    context.checks.extend([
        Check(check='required_input_artifacts_present', result='PASS'),
        Check(check='meta_status', result='PASS', value=context.meta_layer_status.get('status')),
        Check(check='meta_integrity_result', result='PASS', value=context.meta_chain_manifest.get('integrity_result')),
        Check(check='run_id_consistency', result='PASS', value=context.run_id),
    ])
