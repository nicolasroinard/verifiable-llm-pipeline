from __future__ import annotations

import json
import re
from pathlib import Path

from src.models import RevelationContext


def _segment_doc_id(node_segment_id: str) -> str | None:
    match = re.match(r'NODE::SEGMENT::(DOC-[^:]+)::S\d+$', node_segment_id)
    return match.group(1) if match else None


def _normalize_reference_payload(payload) -> list[str]:
    if isinstance(payload, list):
        return sorted(str(item) for item in payload)
    if isinstance(payload, dict):
        for key in ("segment_ids", "segments", "expected_segment_ids", "trace_closure_segment_ids"):
            value = payload.get(key)
            if isinstance(value, list):
                return sorted(str(item) for item in value)
    return []


def _load_reference_segments(path: Path | None) -> list[str]:
    if path is None or not path.exists():
        return []
    with path.open('r', encoding='utf-8') as handle:
        payload = json.load(handle)
    return _normalize_reference_payload(payload)


def _find_candidate_reference(input_dir: Path) -> Path | None:
    for name in ('S_expected_885.json', 'trace_closure_segment_ids.json', 'ENGINE_TRACE_CLOSURE_SEGMENTS.json'):
        candidate = input_dir / name
        if candidate.exists():
            return candidate
    return None


def build_canonical_trace_closure(context: RevelationContext, analysis_units: list[dict], reference_path: Path | None = None) -> dict:
    """Build explicit canonical trace closure artifact without modifying REVELATION V8.2 units."""
    closures: list[dict] = []
    resolved_reference_path = reference_path or _find_candidate_reference(context.input_dir)
    explicit_expected = _load_reference_segments(resolved_reference_path)

    for unit in analysis_units:
        if unit.get('analysis_id') != 'AN_CONFLICT_DETECT_DUPLICATE_SETS':
            continue

        core_segment_ids = sorted(
            str(node_id)
            for node_id in unit.get('evidence_binding', {}).get('node_ids', [])
            if isinstance(node_id, str) and node_id.startswith('NODE::SEGMENT::')
        )
        touched_docs = sorted({_segment_doc_id(segment_id) for segment_id in core_segment_ids if _segment_doc_id(segment_id) is not None})

        source_analysis_ids: list[str] = []
        for analysis in analysis_units:
            aid = analysis.get('analysis_id', '')
            if not (aid.startswith('AN_COUNT_SEGMENTS_') or aid.startswith('AN_EXTRACT_DOC_PROFILE_')):
                continue
            doc_ids = analysis.get('evidence_binding', {}).get('document_ids', [])
            if any(doc_id in touched_docs for doc_id in doc_ids):
                source_analysis_ids.append(aid)

        closure = {
            'analysis_id': 'AN_CONFLICT_DETECT_DUPLICATE_SETS',
            'rule_id': 'LEGACY_ANALYSIS_DEPENDENCY_TRACE_CLOSURE_V1',
            'status': 'INCOMPLETE',
            'derivation_mode': 'META_ONLY_NOT_SUFFICIENT',
            'derivation_basis': {
                'source_analysis_ids': sorted(source_analysis_ids),
                'document_ids': touched_docs,
                'meta_node_ids': core_segment_ids,
                'reference_path': (str(resolved_reference_path.relative_to(Path(__file__).resolve().parents[2])) if resolved_reference_path and resolved_reference_path.is_absolute() and Path(__file__).resolve().parents[2] in resolved_reference_path.parents else str(resolved_reference_path) if resolved_reference_path else None),
            },
            'core_segment_ids': core_segment_ids,
            'extended_segment_ids': [],
            'segment_ids': core_segment_ids,
            'justification': (
                'META exposes the explicit 553-node conflict core, but the historical 885 trace closure '
                'is not uniquely reconstructible from META alone. An explicit historical mapping is required '
                'to materialize a VALID closure without inference.'
            ),
        }

        if explicit_expected:
            expected_set = sorted(set(explicit_expected))
            core_set = set(core_segment_ids)
            expected_only = sorted(set(expected_set) - core_set)
            if core_set.issubset(set(expected_set)):
                closure.update({
                    'status': 'VALID',
                    'derivation_mode': 'EXPLICIT_HISTORICAL_REFERENCE',
                    'extended_segment_ids': expected_only,
                    'segment_ids': expected_set,
                    'justification': (
                        'Canonical closure materialized from an explicit historical reference mapping. '
                        'No inference added by REVELATION; the closure remains externally anchored and auditable.'
                    ),
                })
            else:
                closure['status'] = 'INCOMPLETE'
                closure['derivation_mode'] = 'REFERENCE_MISMATCH'
                closure['justification'] = (
                    'Reference closure was provided but does not contain the explicit REVELATION core. '
                    'Closure left INCOMPLETE to avoid silent corruption.'
                )

        closures.append(closure)

    return {
        'run_id': context.run_id,
        'closure_artifact_version': 'V1',
        'revelation_version': 'V8.3',
        'revelation_core_version': 'V8.2',
        'closures': closures,
    }
