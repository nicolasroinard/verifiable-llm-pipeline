"""
Data models for REVELATION V8.2 pipeline.

Defines the core context object (RevelationContext) that flows through
all processing layers, plus check result representation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Check:
    """
    Represents a single validation or audit check result.
    
    Attributes:
        check: Name of the check performed
        result: Pass/Fail/Skip result
        value: Optional checked value
        violation_count: Optional count of violations found
        orphan_count: Optional count of orphaned references
    """
    check: str
    result: str
    value: Any | None = None
    violation_count: int | None = None
    orphan_count: int | None = None

    def as_dict(self) -> dict:
        """Convert Check to JSON-serializable dictionary."""
        payload = {'check': self.check, 'result': self.result}
        if self.value is not None:
            payload['value'] = self.value
        if self.violation_count is not None:
            payload['violation_count'] = self.violation_count
        if self.orphan_count is not None:
            payload['orphan_count'] = self.orphan_count
        return payload


@dataclass
class RevelationContext:
    """
    Central context object flowing through all REVELATION processing layers.
    
    Holds:
    - Input configuration (paths, flags)
    - Loaded META artifacts
    - Extracted metadata indices (nodes, documents, segments)
    - Accumulating checks and validation results
    
    This replaces global state with explicit, auditable parameter passing.
    """
    # Configuration
    input_dir: Path
    output_dir: Path
    strict: bool
    reproducible: bool
    fixed_timestamp: str | None = None
    package_output: Path | None = None
    odt_source_dir: Path | None = None
    
    # Loaded META artifacts
    meta_graph: dict[str, Any] | None = None
    document_corpus: dict[str, Any] | None = None
    semantic_index: dict[str, Any] | None = None
    meta_layer_status: dict[str, Any] | None = None
    meta_chain_manifest: dict[str, Any] | None = None
    meta_delta_report: dict[str, Any] | None = None
    
    # Extracted and indexed metadata
    run_id: str | None = None
    graph_nodes: list[dict[str, Any]] = field(default_factory=list)
    graph_edges: list[dict[str, Any]] = field(default_factory=list)
    documents: list[dict[str, Any]] = field(default_factory=list)
    
    # Indices for fast lookup and validation
    node_ids: set[str] = field(default_factory=set)
    document_ids: set[str] = field(default_factory=set)
    segment_ids: set[str] = field(default_factory=set)
    
    # Mappings between IDs (for evidence binding and traceability)
    document_node_ids: dict[str, str] = field(default_factory=dict)
    segment_node_ids_by_doc: dict[str, list[str]] = field(default_factory=dict)
    segment_ids_by_doc: dict[str, list[str]] = field(default_factory=dict)
    source_archive_by_doc: dict[str, str] = field(default_factory=dict)
    source_document_by_doc: dict[str, str] = field(default_factory=dict)
    
    # Accumulating validation checks
    checks: list[Check] = field(default_factory=list)

