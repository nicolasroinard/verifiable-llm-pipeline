# META: Corpus & Graph Construction Layer

Merges structured documents into a global corpus and builds graph-like relationships between segments, nodes and references.

---

## Overview

This layer is part of a larger experimental pipeline for verifiable LLM workflows.

It takes structured document archives from the ODT layer and constructs:

- a unified corpus from multiple document sources
- graph-like relationships between segments and nodes
- a structured index for downstream processing

The implementation demonstrates core governance patterns:

- **lineage tracking** — maintains genealogy of corpus construction
- **reference integrity** — validates cross-reference consistency

---

## Purpose

The META layer creates a structured data foundation for downstream analysis.

Instead of treating documents as isolated text, META builds a relational layer that:

- deduplicates content across archives
- tracks segment-to-segment relationships
- identifies conflicts and overlaps
- creates an auditable construction trail
- validates integrity of the resulting graph

---

## Implementation

The layer implements a processing pipeline:

```text
Input (ODT archives) 
  → Validation
  → Aggregation
  → Normalization
  → Cross-archive linking
  → Deduplication
  → Conflict detection
  → Corpus construction
  → Indexing
  → Graph building
  → Delta tracking
  → Final validation
```

Entry point:

```bash
python run_meta_fusion.py --input <input_dir> --output <output_dir>
```

---

## Outputs

The layer produces:

- `META_GLOBAL_GRAPH.json` — Node and edge definitions
- `GLOBAL_DOCUMENT_CORPUS.json` — Document and segment metadata
- `GLOBAL_SEMANTIC_INDEX.json` — Reference index for downstream layers
- `CHAIN_INTEGRITY_MANIFEST.json` — Audit trail and integrity validation

---

## Design principles

- **No implicit inference** — only explicit aggregation and structuring
- **Full traceability** — every element tracks its origin
- **Reproducibility** — identical inputs produce identical outputs
- **Auditability** — integrity manifest enables independent verification

---

## Status

This directory exposes the public, readable version of this layer.

The complete internal implementation includes additional validation, qualification, and forensic tools that are not exposed publicly.

---

## Next steps

The REVELATION layer consumes META outputs and performs deterministic analysis.

The ENGINE layer structures final outputs from REVELATION analysis units.
