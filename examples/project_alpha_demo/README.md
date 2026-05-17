# Project Alpha — End-to-End Demo

This directory demonstrates the verifiable LLM pipeline applied to a fictional project.

## Overview

**Project Alpha** is a document analysis system that transforms unstructured inputs into structured, 
traceable, and verifiable data.

The system implements a four-stage pipeline:
- Document Intake
- Metadata Structuring
- Graph Construction
- Validation Engine

## Input Documents

Three sample documents are provided in `inputs/`:

1. **sample_1_project_overview.odt** — High-level project summary
   - Project identifier: ALPHA-2026
   - Key people: Clara Bennett (owner), Daniel Moore (technical lead)
   - Key dates and milestones
   - System pipeline overview

2. **sample_2_technical_notes.odt** — Detailed technical architecture
   - System stages and components
   - Entity relationships and dependencies
   - Validation requirements

3. **sample_3_validation_scope.odt** — Validation and audit criteria
   - Scope of validation
   - Failure modes and consistency checks
   - Traceability requirements

## Output Artifacts

The `expected_public_outputs/` directory contains simplified public views of the kind of artifacts 
produced by each layer.

### 01_odt_structured_view.md

Shows how the ODT ingestion layer structures the raw documents:
- Document metadata
- Content segmentation
- Source preservation
- Artifact inventory

### 02_meta_graph_view.md

Shows entity extraction and graph construction:
- Extracted entities (project, people, components, dates)
- Cross-document relationships
- Lineage tracking
- Graph structure

### 03_revelation_validation_view.md

Shows deterministic observations and validation:
- Observed patterns (entity consistency, completeness, connectivity)
- Validation criteria
- Pass/fail conditions
- Traceability audit

### 04_engine_public_projection.md

Shows output structuring:
- Projection format
- Structured data organization
- Summary views
- Audit trail

## Important Notes

⚠️ **Fictional Data**

These documents are entirely fictional and created for demonstration purposes.

⚠️ **Public Subset**

This demo illustrates the public subset of the architecture.

The complete internal pipeline includes:
- Advanced validation and integrity checking
- Packaging and freeze infrastructure
- Domain-specific orchestration
- Audit and packaging tooling

These internal components are intentionally not exposed in this public demo.

⚠️ **Simplified Public Views**

The outputs shown in `expected_public_outputs/` are not actual machine outputs.
They are manually created to illustrate how each pipeline stage would structure and 
transform the data.

## Architecture Patterns Demonstrated

This demo shows:

✅ How documents are normalized into structured artifacts  
✅ How entities and relationships are extracted across documents  
✅ How lineage and traceability are maintained  
✅ How deterministic validation works  
✅ How outputs are structured for downstream consumption

## How to reuse the pattern

This example can be used as a conceptual template for documenting a governed document workflow:

1. Define fictional or non-sensitive input documents
2. Identify entities and relations
3. Map source traceability
4. Describe deterministic validation criteria
5. Produce a simplified public projection

## Limitations

- This demo is pedagogical, not production-grade
- Outputs are simplified views created for demonstration
- Real pipeline outputs would include comprehensive metadata and validation results
- The internal pipeline is more sophisticated and includes infrastructure not shown here
