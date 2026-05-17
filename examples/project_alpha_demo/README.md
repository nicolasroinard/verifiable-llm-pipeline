# Project Alpha — End-to-End Demo

This directory demonstrates the verifiable LLM pipeline applied to fictional input documents.

## Overview

**Project Alpha** is a demonstration of a document analysis pipeline that transforms 
unstructured inputs into structured, traceable, and verifiable data.

The system implements a four-stage pipeline:
- Document Intake and Structuring (ODT layer)
- Metadata Structuring and Graph Construction (META layer)
- Deterministic Analysis and Validation (REVELATION layer)
- Output Structuring and Projection (ENGINE layer)

## Input Documents

Three fictional sample documents are provided in `inputs/`:

1. **sample_1_project_overview.odt** — High-level project summary
   - Fictional project identifier: ALPHA-2026
   - Fictional key people: Clara Bennett (owner), Daniel Moore (technical lead)
   - Fictional dates and milestones
   - Fictional system pipeline overview

2. **sample_2_technical_notes.odt** — Detailed technical architecture
   - Fictional system stages and components
   - Fictional entity relationships and dependencies
   - Fictional validation requirements

3. **sample_3_validation_scope.odt** — Validation and audit criteria
   - Fictional scope of validation
   - Fictional failure modes and consistency checks
   - Fictional traceability requirements

## Output Artifacts

The `expected_public_outputs/` directory contains simplified public views that are 
**informed by an internal run** of the broader ODT → META → REVELATION → ENGINE pipeline on these 
fictional input documents.

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

## About This Demo

### What This Represents

This demo illustrates:
- How documents can be normalized into structured artifacts
- How entities and relationships are extracted across documents
- How lineage and traceability are maintained
- How deterministic validation works
- How outputs are structured for downstream consumption

### Data Source

The public views shown here are **informed by an internal run** of the broader ODT → META → REVELATION → ENGINE pipeline 
on these fictional Project Alpha documents.

**Important note** : The broader internal run package is not published because it contains:
- Orchestration infrastructure
- Internal validation and packaging logic
- Transport and freezing infrastructure
- Internal audit and packaging details

The public demonstration only shows **simplified, source-traceable views** of the kind 
of artifacts each pipeline layer produces.

### Verification Basis

The internal run completed with **non-blocking ODT processing notes**. Subsequent stages completed and produced source-traceable public views, including:
- Source-traceable entity extraction
- Complete cross-document lineage
- Deterministic validation criteria satisfied

However, the broader internal run package itself is not published because it exposes internal 
infrastructure not suitable for public consumption.

## How to Reuse the Pattern

This example can be used as a conceptual template for demonstrating a document workflow:

1. Define fictional or non-sensitive input documents
2. Identify entities and relations
3. Map source traceability
4. Describe deterministic validation criteria
5. Produce a simplified public projection
6. Clarify the relationship between internal processing and public views

## Limitations

- This demo uses fictional data for illustration only
- Outputs are simplified public views, not raw internal artifacts
- Real pipeline outputs include comprehensive internal metadata and validation details
- The internal pipeline includes infrastructure not shown in this demo
- The internal run evidence is retained internally and not published

## Architecture Scope

**This repository shows** :
- ✅ Document ingestion patterns
- ✅ Entity and relationship extraction concepts
- ✅ Lineage and traceability approaches
- ✅ Deterministic validation logic
- ✅ Output structuring patterns

**This repository does not include** :
- ❌ Complete internal run logs
- ❌ Internal orchestration infrastructure
- ❌ Freeze and lock mechanisms
- ❌ Internal validation tooling
- ❌ Internal audit and packaging details
- ❌ Transport infrastructure
