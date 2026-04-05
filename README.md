# Verifiable LLM Pipeline

Turning probabilistic LLM outputs into structured, traceable, and verifiable systems.

---

## Overview

This repository provides a runnable subset of a larger system designed to process complex document corpora.

The goal is not to improve prompts, but to build a system layer that makes AI outputs reliable, auditable, and reproducible.

---

## Why

LLM outputs are inherently probabilistic and difficult to verify.

When applied to complex or multi-document contexts, this leads to:

* loss of coherence
* lack of traceability
* difficulty in validating results

This project explores a different approach:

Building a system layer that enforces structure, traceability, and validation.

---

## What is currently implemented

This repository exposes a runnable subset of the system:

### ODT — Document Ingestion & Structuring

* Ingests raw `.odt` documents
* Segments and structures content
* Produces a traceable and reconstructible archive
* Preserves source integrity

Entry point:

```text
odt/run_odt_v75.py
```

---

### META — Corpus & Graph Construction

* Merges structured documents into a global corpus
* Builds a graph representation of relationships
* Produces a unified structured data layer

Entry point:

```text
meta/run_meta_fusion.py
```

---

## Full System Architecture

The complete system is composed of multiple layers:

### Core processing

* GLOBAL PIPELINE — End-to-End Orchestration
* ODT — Document Ingestion & Structuring
* META — Corpus & Graph Construction
* REVELATION — Deterministic Analysis
* ENGINE — Output Structuring

### Validation & grounding

* PROOF — Evidence Reconstruction
* REANCHOR — Source Grounding

### Advanced system layers

* COGNITIVE — Controlled Reasoning & Validation
* SESSION_MANAGER — Orchestration & Monitoring
* MEMORY_GRAPH_AGENT — Memory & Consistency

This repository currently exposes a runnable subset (ODT + META).
Other layers are part of ongoing development.

---

## Repository Structure

```text
odt/
  run_odt_v75.py
  src/
  demo/
    input/
      example_improved.odt

meta/
  run_meta_fusion.py
  src/
```

---

## Demo

A minimal example document is provided:

```text
odt/demo/input/example_improved.odt
```

This example is intentionally simple.

The pipeline transforms raw documents into structured and traceable representations that can be audited and reconstructed.

---

## How to Run

```bash
# Install dependencies
pip install -r odt/requirements.txt
pip install -r meta/requirements.txt

# Step 1 — Ingest and structure documents
python odt/run_odt_v75.py

# Step 2 — Build corpus and graph
python meta/run_meta_fusion.py
```

---

## Output

Outputs are generated at runtime and are not included in this repository.

Running the pipeline produces:

* structured document archives
* validation metadata
* traceable intermediate representations

---

## Design Principles

* Deterministic processing layers
* Full traceability
* No implicit inference
* Reproducible outputs
* Separation between data and system logic

---

## Notes

This repository focuses on system design and execution logic.

It does not include:

* real datasets
* personal documents
* production-scale outputs

---

## Positioning

This project reflects a broader approach:

> Building verifiable LLM systems for complex data environments.

---

## Author

Nicolas Roinard
LLM Systems Architect
