# Verifiable LLM Pipeline

Turning probabilistic LLM outputs into structured, traceable, and verifiable systems.

---

## Repository Status & Scope

**This repository is a public demonstration subset of a broader end-to-end pipeline.**

The internal pipeline was designed and reviewed through internal audit workflows as a file-level and byte-sensitive, multi-layer workflow:

```text
ODT → META → REVELATION → ENGINE
```

The public repository intentionally exposes only the parts that are safe, readable, and useful for demonstrating the architecture:

- document ingestion and structuring
- graph construction and corpus building
- traceability patterns (lineage tracking, reference integrity)
- deterministic analysis without inference
- minimal projection layer structure

This is intentional and correct.

### What's intentionally NOT included in this public subset

- **Full internal E2E orchestration** — The complete end-to-end system exists and is reviewed through internal audit workflows, but is intentionally not published in this public subset
- **Internal validation / freeze / transport tooling** — Kept private because it is domain-specific and tightly coupled to internal forensic packaging infrastructure
- **Complete ENGINE implementation** — Intentionally reduced in the public repository to avoid exposing internal projection and validation infrastructure
- **Qualification and forensic suite** — Internal tools for comprehensive system validation, not suitable for public distribution

This is by design. The goal is to demonstrate the architecture patterns, not to expose the complete internal system infrastructure.

### Positioning

This repository demonstrates an **architecture pattern** for AI-driven document workflows with emphasis on governance, traceability, and auditability.

It is **not**:
- A commercial product
- A complete system (intentionally simplified for clarity)
- A prompt engineering framework
- A machine learning research paper

It **is**:
- A system architecture for human-in-the-loop AI workflows
- A demonstration of governance and traceability patterns in LLM pipelines
- A blueprint for building verifiable, auditable document processing systems

---

## Development

This project was developed with intensive AI assistance, including ChatGPT and Claude.

My role was to design and drive the architecture, define the constraints, structure the layers, validate outputs, audit regressions, and progressively turn a complex workflow into a traceable system.

The value of the project is not "AI wrote code".

The value is the system design, orchestration, validation logic, and controlled workflow around AI-generated and AI-assisted outputs.

---

## Overview

This repository presents a runnable subset of a larger AI-assisted system designed to process complex document corpora.

The goal is not to improve prompts.

The goal is to build a system layer that makes LLM-based workflows more reliable, auditable, traceable, and reproducible.

---

## Why

LLM outputs are probabilistic by nature.

When applied to long-context or multi-document workflows, this often creates:

- loss of coherence
- fragmented memory
- lack of traceability
- unstable outputs
- difficulty validating results

This project explores a different approach:

> instead of relying on better prompts, build a structured system around the LLM workflow.

---

## Core idea

The system transforms raw document inputs into structured, traceable artifacts through separated processing layers.

```text
Raw documents
↓
Document ingestion
↓
Corpus structuring
↓
Graph construction
↓
Deterministic analysis
↓
Structured outputs
↓
Validation / audit
```

The key design principle is:

> recognize, structure and validate — do not freely infer.

---

## Implemented layers

This repository currently exposes the main document-processing layers.

### 01 — ODT: Document Ingestion & Structuring

- ingests raw `.odt` documents
- segments and structures content
- preserves source integrity
- produces traceable document archives

Entry point:

```text
01_ODT_Document_Ingestion_and_Structuring_Layer/run_odt_v75.py
```

---

### 02 — META: Corpus & Graph Construction

- merges structured documents into a global corpus
- builds graph-like relationships between segments, nodes and references
- creates a structured data layer for downstream processing
- demonstrates data lineage tracking (lineage_layer)
- demonstrates reference integrity validation (reference_integrity_layer)

Entry point:

```text
02_META_Corpus_and_Graph_Construction_Layer/run_meta_fusion.py
```

---

### 03 — REVELATION: Deterministic Analysis & Transformation

- performs controlled analysis on structured corpus artifacts
- separates observation from interpretation
- prepares structured analysis units for downstream projection
- demonstrates deterministic trace closure (canonical_trace_closure_layer)
- demonstrates pipeline bridging (engine_bundle_layer)

Status:

```text
public subset / core layers exposed
```

---

### 04 — ENGINE: Output Structuring & Projection

- structures downstream outputs from validated analysis units
- keeps output generation separated from source ingestion and graph construction
- supports the system's validation-oriented workflow

Status:

```text
public subset / minimal (core coupling to internal validation infrastructure)
```

---

## Full system direction

The broader system includes additional validation and grounding layers:

- PROOF — evidence reconstruction
- REANCHOR — source grounding
- COGNITIVE — controlled reasoning and validation
- SESSION_MANAGER — orchestration and monitoring
- MEMORY_GRAPH_AGENT — memory and consistency

These layers are part of the broader architecture, but are not fully exposed here.

---

## Repository structure

```text
verifiable-llm-pipeline/
  README.md

  01_ODT_Document_Ingestion_and_Structuring_Layer/
    run_odt_v75.py
    requirements.txt
    src/
    demo/
      input/

  02_META_Corpus_and_Graph_Construction_Layer/
    run_meta_fusion.py
    requirements.txt
    src/

  03_REVELATION_Deterministic_Analysis_and_Transformation_Layer/
    README.md
    run_revelation.py
    src/

  04_ENGINE_Output_Structuring_and_Projection_Layer/
    README.md
    # minimal public placeholder
```

---

## Demo

A minimal demo input is included in the ODT layer.

The demo is intentionally small and anonymized.

It is not meant to reproduce the full original corpus.  
It is meant to show the processing logic:

```text
input document
↓
structured archive
↓
corpus layer
↓
traceable system artifacts
```

---

## How to run

Install dependencies layer by layer:

```bash
pip install -r 01_ODT_Document_Ingestion_and_Structuring_Layer/requirements.txt
pip install -r 02_META_Corpus_and_Graph_Construction_Layer/requirements.txt
```

Run the first layers:

```bash
python 01_ODT_Document_Ingestion_and_Structuring_Layer/run_odt_v75.py
python 02_META_Corpus_and_Graph_Construction_Layer/run_meta_fusion.py
```

---

## Output

Outputs are generated at runtime and are not included in this repository.

Depending on the executed layer, the system can produce:

- structured document archives
- corpus-level JSON artifacts
- graph-like representations
- validation metadata
- traceable intermediate representations
- audit-oriented outputs

---

## Design principles

- deterministic processing layers
- source preservation
- traceability by design
- no implicit inference
- reproducible outputs
- separated responsibilities between layers
- human-readable audit trail
- validation before projection

---

## What this is not

This project is not:

- a foundation model
- an AGI system
- a chatbot
- a prompt collection
- a production SaaS
- a fully industrialized framework
- a claim that everything was coded manually from scratch

It is a public technical subset of a larger AI-assisted system architecture.

---

## Author

Nicolas Roinard  
LLM Systems Architect / AI Workflow Governance
