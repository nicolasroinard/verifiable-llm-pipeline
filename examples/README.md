# Examples

This directory contains example walkthroughs of the verifiable LLM pipeline.

## Project Alpha Demo

The `project_alpha_demo/` directory contains a simplified public walkthrough of the end-to-end architecture:

1. **Input** : Three sample ODT documents describing a fictional project
2. **ODT Layer** : Structured ingestion and document preservation
3. **META Layer** : Entity extraction, graph construction, lineage tracking
4. **REVELATION Layer** : Deterministic observations and validation criteria
5. **ENGINE Layer** : Public output projection

### How to read this demo

This example is a public walkthrough, not a complete runnable orchestration.
It is designed to show how the architecture behaves across layers without exposing internal orchestration, validation, or packaging tooling.

1. Start with `project_alpha_demo/README.md` for an overview
2. Examine the input documents in `inputs/`
3. Follow the transformation through each layer in `expected_public_outputs/`

### Important notes

- These are fictional documents for demonstration purposes
- The outputs shown are simplified public views, created to illustrate architecture patterns
- The actual pipeline (internal) is more comprehensive and includes validation, packaging, and orchestration infrastructure
- This demo illustrates the public subset of the architecture
