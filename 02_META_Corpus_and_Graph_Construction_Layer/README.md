# META_FUSION V9.2 — Script Archive

Archive script industrielle pour exécuter **META_FUSION V9.2 — FINAL LOCK** sur des archives **ODT V7.5 validées**.

## Objectif

Cette archive ne cherche pas à réinventer le protocole. Elle cherche à le rendre :

- exécutable
- transmissible
- auditable
- rejouable sur cas réel
- propre pour passation

## Pipeline implémenté

`VALIDATION → AGREGATION → NORMALISATION → CROSS_ARCHIVE → DEDUP → CONFLICT → CORPUS → INDEX → GRAPH → DELTA → VALIDATION_FINALE`

## Structure

```text
meta_fusion_v92_pipeline/
  README.md
  requirements.txt
  run_meta_fusion.py
  src/
    utils.py
    models.py
    scoring.py
    manifest.py
    layers/
      validation_layer.py
      aggregation_layer.py
      normalization_layer.py
      cross_archive_layer.py
      dedup_layer.py
      conflict_layer.py
      corpus_layer.py
      index_layer.py
      graph_layer.py
      delta_layer.py
  demo/
    input/
    output/
  docs/
    PASSATION_NOTES.md
```

## Usage

Run de référence :

```bash
python run_meta_fusion.py \
  --input demo/input \
  --output demo/output/META_FUSION_REFERENCE_RUN \
  --run-id SYSTEM_RUN_META_V92_REFERENCE
```

Run durci :

```bash
python run_meta_fusion.py \
  --input demo/input \
  --output demo/output/META_FUSION_REFERENCE_RUN \
  --run-id SYSTEM_RUN_META_V92_REFERENCE \
  --strict \
  --reproducible
```

## Contrat d'entrée

Chaque archive source doit fournir au minimum :

- `GLOBAL_DOCUMENT_CORPUS.json`
- `ARTEFACT_segments.json`
- `CHAIN_INTEGRITY_MANIFEST.json`
- `SOURCE_DOCUMENT_METADATA.json`
- `ARCHIVE_MANIFEST.json`
- `MASTER_SHA256.txt`
- `ODT_LAYER_STATUS.json`

## Contrat de sortie

Le script écrit :

- `META_GLOBAL_GRAPH.json`
- `GLOBAL_DOCUMENT_CORPUS.json`
- `GLOBAL_SEMANTIC_INDEX.json`
- `META_LAYER_STATUS.json`
- `CHAIN_INTEGRITY_MANIFEST.json`
- `META_DELTA_REPORT.json`
- `MASTER_SHA256.txt`

## Durcissements ajoutés

Sans changer le protocole META, cette archive ajoute :

- un mode `--strict` qui bloque sur refs orphelines ou provenance incomplète
- un mode `--reproducible` qui explicite l'exigence d'ordre déterministe
- une validation d'intégrité réelle des types, scores, `node_refs`, `run_id` et `ingest_timestamp_utc`
- une clôture manifest/master cohérente avec l'état livré

## Positionnement de cette archive

Cette archive est volontairement :

- **petite mais vraie**
- modulaire sans sur-ingénierie
- alignée avec le vécu forensic du chantier
- pensée pour reprise par une autre session ou une autre personne

## Limites assumées du MVP

- pas d'inférence de conflits implicites
- déduplication forte par `content_hash`
- pas d'enrichissement sémantique décoratif

Ces limites sont cohérentes avec la règle protocolaire :
**aucune inférence implicite**.


## Durcissements V10/10

- `--fixed-timestamp` pour figer la provenance en mode reproductible.
- Références d’index segmentaires en `REF_TYPE = NODE` vers des IDs canoniques uniques.
- Provenance `DOCUMENT` explicitement typée avec `segment_id = __NA_DOCUMENT_NODE__`.
- Validation amont renforcée sur `MASTER_SHA256.txt` et `CHAIN_INTEGRITY_MANIFEST.json` des archives ODT sources.
- Séquence de clôture explicitée : outputs écrits → manifest/master → contrôle anti-mutation.


## Reference run command

```bash
python run_meta_fusion.py \
  --input demo/input \
  --output demo/output/META_FUSION_REFERENCE_RUN \
  --run-id SYSTEM_RUN_META_V92_REFERENCE_20260404T163500Z \
  --strict --reproducible --fixed-timestamp 2026-04-04T16:35:00Z
```
