# REVELATION V8.2 — Deterministic Analytical Pipeline

Script exécutable pour **REVELATION V8.2 — FINAL LOCK** : transforme les artefacts **META_FUSION V9.2** en analyses strictement déterministes, traçables et reconstructibles.

## Vue d'ensemble

**REVELATION** n'est pas un moteur IA ou un agent autonome.

C'est une **couche analytique déterministe** qui :
- Charge 5 artefacts META validés
- Produit ~23 unités d'analyse fermées
- Construit un graphe de trace justifié
- Génère des métriques de conformité (coverage, integrity, traceability)
- Opère **sans aucune inférence, hypothèse ou complétion intelligente**

Chaque sortie est :
- ✅ Reconstructible depuis META
- ✅ Explicitement tracée (input_refs + evidence_binding)
- ✅ Déterministe (replayable)
- ✅ Auditable (manifest + checksums)

## Pipeline implémenté

```
VALIDATION (5 META artifacts)
  → ANALYSIS_UNITS (23 deterministic units)
    → TRACE_GRAPH (dependency graph)
      → LAYER_STATUS (metrics + checks)
        → MANIFEST (final audit artifact)
          → PACKAGE [optional] (transport)
```

## Structure du repo

```
revelation_v82_pipeline/
  README.md                              # This file
  requirements.txt                       # stdlib only
  run_revelation.py                      # Main entrypoint
  src/
    __init__.py
    models.py                            # RevelationContext, Check
    utils.py                             # JSON I/O, hashing, timestamps
    scoring.py                           # Metrics & audit functions
    manifest.py                          # Integrity manifest building
    layers/
      __init__.py
      validation_layer.py                # Load & validate 5 META artifacts
      unit_extraction_layer.py           # Generate analysis units (MVP core)
      trace_graph_layer.py               # Build dependency graph
      status_layer.py                    # Compute status & checks
      package_layer.py                   # Optional package assembly
  demo/
    input/
      meta_package_reference/            # Test case 1 (7 documents)
      meta_package_reference_test2/      # Test case 2 (forensic)
    output/
      REVELATION_REFERENCE_RUN/          # Reference outputs (test 1)
      REVELATION_REFERENCE_RUN_TEST2/    # Reference outputs (test 2)
  docs/
    PASSATION_NOTES.md                   # Forensic background & lessons learned
```

## Contrat d'entrée (5 artefacts META obligatoires)

Le répertoire `--input` doit contenir :

```
META_GLOBAL_GRAPH.json              # Node & edge definitions
GLOBAL_DOCUMENT_CORPUS.json         # Document & segment metadata
GLOBAL_SEMANTIC_INDEX.json          # Semantic reference index
META_LAYER_STATUS.json              # META layer status (VALID/DEGRADED)
CHAIN_INTEGRITY_MANIFEST.json       # META integrity manifest (integrity_result = PASS)
```

**Rejet immédiat si** :
- Un artefact manque
- JSON malformé
- `META_LAYER_STATUS.status` ∉ {VALID, DEGRADED}
- `CHAIN_INTEGRITY_MANIFEST.integrity_result ≠ PASS`
- run_id incohérent

## Sorties canoniques (4 artefacts)

Le répertoire `--output` reçoit :

```
REVELATION_ANALYSIS_SET.json        # Array of ~23 analysis units
REVELATION_TRACE_GRAPH.json         # Nodes (units) & edges (dependencies)
REVELATION_LAYER_STATUS.json        # Status, metrics, run_id
CHAIN_INTEGRITY_MANIFEST.json       # Audit manifest (checks + hashes + integrity)
```

Chaque unité est une struct fermée :
```json
{
  "analysis_id": "AN_COUNT_SEGMENTS_001",
  "operation_type": "COUNT",                // ∈ {EXTRACT, GROUP, LINK, COMPARE, COUNT, CONFLICT_DETECT}
  "input_refs": ["document_id:doc_1", "node_id:node_1"],
  "transformation_rule": "Count segment node ids for one document",
  "output_schema_type": "INTEGER",
  "output_payload": 42,
  "evidence_binding": {
    "document_ids": ["doc_1"],
    "node_ids": ["node_1", "node_2", ...]
  },
  "deterministic_proof": {
    "replayable": true,
    "rule_applied": "COUNT(segment_node_ids WHERE doc_id=doc_1)"
  },
  "run_id": "META-V9_2-20260404T151500Z-7ARCHIVES",
  "status": "VALID"
}
```

## Usage

### Run minimal

```bash
python run_revelation.py \
  --input demo/input/meta_package_reference \
  --output demo/output/REVELATION_TEST_RUN
```

### Run déterministe (avec timestamps reproductibles)

```bash
python run_revelation.py \
  --input demo/input/meta_package_reference \
  --output demo/output/REVELATION_TEST_RUN \
  --reproducible
```

### Run strict (échoue si status ≠ VALID)

```bash
python run_revelation.py \
  --input demo/input/meta_package_reference \
  --output demo/output/REVELATION_TEST_RUN \
  --strict
```

### Run avec package assembly

```bash
python run_revelation.py \
  --input demo/input/meta_package_reference \
  --output demo/output/REVELATION_TEST_RUN \
  --odt-source /path/to/ODT_SOURCE \
  --package-output demo/output/REVELATION_PACKAGE
```

### Tous les flags

```bash
python run_revelation.py \
  --input demo/input/meta_package_reference \
  --output demo/output/REVELATION_TEST_RUN \
  --strict \
  --reproducible \
  --fixed-timestamp 2026-04-04T20:54:24Z \
  --odt-source /path/to/ODT_SOURCE \
  --package-output demo/output/REVELATION_PACKAGE
```

## Sortie en cas d'erreur

Si une violation de protocole est détectée, REVELATION s'arrête et écrit :

```json
{
  "status": "REJECTED",
  "coverage_ratio": 0.0,
  "deterministic_integrity": 0.0,
  "traceability_score": 0.0,
  "run_id": null,
  "reason": "Explicit protocol violation message"
}
```

Code retour : `2`

## Ce que REVELATION produit vraiment

✅ **Fait** :
- Extraction stricte de profils documentaires
- Comptages déterministes par document et globaux
- Synthèse de type `GROUP` sans inférence
- Scan déterministe des duplications (depuis `META_DELTA_REPORT.json`)
- Trace graph justifié (INPUT_DEPENDENCY, DERIVATION)
- Contrôle explicite de `input_refs` et `evidence_binding`
- Manifest final cohérent avec l'état livré
- Métriques de conformité (coverage, integrity, traceability)

❌ **Ne fait pas** :
- Résumé intelligent
- Déduction thématique
- Rapprochement implicite
- Enrichissement sémantique
- Complétion automatique d'un META incomplet
- Transformation en moteur "intelligent"

## Principes directeurs

1. **Déterminisme total** : même input → même output, toujours
2. **Zéro inférence** : opérations autorisées uniquement
3. **Traçabilité complète** : chaque output pointe vers sa source META
4. **Clôture structurelle** : unités fermées, no optional fields
5. **Reconstructibilité** : peut rejouer la computation exactement
6. **Audit-proof** : manifest signifie qu'on peut reverify indépendamment

## Cas de test de référence

Deux cas de test de référence sont fournis avec outputs pré-calculés :

**Test 1 : 7 documents, 23 unités d'analyse**
```bash
python run_revelation.py \
  --input demo/input/meta_package_reference \
  --output /tmp/test_ref_run
# Expected: Exact match with demo/output/REVELATION_REFERENCE_RUN/REVELATION_ANALYSIS_SET.json
```

**Test 2 : Forensic reproduction (TEST2_RUN_ID)**
```bash
python run_revelation.py \
  --input demo/input/meta_package_reference_test2/META \
  --output /tmp/test_ref_run_test2
# Expected: Exact match with demo/output/REVELATION_REFERENCE_RUN_TEST2/REVELATION_ANALYSIS_SET.json
```

Vérifier l'appairage :
```bash
python -c "
import json
with open('/tmp/test_ref_run/REVELATION_ANALYSIS_SET.json') as f:
    gen = json.load(f)
with open('demo/output/REVELATION_REFERENCE_RUN/REVELATION_ANALYSIS_SET.json') as f:
    ref = json.load(f)
print('MATCH ✅' if gen == ref else 'DIFF ❌')
"
```

## Dépendances

```
Python 3.9+
stdlib only: pathlib, json, re, hashlib, shutil, datetime
```

## Passation & Forensic Notes

Voir `docs/PASSATION_NOTES.md` pour :
- Historique des phases (ODT → META → REVELATION)
- Erreurs rencontrées et corrections apportées
- Leçons apprises sur le système
- Règles absolues pour les modifications futures

## Garanties

✅ Sorties **déterministes** : même input → bit-exact same output  
✅ **Traçables** : input_refs + evidence_binding explicites  
✅ **Reconstructibles** : graphe de trace complet  
✅ **Auditables** : manifest + SHA-256 checksums  
✅ **Non-inferential** : zéro intelligence, opérations fermées

## Support

Pour questions ou issues liées au protocole REVELATION V8.2 :
1. Vérifier `docs/PASSATION_NOTES.md`
2. Vérifier que les 5 artefacts META sont présents et valides
3. Vérifier `CHAIN_INTEGRITY_MANIFEST.integrity_result = PASS`
4. Exécuter un cas de test de référence pour confirmation

