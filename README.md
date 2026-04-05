# ODT V7.5 — Pipeline déterministe

Pipeline Python de traitement documentaire ODT produisant une archive structurée, traceable, reconstituable et validée.

## Structure du projet

```
run_odt_v75.py          Orchestrateur — point d'entrée CLI
src/
  __init__.py
  protocol.py           Constantes partagées (FREEZE_POLICY, TOP_LEVEL_MANIFEST_FILES…)
  utils.py              I/O générique (sha256, write_json, write_text…)
  ids.py                Dérivation SYSTEM_GLOBAL_ID, document_id, timestamp
  extractor.py          Validation ODT, extraction texte, segmentation
  manifests.py          Calcul hashes, écriture manifests, validation finale
  builders.py           Constructeurs de tous les artefacts JSON du protocole
requirements.txt        Aucune dépendance tierce requise
demo/
  input/                ODT de test (PARTIE 1 et PARTIE 2)
  output/               Archive de référence générée sur PARTIE 2
docs/
  PASSATION_NOTES.md    Notes de passation inter-sessions
```

## Pipeline ODT V7.5 — ordre des étapes (ne pas modifier)

```
ODT → VALIDATION → EXTRACTION → ARTEFACTS → STRUCTURE
    → EXECUTION → EXPLOITATION → VALIDATION FINALE → ARCHIVE_COMPLETE_V7_5
```

Détail des 15 étapes dans `build_archive()` de `run_odt_v75.py`.

## Prérequis

Python 3.10+. Aucune dépendance tierce.

```bash
python --version   # >= 3.10
```

## Utilisation

### Run standard

```bash
python run_odt_v75.py \
    --input "demo/input/Portefolio Projet Pippeline Livre PARTIE 1.odt" \
    --output demo/output/ARCHIVE_COMPLETE_V7_5
```

### Run reproductible avec timestamp fixe (output bit-à-bit identique)

En mode `--reproducible --fixed-timestamp`, deux runs sur le même ODT produisent
une archive **cryptographiquement identique** quel que soit le dossier de sortie :
`MASTER_SHA256.txt`, `CHAIN_INTEGRITY_MANIFEST.json` et `VALIDATION_FINALE.json`
sont tous identiques entre les deux runs.

```bash
python run_odt_v75.py \
    --input "demo/input/Portefolio Projet Pippeline Livre PARTIE 1.odt" \
    --output demo/output/ARCHIVE_COMPLETE_V7_5_REPRO \
    --reproducible \
    --fixed-timestamp 2000-01-01T00:00:00Z
```

Note : en mode `--reproducible` sans `--fixed-timestamp`, le timestamp est fixé à
`2000-01-01T00:00:00Z` par défaut. La valeur `archive_output_dir` dans
`logs/INTERNAL_EXPERT_AUDIT.json` est neutralisée en `__REPRODUCIBLE_MODE__`
pour garantir l'identité cryptographique entre runs.

### Options disponibles

```
--input             Chemin vers le fichier .odt source (obligatoire)
--output            Dossier de sortie (obligatoire)
--reproducible      Mode déterministe (même output pour même input)
--fixed-timestamp   Timestamp UTC ISO 8601 fixe (à utiliser avec --reproducible)
--system-global-id  Override optionnel du SYSTEM_GLOBAL_ID
--verbose / -v      Logs DEBUG sur stderr
--quiet / -q        Supprime tous les logs (JSON stdout uniquement)
--help              Aide complète
```

## Ce que produit le pipeline

```
ARCHIVE_COMPLETE_V7_5/
  SYSTEM_GLOBAL_ID.txt
  SYSTEM_VERSION.json
  ODT_LAYER_STATUS.json
  ARCHIVE_MANIFEST.json
  CHAIN_INTEGRITY_MANIFEST.json
  MASTER_SHA256.txt
  SOURCE/ODT_NATIVE/         Copie native de l'ODT source
  archive/
    GLOBAL_SYSTEM_STATUS.json
  artefacts/
    ARTEFACT_MASTER.json
    ARTEFACT_paragraphs.json
    ARTEFACT_segments.json
  structure/
    ARTEFACT_STRUCTURE_TREE.json
    ARCHIVE_STRUCTURE_MAP.json
    SOURCE_DOCUMENT_METADATA.json
    ARTEFACT_INTEGRITY_REPORT.json
  meta/
    GLOBAL_ARCHIVE_STATS.json
    GLOBAL_DOCUMENT_CORPUS.json
    ARCHIVE_INDEX_MASTER.json
    QUERY_MAP.json
    FINALIZATION_POLICY.json
    ARTEFACT_RELATIONS.json
  logs/
    INPUT_VALIDATION.json
    ARTEFACT_EXTRACTION_LOG.json
    PIPELINE_EXECUTION_META.json
    ARTEFACT_EXECUTION_META.json
    VALIDATION_FINALE.json
    INTERNAL_EXPERT_AUDIT.json
  bootstrap/
    DOSSIER_BOOT_SUMMARY.txt
```

## Extensions forensiques additives

- `meta/FINALIZATION_POLICY.json`
- `meta/ARTEFACT_RELATIONS.json`
- `logs/INTERNAL_EXPERT_AUDIT.json`
- Champs `provenance_mode`, `traceability_level`, `reconstructibility_path` dans chaque artefact

Ces enrichissements sont additifs et compatibles avec les consommateurs META_FUSION en aval.

## Compatibilité chaîne

`ODT_V7_5` → `META_FUSION_V9_2` → `META` / `REVELATION` / `ENGINE` / `SESSION`
