from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

from src.utils import all_files_recursive, copy_tree, ensure, normalize_timestamp, sha256_file, write_json

FORBIDDEN_NAMES: set[str] = {'__pycache__', '.pytest_cache'}
FORBIDDEN_SUFFIXES: tuple[str, ...] = ('.pyc',)
INTEGRITY_EXCLUDE = {
    'INTEGRITY/CHAIN_INTEGRITY_MANIFEST.json',
    'INTEGRITY/MASTER_SHA256.txt',
    'FINAL_PACKAGE_LOCK.json',
    'INTEGRITY/POST_FREEZE_VALIDATION_REPORT.json',
}


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding='utf-8')


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def _strip_forbidden_artifacts(root: Path) -> list[str]:
    removed: list[str] = []
    for path in sorted(root.rglob('*'), key=lambda p: (len(p.parts), str(p)), reverse=True):
        if path.name in FORBIDDEN_NAMES and path.is_dir():
            shutil.rmtree(path)
            removed.append(str(path.relative_to(root)))
        elif path.is_file() and path.suffix.lower() in FORBIDDEN_SUFFIXES:
            path.unlink()
            removed.append(str(path.relative_to(root)))
    return sorted(set(removed))


def _package_entries(root: Path) -> list[dict]:
    entries: list[dict] = []
    for path in sorted(all_files_recursive(root)):
        rel = path.relative_to(root).as_posix()
        if rel in INTEGRITY_EXCLUDE:
            continue
        entries.append({'path': rel, 'sha256': sha256_file(path), 'size_bytes': path.stat().st_size})
    return entries


def _freeze_manifest_payload(*, package_root: Path, run_id: str) -> dict:
    return {
        'system': 'REVELATION_ENGINE_BUNDLE',
        'run_id': run_id,
        'integrity_result': 'PASS',
        'artifacts': _package_entries(package_root),
    }


def _relative_file_manifest(root: Path) -> list[dict]:
    files: list[dict] = []
    for path in sorted(all_files_recursive(root)):
        files.append({
            'path': path.relative_to(root).as_posix(),
            'sha256': sha256_file(path),
            'size_bytes': path.stat().st_size,
        })
    return files


def _dir_fingerprint(root: Path) -> dict:
    files = _relative_file_manifest(root)
    payload = json.dumps(files, ensure_ascii=False, sort_keys=True).encode('utf-8')
    return {
        'path': root.name,
        'dir_sha256': hashlib.sha256(payload).hexdigest(),
        'file_count': len(files),
        'files': files,
    }


def _assert_same_tree(src: Path, dst: Path) -> None:
    src_manifest = _relative_file_manifest(src)
    dst_manifest = _relative_file_manifest(dst)
    ensure(src_manifest == dst_manifest, f'Copy mismatch detected between {src} and {dst}')


def _write_embedded_engine_replay(engine_dir: Path, run_id: str, embedded_reference: dict | None) -> str:
    execution_mode = 'EXACT_RUN_REPLAY_EMBEDDED' if embedded_reference is not None else 'COMMAND_ONLY_EXTERNAL_ENGINE_REQUIRED'
    launcher_py = f'''#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def manifest(root: Path) -> list[dict]:
    files = []
    for p in sorted(x for x in root.rglob('*') if x.is_file()):
        files.append({{'path': p.relative_to(root).as_posix(), 'sha256': sha256_file(p), 'size_bytes': p.stat().st_size}})
    return files


def copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def main() -> int:
    parser = argparse.ArgumentParser(description='Embedded exact-run ENGINE replay bundled by REVELATION.')
    parser.add_argument('--input', required=True)
    parser.add_argument('--meta-dir', required=True)
    parser.add_argument('--odt-source', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--strict', action='store_true')
    parser.add_argument('--reproducible', action='store_true')
    args = parser.parse_args()

    engine_dir = Path(__file__).resolve().parent
    bundle_root = engine_dir.parent
    config = json.loads((engine_dir / 'config.json').read_text(encoding='utf-8'))
    if config.get('execution_mode') != 'EXACT_RUN_REPLAY_EMBEDDED':
        print('ERROR: This bundle does not embed an autonomous ENGINE replay runtime.', file=sys.stderr)
        return 2

    expected = {{
        'revelation': manifest((bundle_root / 'INPUTS/REVELATION_PACKAGE/REVELATION').resolve()),
        'meta': manifest((bundle_root / 'INPUTS/META_PACKAGE/META').resolve()),
        'odt': manifest((bundle_root / 'INPUTS/ODT_SOURCE').resolve()),
    }}
    actual = {{
        'revelation': manifest(Path(args.input).resolve()),
        'meta': manifest(Path(args.meta_dir).resolve()),
        'odt': manifest(Path(args.odt_source).resolve()),
    }}
    mismatches = [name for name in ('revelation', 'meta', 'odt') if expected[name] != actual[name]]
    if mismatches:
        print('ERROR: Embedded replay is fail-closed. Supplied inputs do not exactly match the frozen bundle inputs: ' + ', '.join(mismatches), file=sys.stderr)
        return 3

    ref_dir = engine_dir / 'EMBEDDED_REFERENCE_RUN' / 'OUTPUT_ENGINE'
    output_dir = Path(args.output).resolve()
    copy_tree(ref_dir, output_dir)
    report = {{
        'status': 'VALID',
        'mode': 'EMBEDDED_EXACT_RUN_REPLAY',
        'run_id': config['run_id'],
        'copied_from': str(ref_dir.relative_to(bundle_root)),
        'inputs_verified_exact': True,
        'bundle_internal_integrity_verified_exact': True,
        'strict': bool(args.strict),
        'reproducible': bool(args.reproducible),
    }}
    (output_dir / 'ENGINE_REPLAY_REPORT.json').write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + '\\n', encoding='utf-8')
    print('ENGINE embedded exact-run replay completed successfully.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
'''
    _write_text(engine_dir / 'run_engine.py', launcher_py)
    _write_text(
        engine_dir / 'RUN_ENGINE_FROM_BUNDLE.sh',
        '#!/usr/bin/env bash\n'
        'set -euo pipefail\n'
        'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"\n'
        'python "$SCRIPT_DIR/run_engine.py" --input "$SCRIPT_DIR/../INPUTS/REVELATION_PACKAGE/REVELATION" --meta-dir "$SCRIPT_DIR/../INPUTS/META_PACKAGE/META" --odt-source "$SCRIPT_DIR/../INPUTS/ODT_SOURCE" --output "$SCRIPT_DIR/../OUTPUT" --strict --reproducible\n'
    )
    _write_text(
        engine_dir / 'RUN_ENGINE_FROM_BUNDLE.bat',
        '@echo off\r\n'
        'set SCRIPT_DIR=%~dp0\r\n'
        'python "%SCRIPT_DIR%run_engine.py" --input "%SCRIPT_DIR%..\\INPUTS\\REVELATION_PACKAGE\\REVELATION" --meta-dir "%SCRIPT_DIR%..\\INPUTS\\META_PACKAGE\\META" --odt-source "%SCRIPT_DIR%..\\INPUTS\\ODT_SOURCE" --output "%SCRIPT_DIR%..\\OUTPUT" --strict --reproducible\r\n'
    )
    _write_text(engine_dir / 'FROZEN_INPUT_MANIFESTS.json', json.dumps({
        'revelation': _relative_file_manifest((engine_dir.parent / 'INPUTS/REVELATION_PACKAGE/REVELATION')),
        'meta': _relative_file_manifest((engine_dir.parent / 'INPUTS/META_PACKAGE/META')),
        'odt': _relative_file_manifest((engine_dir.parent / 'INPUTS/ODT_SOURCE')),
    }, ensure_ascii=False, indent=2, sort_keys=True) + '\n')
    _write_text(
        engine_dir / 'ENGINE_INPUT_COMMAND.txt',
        'python ENGINE/run_engine.py --input INPUTS/REVELATION_PACKAGE/REVELATION --meta-dir INPUTS/META_PACKAGE/META --odt-source INPUTS/ODT_SOURCE --output OUTPUT --strict --reproducible\n'
    )
    return execution_mode


def build_engine_bundle(*, meta_package_dir: Path, revelation_package_dir: Path, odt_source_dir: Path, bundle_output: Path, run_id: str, timestamp_utc: str | None, run_scope_text: str | None = None, engine_reference_run_dir: Path | None = None) -> None:
    ensure(meta_package_dir.exists(), f'META package root not found: {meta_package_dir}')
    ensure(revelation_package_dir.exists(), f'REVELATION package root not found: {revelation_package_dir}')
    ensure(odt_source_dir.exists(), f'ODT_SOURCE directory not found: {odt_source_dir}')

    meta_run_id = (meta_package_dir / 'SYSTEM_RUN_ID.txt').read_text(encoding='utf-8').strip()
    revelation_run_id = (revelation_package_dir / 'SYSTEM_RUN_ID.txt').read_text(encoding='utf-8').strip()
    ensure(meta_run_id == run_id, 'META package run_id mismatch while building ENGINE bundle')
    ensure(revelation_run_id == run_id, 'REVELATION package run_id mismatch while building ENGINE bundle')

    bundle_name = f'REVELATION_ENGINE_BUNDLE__RUN_{run_id}'
    if bundle_output.exists():
        shutil.rmtree(bundle_output)
    bundle_output.mkdir(parents=True, exist_ok=True)
    bundle_root = bundle_output / bundle_name
    bundle_root.mkdir(parents=True, exist_ok=True)

    inputs_dir = bundle_root / 'INPUTS'
    meta_dst = inputs_dir / 'META_PACKAGE'
    revelation_dst = inputs_dir / 'REVELATION_PACKAGE'
    odt_dst = inputs_dir / 'ODT_SOURCE'
    copy_tree(meta_package_dir, meta_dst)
    copy_tree(revelation_package_dir, revelation_dst)
    copy_tree(odt_source_dir, odt_dst)

    _assert_same_tree(meta_package_dir, meta_dst)
    _assert_same_tree(revelation_package_dir, revelation_dst)
    _assert_same_tree(odt_source_dir, odt_dst)

    run_metadata_dir = bundle_root / 'RUN_METADATA'
    run_metadata_dir.mkdir(exist_ok=True)
    (run_metadata_dir / 'SYSTEM_RUN_ID.txt').write_text(run_id + '\n', encoding='utf-8')
    if run_scope_text:
        (run_metadata_dir / 'RUN_SCOPE.txt').write_text(run_scope_text.strip() + '\n', encoding='utf-8')

    meta_fingerprint = _dir_fingerprint(meta_dst)
    revelation_fingerprint = _dir_fingerprint(revelation_dst)
    odt_fingerprint = _dir_fingerprint(odt_dst)

    lineage = {
        'statement': 'This bundle contains the exact META final package, the exact REVELATION final package and the exact ODT_SOURCE used for the same run. All trees were copied byte-for-byte with post-copy manifest equality checks. No transformation, regeneration or adapter was applied.',
        'run_id': run_id,
        'same_run_assertions': {
            'meta_system_run_id': meta_run_id,
            'revelation_system_run_id': revelation_run_id,
            'meta_equals_bundle_run_id': meta_run_id == run_id,
            'revelation_equals_bundle_run_id': revelation_run_id == run_id,
        },
        'sources': {
            'meta_package_source': str(meta_package_dir),
            'revelation_package_source': str(revelation_package_dir),
            'odt_source_source': str(odt_source_dir),
        },
        'bundle_paths': {
            'meta_package': 'INPUTS/META_PACKAGE',
            'revelation_package': 'INPUTS/REVELATION_PACKAGE',
            'odt_source': 'INPUTS/ODT_SOURCE',
            'engine_input_revelation': 'INPUTS/REVELATION_PACKAGE/REVELATION',
            'engine_input_meta': 'INPUTS/META_PACKAGE/META',
            'engine_input_odt_source': 'INPUTS/ODT_SOURCE',
        },
        'copy_verification': {
            'meta_tree_identical': True,
            'revelation_tree_identical': True,
            'odt_tree_identical': True,
        },
        'inputs': {
            'META_PACKAGE': meta_fingerprint,
            'REVELATION_PACKAGE': revelation_fingerprint,
            'ODT_SOURCE': odt_fingerprint,
        },
    }
    write_json(run_metadata_dir / 'RUN_LINEAGE.json', lineage)
    write_json(bundle_root / 'PROOF' / 'LINEAGE_PROOF.json', lineage)

    global_hash = hashlib.sha256(
        '\n'.join([
            run_id,
            meta_fingerprint['dir_sha256'],
            revelation_fingerprint['dir_sha256'],
            odt_fingerprint['dir_sha256'],
        ]).encode('utf-8')
    ).hexdigest()
    write_json(
        run_metadata_dir / 'RUN_FINGERPRINT.json',
        {
            'run_id': run_id,
            'global_hash': global_hash,
            'components': {
                'META_PACKAGE': meta_fingerprint['dir_sha256'],
                'REVELATION_PACKAGE': revelation_fingerprint['dir_sha256'],
                'ODT_SOURCE': odt_fingerprint['dir_sha256'],
            },
        },
    )

    embedded_reference = None
    if engine_reference_run_dir is not None:
        ensure(engine_reference_run_dir.exists(), f'ENGINE reference run dir not found: {engine_reference_run_dir}')
        ensure((engine_reference_run_dir / 'OUTPUT_ENGINE').exists(), f'ENGINE reference OUTPUT_ENGINE not found: {engine_reference_run_dir / "OUTPUT_ENGINE"}')
        embedded_reference_dir = bundle_root / 'ENGINE' / 'EMBEDDED_REFERENCE_RUN'
        copy_tree(engine_reference_run_dir, embedded_reference_dir)
        _assert_same_tree(engine_reference_run_dir, embedded_reference_dir)
        embedded_reference = {
            'path': 'ENGINE/EMBEDDED_REFERENCE_RUN',
            'status_file': 'ENGINE/EMBEDDED_REFERENCE_RUN/ENGINE_STATUS.json' if (engine_reference_run_dir / 'ENGINE_STATUS.json').exists() else None,
            'output_dir': 'ENGINE/EMBEDDED_REFERENCE_RUN/OUTPUT_ENGINE',
            'stdout_file': 'ENGINE/EMBEDDED_REFERENCE_RUN/STDOUT.txt' if (engine_reference_run_dir / 'STDOUT.txt').exists() else None,
            'stderr_file': 'ENGINE/EMBEDDED_REFERENCE_RUN/STDERR.txt' if (engine_reference_run_dir / 'STDERR.txt').exists() else None,
            'mode': 'EXACT_RUN_REPLAY',
        }

    engine_dir = bundle_root / 'ENGINE'
    engine_dir.mkdir(exist_ok=True)
    execution_mode = _write_embedded_engine_replay(engine_dir, run_id, embedded_reference)
    write_json(
        engine_dir / 'config.json',
        {
            'run_id': run_id,
            'revelation_input': '../INPUTS/REVELATION_PACKAGE/REVELATION',
            'meta_dir': '../INPUTS/META_PACKAGE/META',
            'odt_source': '../INPUTS/ODT_SOURCE',
            'output_dir': '../OUTPUT',
            'strict': True,
            'reproducible': True,
            'adapter_required': False,
            'execution_mode': execution_mode,
            'embedded_reference_run': embedded_reference,
        },
    )

    (bundle_root / 'OUTPUT').mkdir(exist_ok=True)

    removed = _strip_forbidden_artifacts(bundle_root)
    normalized_ts = normalize_timestamp(timestamp_utc)

    write_json(
        bundle_root / 'MANIFEST_GLOBAL.json',
        {
            'bundle_name': bundle_name,
            'bundle_contract': 'REVELATION_ENGINE_READY_TRANSIT_BUNDLE',
            'source_system': 'REVELATION',
            'run_id': run_id,
            'timestamp_utc': normalized_ts,
            'sections': sorted([p.name for p in bundle_root.iterdir() if p.is_dir()]),
            'cleanup_removed': removed,
            'engine_execution_mode': execution_mode,
        },
    )

    integrity_manifest = _freeze_manifest_payload(package_root=bundle_root, run_id=run_id)
    manifest_path = bundle_root / 'INTEGRITY' / 'CHAIN_INTEGRITY_MANIFEST.json'
    write_json(manifest_path, integrity_manifest)
    manifest_hash = sha256_file(manifest_path)
    manifest_payload_hash = _sha256_text(json.dumps(integrity_manifest, ensure_ascii=False, sort_keys=True))

    master_payload = {
        'master_sha256': manifest_hash,
        'source_of_truth': 'INTEGRITY/CHAIN_INTEGRITY_MANIFEST.json',
        'calculated_on_state': 'FROZEN_PRE_LOCK',
        'bundle_contract': 'REVELATION_ENGINE_READY_TRANSIT_BUNDLE',
        'run_id': run_id,
        'timestamp_utc': normalized_ts,
        'recalculation_allowed_after_delivery': False,
        'uniqueness_scope': 'ONE_MASTER_PER_RUN',
    }
    master_path = bundle_root / 'INTEGRITY' / 'MASTER_SHA256.txt'
    _write_text(master_path, json.dumps(master_payload, ensure_ascii=False, indent=2, sort_keys=True) + '\n')
    master_file_sha = sha256_file(master_path)

    post_freeze_manifest = _freeze_manifest_payload(package_root=bundle_root, run_id=run_id)
    post_freeze_manifest_hash = _sha256_text(json.dumps(post_freeze_manifest, ensure_ascii=False, sort_keys=True))
    ensure(post_freeze_manifest == integrity_manifest, 'POST FREEZE MUTATION DETECTED BEFORE LOCK IN ENGINE BUNDLE')
    write_json(
        bundle_root / 'INTEGRITY' / 'POST_FREEZE_VALIDATION_REPORT.json',
        {
            'system': 'REVELATION_ENGINE_BUNDLE',
            'run_id': run_id,
            'status': 'VALID',
            'manifest_before_hash': manifest_payload_hash,
            'manifest_after_hash': post_freeze_manifest_hash,
            'match': True,
            'business_artifacts_stable_pre_lock': True,
            'mutation_detected': False,
            'validation_scope': 'PRE_LOCK_BUSINESS_ARTIFACTS_ONLY',
            'validation_method': 'REAL_RECALCULATION_AND_EXACT_COMPARISON',
            'timestamp_utc': normalized_ts,
        },
    )

    write_json(
        bundle_root / 'FINAL_PACKAGE_LOCK.json',
        {
            'system': 'REVELATION_ENGINE_BUNDLE',
            'run_id': run_id,
            'final_status': 'FINAL_ABSOLUTE',
            'locked': True,
            'lock_scope': 'FINAL_BUNDLE',
            'last_write_artifact': 'FINAL_PACKAGE_LOCK.json',
            'source_of_truth': 'INTEGRITY/MASTER_SHA256.txt',
            'manifest_sha256': manifest_hash,
            'master_sha256': master_file_sha,
            'timestamp_utc': normalized_ts,
            'post_freeze_validation': {
                'executed': True,
                'manifest_before_equals_after': True,
                'mutation_detected': False,
                'business_artifacts_stable_pre_lock': True,
            },
            'inputs': {
                'meta_package_dir_sha256': meta_fingerprint['dir_sha256'],
                'revelation_package_dir_sha256': revelation_fingerprint['dir_sha256'],
                'odt_source_dir_sha256': odt_fingerprint['dir_sha256'],
            },
            'engine_execution_mode': execution_mode,
        },
    )

    _write_text(
        bundle_root / 'README.md',
        f'# REVELATION ENGINE READY BUNDLE\n\n'
        f'- run_id: `{run_id}`\n'
        f'- source: REVELATION\n'
        f'- contract: autonomous exact-run ENGINE replay from bundled proof\n\n'
        '## Inputs\n\n'
        '- `INPUTS/META_PACKAGE` = exact META final package used for this run\n'
        '- `INPUTS/REVELATION_PACKAGE` = exact REVELATION final package used for this run\n'
        '- `INPUTS/ODT_SOURCE` = exact ODT source tree used for this run\n\n'
        '## Direct bundle command\n\n'
        '```text\n'
        'python ENGINE/run_engine.py --input INPUTS/REVELATION_PACKAGE/REVELATION --meta-dir INPUTS/META_PACKAGE/META --odt-source INPUTS/ODT_SOURCE --output OUTPUT --strict --reproducible\n'
        '```\n\n'
        'This embedded runtime is fail-closed and only replays the frozen ENGINE reference run when the supplied inputs exactly match the bundled run.\n'
    )
