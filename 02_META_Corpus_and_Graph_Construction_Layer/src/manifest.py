from __future__ import annotations

from src.utils import sha256_file, write_json


MANIFEST_EXCLUDE = {"CHAIN_INTEGRITY_MANIFEST.json", "MASTER_SHA256.txt"}
MASTER_EXCLUDE = {"MASTER_SHA256.txt"}


def build_manifest_and_master(context) -> None:
    output_artifacts = []
    for path in sorted(context.output_dir.iterdir()):
        if path.is_file() and path.name not in MANIFEST_EXCLUDE:
            output_artifacts.append({"path": path.name, "sha256": sha256_file(path), "size_bytes": path.stat().st_size})

    manifest = {
        "input_artifacts": [
            {"path": archive.archive_path.name, "sha256": sha256_file(archive.archive_path), "size_bytes": archive.archive_path.stat().st_size}
            for archive in context.source_archives
        ],
        "output_artifacts": output_artifacts,
        "checks": [check.as_dict() for check in context.checks],
        "run_id": context.run_id,
        "integrity_result": "PASS" if all(check.result == "PASS" for check in context.checks) else "FAIL",
    }
    write_json(context.output_dir / "CHAIN_INTEGRITY_MANIFEST.json", manifest)

    master_lines = []
    for path in sorted(context.output_dir.iterdir()):
        if path.is_file() and path.name not in MASTER_EXCLUDE:
            master_lines.append(f"{sha256_file(path)}  {path.name}")
    (context.output_dir / "MASTER_SHA256.txt").write_text("\n".join(master_lines) + "\n", encoding="utf-8")
