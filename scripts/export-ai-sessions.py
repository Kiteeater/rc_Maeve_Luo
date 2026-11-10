"""Archive this project's original AI session files with integrity metadata."""

import argparse
from collections import defaultdict
from compression import zstd
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.parse import unquote
import zipfile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--home", type=Path, default=Path.home())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    project = args.project.resolve()
    home = args.home.resolve()
    output = args.output or project / "ai-sessions.zip"
    started_at = datetime.now(timezone.utc).isoformat()
    sources: dict[str, Path] = {}
    sessions: dict[str, set[str]] = {"codex": set(), "claude": set(), "grok": set()}

    for name in ("sessions", "archived_sessions", "archives"):
        root = home / ".codex" / name
        for path in sorted(root.rglob("*")):
            if path.name.endswith(".jsonl.zst"):
                with zstd.open(path, "rb") as stream:
                    first_line = stream.readline()
            elif path.suffix == ".jsonl":
                with path.open("rb") as stream:
                    first_line = stream.readline()
            else:
                continue
            if not first_line:
                continue
            metadata = json.loads(first_line)
            if metadata.get("type") != "session_meta":
                continue
            payload = metadata["payload"]
            cwd = payload.get("cwd")
            if not cwd or not Path(cwd).resolve().is_relative_to(project):
                continue
            sessions["codex"].add(payload["id"])
            sources[f"codex/{name}/{path.relative_to(root).as_posix()}"] = path

    claude_root = home / ".claude" / "projects"
    for directory in sorted(claude_root.glob("*")):
        matching_sessions: set[str] = set()
        for path in sorted(directory.glob("*.jsonl")):
            with path.open("rb") as stream:
                for line in stream:
                    record = json.loads(line)
                    cwd = record.get("cwd")
                    if cwd:
                        if Path(cwd).resolve().is_relative_to(project):
                            matching_sessions.add(path.stem)
                        break
        if not matching_sessions:
            continue
        sessions["claude"].update(matching_sessions)
        for path in sorted(directory.rglob("*")):
            if not path.is_file():
                continue
            sources[f"claude/{path.relative_to(claude_root).as_posix()}"] = path

    grok_root = home / ".grok" / "sessions"
    for directory in sorted(grok_root.glob("*")):
        if not Path(unquote(directory.name)).resolve().is_relative_to(project):
            continue
        for path in sorted(directory.rglob("*")):
            if not path.is_file():
                continue
            sources[f"grok/{path.relative_to(grok_root).as_posix()}"] = path
            if path.name == "chat_history.jsonl":
                sessions["grok"].add(path.parent.name)

    if not sources:
        raise SystemExit(f"No AI session files found for {project}")

    entries: list[dict[str, str | int]] = []
    file_counts: dict[str, int] = defaultdict(int)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output.with_suffix(output.suffix + ".tmp")
    with zipfile.ZipFile(temporary_output, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for archive_path, source in sorted(sources.items()):
            content = source.read_bytes()
            captured_at = datetime.now(timezone.utc).isoformat()
            archive.writestr(archive_path, content)
            entries.append({
                "archive_path": archive_path,
                "source_path": str(source),
                "size_bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "captured_at_utc": captured_at,
            })
            file_counts[archive_path.split("/", 1)[0]] += 1
        manifest = {
            "format_version": 1,
            "project": str(project),
            "capture_started_at_utc": started_at,
            "capture_finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": "Project cwd and descendant cwd; original on-disk bytes, without editing or redaction.",
            "snapshot_note": "Files are captured individually. Messages written after capture require a new export.",
            "sessions": {name: sorted(ids) for name, ids in sessions.items()},
            "file_counts": dict(file_counts),
            "files": entries,
        }
        archive.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")

    with zipfile.ZipFile(temporary_output) as archive:
        for entry in entries:
            content = archive.read(entry["archive_path"])
            if len(content) != entry["size_bytes"] or hashlib.sha256(content).hexdigest() != entry["sha256"]:
                raise RuntimeError(f"Archive integrity check failed: {entry['archive_path']}")
    temporary_output.replace(output)
    checksum = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(output.suffix + ".sha256").write_text(f"{checksum}  {output.name}\n", encoding="utf-8")
    print(json.dumps({
        "archive": str(output),
        "archive_bytes": output.stat().st_size,
        "sessions": {name: len(ids) for name, ids in sessions.items()},
        "file_counts": dict(file_counts),
        "sha256": checksum,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
