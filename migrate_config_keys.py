#!/usr/bin/env python3
"""
Migrate Sensible configuration JSON in place without renaming config names.

For each (old_id -> new_id) in the field map, updates:
  - dict keys equal to old_id (uncommon in SenseML)
  - field ids: \"id\": \"old_id\" -> \"id\": \"new_id\"

Default map covers commission-statement field renames; override with --field-map or
--old-key/--new-key for a single pair.

Loads variables from a .env file in the current directory (or above) if present.
You can still override with real environment variables. Requires SENSIBLE_API_KEY.
Pass a folder name, use --document-type-id, or pass --folders-csv to process many folders.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

API_BASE_DEFAULT = "https://api.sensible.so/v0"

# Default SenseML field id renames (old -> new). Order is stable; avoid mapping a
# new name to another row's old id in the same pass.
DEFAULT_FIELD_RENAMES: Dict[str, str] = {
    "policy_name": "insured_name",
    "comm_rate": "commission_percentage",
    "prem_applied": "gross_premium_amount",
    "orig_eff_date": "policy_effective_date",
    "product_name": "policy_coverage_type",
    "billing_detail": "commission_payments",
}


def normalize_field_map(data: Any) -> Dict[str, str]:
    if not isinstance(data, dict):
        raise ValueError("field map must be a JSON object mapping old names to new names")
    out: Dict[str, str] = {}
    for k, v in data.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ValueError(f"field map keys and values must be strings, got {k!r} -> {v!r}")
        k2, v2 = k.strip(), v.strip()
        if not k2 or not v2:
            raise ValueError("field map entries must be non-empty strings")
        if k2 in out:
            raise ValueError(f"duplicate old field name in map: {k2!r}")
        out[k2] = v2
    return out


def load_field_map_path(path: Path) -> Dict[str, str]:
    text = path.read_text(encoding="utf-8")
    return normalize_field_map(json.loads(text))


def read_folder_names_from_csv(
    path: Path,
    *,
    column: str,
    no_header: bool,
) -> List[str]:
    """
    Read document type folder names from CSV. Deduplicates while preserving order.
    With header: match column by name (case-insensitive), or first of known aliases:
    folder, folder_name, name, document_type, document_type_name.
    """
    with path.open(newline="", encoding="utf-8-sig") as f:
        if no_header:
            reader = csv.reader(f)
            out: List[str] = []
            seen: set[str] = set()
            for row in reader:
                if not row:
                    continue
                cell = str(row[0]).strip()
                if not cell or cell.startswith("#"):
                    continue
                if cell not in seen:
                    seen.add(cell)
                    out.append(cell)
            return out

        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row")

        header_to_key = {h.strip().lower(): h for h in reader.fieldnames if h and h.strip()}
        want = column.strip().lower()
        col_key: Optional[str] = header_to_key.get(want)
        if col_key is None:
            for alias in (
                "folder",
                "folder_name",
                "name",
                "document_type",
                "document_type_name",
            ):
                if alias in header_to_key:
                    col_key = header_to_key[alias]
                    break
        if col_key is None:
            raise ValueError(
                f"CSV has no column {column!r}. Columns: {list(reader.fieldnames)}"
            )

        out_list: List[str] = []
        seen_names: set[str] = set()
        for row in reader:
            raw = (row.get(col_key) or "").strip()
            if not raw or raw.startswith("#"):
                continue
            if raw not in seen_names:
                seen_names.add(raw)
                out_list.append(raw)
        return out_list


def apply_field_renames(
    config_obj: Any,
    field_map: Dict[str, str],
    *,
    replace_strings: bool,
) -> Tuple[Any, int, int, int]:
    """Apply each old->new pair: dict keys, then id values, then optional string replace."""
    obj = config_obj
    total_key = 0
    total_id = 0
    total_str = 0
    for old_name, new_name in field_map.items():
        if old_name == new_name:
            continue
        obj, k = rename_key_recursive(obj, old_name, new_name)
        obj, i = rename_id_field_values_recursive(obj, old_name, new_name)
        total_key += k
        total_id += i
        if replace_strings:
            obj, s = replace_strings_recursive(obj, old_name, new_name)
            total_str += s
    return obj, total_key, total_id, total_str


def build_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def list_document_types(
    api_base: str,
    headers: Dict[str, str],
) -> List[Dict[str, Any]]:
    r = requests.get(
        f"{api_base}/document_types",
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("document_types", "data", "items", "results"):
            inner = data.get(key)
            if isinstance(inner, list):
                return inner
    raise ValueError(
        "unexpected /document_types response shape; expected a list or object with a list field"
    )


def document_type_label(t: Dict[str, Any]) -> str:
    for key in ("name", "title", "display_name"):
        v = t.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def resolve_document_type(
    api_base: str,
    headers: Dict[str, str],
    *,
    folder_name: Optional[str],
    document_type_id: Optional[str],
) -> Tuple[str, str]:
    """
    Return (id, human_label) for API calls and logging.
    Exactly one of folder_name or document_type_id must be provided.
    """
    has_id = document_type_id is not None and str(document_type_id).strip() != ""
    has_name = folder_name is not None and str(folder_name).strip() != ""

    if has_id and has_name:
        raise ValueError("pass only one of FOLDER_NAME (positional) and --document-type-id")
    if not has_id and not has_name:
        raise ValueError(
            "pass the folder name as the first argument, or use --document-type-id ID"
        )

    if has_id:
        tid = str(document_type_id).strip()
        return tid, tid

    want = str(folder_name).strip()
    types = list_document_types(api_base, headers)
    matches: List[Dict[str, Any]] = []
    for t in types:
        label = document_type_label(t)
        if label.lower() == want.lower():
            matches.append(t)

    if len(matches) == 1:
        t = matches[0]
        tid = t.get("id")
        if tid is None or str(tid).strip() == "":
            raise ValueError("document type matched but API response had no id")
        return str(tid), document_type_label(t) or want

    if not matches:
        names = sorted({document_type_label(x) for x in types if document_type_label(x)})
        sample = ", ".join(names[:20])
        more = "" if len(names) <= 20 else f" (+{len(names) - 20} more)"
        raise ValueError(
            f"no document type named {want!r}. Known names: {sample}{more}"
        )

    lines = [
        f"  id={m.get('id')} name={document_type_label(m)!r}" for m in matches
    ]
    raise ValueError(
        "multiple document types match that name:\n" + "\n".join(lines)
    )


def list_configs(
    api_base: str,
    headers: Dict[str, str],
    document_type_id: str,
) -> List[Dict[str, Any]]:
    r = requests.get(
        f"{api_base}/document_types/{document_type_id}/configurations",
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def get_config(
    api_base: str,
    headers: Dict[str, str],
    document_type_id: str,
    config_name: str,
) -> Dict[str, Any]:
    r = requests.get(
        f"{api_base}/document_types/{document_type_id}/configurations/{config_name}",
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def draft_version_id_from_get(raw: Dict[str, Any]) -> Optional[str]:
    """
    PUT /configurations/{name} requires current_draft when replacing the in-app draft.
    See PutConfiguration.current_draft in the Sensible API reference.
    """
    for v in raw.get("versions") or []:
        if not isinstance(v, dict):
            continue
        is_draft = v.get("draft") is True or v.get("draft") == "true"
        if is_draft:
            vid = v.get("version_id")
            if vid is not None and str(vid).strip():
                return str(vid)
    return None


def update_config(
    api_base: str,
    headers: Dict[str, str],
    document_type_id: str,
    config_name: str,
    body: Dict[str, Any],
) -> Dict[str, Any]:
    r = requests.put(
        f"{api_base}/document_types/{document_type_id}/configurations/{config_name}",
        headers=headers,
        data=json.dumps(body),
        timeout=60,
    )
    if not r.ok:
        detail = (r.text or "").strip() or r.reason
        raise requests.HTTPError(
            f"{r.status_code} Client Error: {r.reason} for url: {r.url} — {detail}",
            response=r,
        )
    return r.json()


def publish_config(
    api_base: str,
    headers: Dict[str, str],
    document_type_id: str,
    config_name: str,
    version_id: str,
    env: str,
) -> Dict[str, Any]:
    r = requests.put(
        f"{api_base}/document_types/{document_type_id}/configurations/{config_name}/{env}",
        headers=headers,
        data=json.dumps({"version_id": version_id}),
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def rename_key_recursive(value: Any, old_key: str, new_key: str) -> Tuple[Any, int]:
    replacements = 0

    if isinstance(value, dict):
        new_dict: Dict[str, Any] = {}
        for k, v in value.items():
            new_k = new_key if k == old_key else k
            if new_k != k:
                replacements += 1
            new_v, child_count = rename_key_recursive(v, old_key, new_key)
            replacements += child_count
            new_dict[new_k] = new_v
        return new_dict, replacements

    if isinstance(value, list):
        new_list: List[Any] = []
        for item in value:
            new_item, child_count = rename_key_recursive(item, old_key, new_key)
            replacements += child_count
            new_list.append(new_item)
        return new_list, replacements

    return value, 0


def rename_id_field_values_recursive(
    value: Any, old_id_value: str, new_id_value: str
) -> Tuple[Any, int]:
    """
    Sensible field names appear as object properties: {\"id\": \"field_name\", ...}.
    Replace when the key is exactly \"id\" and the value equals old_id_value.
    """
    replacements = 0

    if isinstance(value, dict):
        new_dict: Dict[str, Any] = {}
        for k, v in value.items():
            if k == "id" and v == old_id_value:
                new_dict[k] = new_id_value
                replacements += 1
            else:
                new_v, child_count = rename_id_field_values_recursive(
                    v, old_id_value, new_id_value
                )
                replacements += child_count
                new_dict[k] = new_v
        return new_dict, replacements

    if isinstance(value, list):
        new_list: List[Any] = []
        for item in value:
            new_item, child_count = rename_id_field_values_recursive(
                item, old_id_value, new_id_value
            )
            replacements += child_count
            new_list.append(new_item)
        return new_list, replacements

    return value, 0


def replace_strings_recursive(
    value: Any, old_text: str, new_text: str
) -> Tuple[Any, int]:
    replacements = 0

    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            new_v, c = replace_strings_recursive(v, old_text, new_text)
            replacements += c
            out[k] = new_v
        return out, replacements

    if isinstance(value, list):
        out_list: List[Any] = []
        for item in value:
            new_item, c = replace_strings_recursive(item, old_text, new_text)
            replacements += c
            out_list.append(new_item)
        return out_list, replacements

    if isinstance(value, str):
        new_value = value.replace(old_text, new_text)
        if new_value != value:
            replacements += 1
        return new_value, replacements

    return value, 0


def safe_backup_filename(config_name: str) -> str:
    """Avoid path traversal / odd filesystem names from API config names."""
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in config_name)
    return f"{safe}.json"


def append_audit_line(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Rename field ids and dict keys in Sensible config JSON (same config name).",
        epilog=(
            "Credentials: use a .env file in this directory (or a parent), or export "
            "variables in the shell. Optional flag --env-file PATH (parsed first) loads "
            "a specific file. Variables are not overridden if already set in the environment."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "folder_name",
        nargs="?",
        default=None,
        metavar="FOLDER_NAME",
        help="Single document type folder name (omit if using --folders-csv or --document-type-id).",
    )
    p.add_argument(
        "--folders-csv",
        type=Path,
        default=None,
        metavar="FILE",
        help="CSV of folder names to process in order (one document type per row). "
        "See --folders-csv-column. Not with FOLDER_NAME or --document-type-id.",
    )
    p.add_argument(
        "--folders-csv-column",
        default="folder",
        metavar="NAME",
        help="Header name for the folder column (case-insensitive). "
        "If missing, uses first of: folder, folder_name, name, document_type, document_type_name.",
    )
    p.add_argument(
        "--folders-csv-no-header",
        action="store_true",
        help="CSV has no header; use the first column on every row as the folder name.",
    )
    p.add_argument(
        "--document-type-id",
        default=None,
        metavar="ID",
        help="Use this document type id directly (skip name lookup). Not with --folders-csv.",
    )
    p.add_argument(
        "--api-base",
        default=os.environ.get("SENSIBLE_API_BASE", API_BASE_DEFAULT),
        help="API base URL. Default: env SENSIBLE_API_BASE or sensible.so v0.",
    )
    p.add_argument(
        "--field-map",
        type=Path,
        default=None,
        metavar="FILE",
        help="JSON file {\"old_id\":\"new_id\",...}; overrides the built-in default map.",
    )
    p.add_argument(
        "--old-key",
        default=None,
        metavar="OLD",
        help="With --new-key only: rename a single pair (ignores default map). "
        "Do not combine with --field-map.",
    )
    p.add_argument(
        "--new-key",
        default=None,
        metavar="NEW",
        help="With --old-key only: rename a single pair.",
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Perform API updates. Without this, dry-run only (no writes, no backups).",
    )
    p.add_argument(
        "--publish-env",
        default=None,
        metavar="ENV",
        help='After update, publish version to this environment (e.g. development).',
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N configs (order from list API).",
    )
    p.add_argument(
        "--replace-strings",
        action="store_true",
        help="Also replace each old id with the new id inside string values (e.g. source_id).",
    )
    p.add_argument(
        "--backup-dir",
        type=Path,
        default=None,
        help="Directory for per-config JSON backups of the full GET payload before PUT. "
        "Created if missing. Only used with --execute when an update runs.",
    )
    p.add_argument(
        "--audit-log",
        type=Path,
        default=None,
        help="Append one JSON object per line (JSONL) for each config processed.",
    )
    return p.parse_args(argv)


def process_document_type(
    *,
    api_base: str,
    headers: Dict[str, str],
    doc_id: str,
    doc_label: str,
    field_map: Dict[str, str],
    dry_run: bool,
    replace_strings: bool,
    publish_env: Optional[str],
    limit: Optional[int],
    audit_log: Optional[Path],
    type_backup_root: Optional[Path],
) -> Tuple[int, int, int]:
    """Run migration for every config in one document type. Returns (changed, unchanged, failed)."""
    configs = list_configs(api_base, headers, doc_id)
    if limit is not None:
        configs = configs[:limit]

    print(f"Found {len(configs)} config(s) in document type {doc_label!r} ({doc_id})")

    changed = 0
    unchanged = 0
    failed = 0

    for summary in configs:
        config_name = summary["name"]
        print(f"\n--- Processing: {config_name}")

        record: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "document_type_id": doc_id,
            "document_type_label": doc_label,
            "config_name": config_name,
            "dry_run": dry_run,
            "field_renames": field_map,
            "replace_strings": replace_strings,
            "status": "unknown",
            "key_replacements": 0,
            "id_value_replacements": 0,
            "string_replacements": 0,
            "total_replacements": 0,
            "error": None,
        }

        try:
            raw = get_config(api_base, headers, doc_id, config_name)
            config_str = raw["configuration"]
            config_obj = json.loads(config_str)

            updated_obj, key_replacements, id_value_replacements, string_replacements = (
                apply_field_renames(
                    config_obj,
                    field_map,
                    replace_strings=replace_strings,
                )
            )

            total = key_replacements + id_value_replacements + string_replacements
            record["key_replacements"] = key_replacements
            record["id_value_replacements"] = id_value_replacements
            record["string_replacements"] = string_replacements
            record["total_replacements"] = total

            if total == 0:
                print("No matching keys, field id values, or strings; skipping")
                record["status"] = "unchanged"
                unchanged += 1
                if audit_log:
                    append_audit_line(audit_log, record)
                continue

            parts = [
                f"{key_replacements} dict key replacement(s)",
                f"{id_value_replacements} field id replacement(s)",
            ]
            if replace_strings:
                parts.append(f"{string_replacements} string replacement(s)")
            print(", ".join(parts))

            if dry_run:
                record["status"] = "dry_run_would_change"
                changed += 1
                if audit_log:
                    append_audit_line(audit_log, record)
                continue

            if type_backup_root is not None:
                type_backup_root.mkdir(parents=True, exist_ok=True)
                backup_path = type_backup_root / safe_backup_filename(config_name)
                backup_path.write_text(
                    json.dumps(raw, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                record["backup_path"] = str(backup_path)

            update_body: Dict[str, Any] = {
                "name": raw.get("name", config_name),
                "configuration": json.dumps(updated_obj),
            }
            draft_vid = draft_version_id_from_get(raw)
            if draft_vid:
                update_body["current_draft"] = draft_vid

            update_resp = update_config(
                api_base, headers, doc_id, config_name, update_body
            )
            print("Updated successfully")
            record["status"] = "updated"
            changed += 1

            version_id = update_resp.get("version_id")
            record["version_id"] = version_id
            if publish_env and version_id:
                publish_config(
                    api_base,
                    headers,
                    doc_id,
                    config_name,
                    version_id,
                    publish_env,
                )
                print(f"Published version {version_id} to {publish_env}")
                record["published_env"] = publish_env

        except Exception as e:
            failed += 1
            record["status"] = "failed"
            record["error"] = str(e)
            print(f"FAILED: {config_name}: {e}")

        if audit_log:
            append_audit_line(audit_log, record)

    return changed, unchanged, failed


def main(argv: Optional[List[str]] = None) -> int:
    raw = sys.argv[1:] if argv is None else argv
    env_pre = argparse.ArgumentParser(add_help=False)
    env_pre.add_argument("--env-file", type=Path, default=None)
    env_ns, argv_rest = env_pre.parse_known_args(raw)
    dotenv_path = env_ns.env_file
    load_dotenv(dotenv_path=dotenv_path, override=False)

    args = parse_args(argv_rest)
    dry_run = not args.execute

    api_key = os.environ.get("SENSIBLE_API_KEY")
    if not api_key:
        print(
            "error: set SENSIBLE_API_KEY in the environment or in a .env file",
            file=sys.stderr,
        )
        return 1

    headers = build_headers(api_key)

    if args.field_map is not None:
        if args.old_key is not None or args.new_key is not None:
            print(
                "error: use either --field-map or --old-key/--new-key, not both",
                file=sys.stderr,
            )
            return 1
        try:
            field_map = load_field_map_path(args.field_map)
        except (OSError, json.JSONDecodeError, ValueError) as e:
            print(f"error: --field-map: {e}", file=sys.stderr)
            return 1
    elif args.old_key is not None or args.new_key is not None:
        if args.old_key is None or args.new_key is None:
            print(
                "error: pass both --old-key and --new-key for a single rename",
                file=sys.stderr,
            )
            return 1
        field_map = {args.old_key.strip(): args.new_key.strip()}
    else:
        field_map = dict(DEFAULT_FIELD_RENAMES)

    csv_mode = args.folders_csv is not None
    if csv_mode:
        if args.folder_name:
            print(
                "error: do not pass FOLDER_NAME when using --folders-csv",
                file=sys.stderr,
            )
            return 1
        if args.document_type_id:
            print(
                "error: do not use --document-type-id with --folders-csv",
                file=sys.stderr,
            )
            return 1
        try:
            folder_names = read_folder_names_from_csv(
                args.folders_csv,
                column=args.folders_csv_column,
                no_header=args.folders_csv_no_header,
            )
        except (OSError, ValueError) as e:
            print(f"error: --folders-csv: {e}", file=sys.stderr)
            return 1
        if not folder_names:
            print("error: --folders-csv produced no folder names", file=sys.stderr)
            return 1
        targets: List[Tuple[Optional[str], Optional[str]]] = [
            (name, None) for name in folder_names
        ]
    elif args.document_type_id:
        if args.folder_name:
            print(
                "error: pass only one of FOLDER_NAME and --document-type-id",
                file=sys.stderr,
            )
            return 1
        targets = [(None, args.document_type_id)]
    elif args.folder_name:
        targets = [(args.folder_name, None)]
    else:
        print(
            "error: pass FOLDER_NAME, or --document-type-id, or --folders-csv FILE",
            file=sys.stderr,
        )
        return 1

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_base: Optional[Path] = None
    if args.backup_dir is not None:
        backup_base = args.backup_dir / run_ts

    print(
        f"Field renames ({len(field_map)}): "
        + ", ".join(f"{k!r} -> {v!r}" for k, v in field_map.items())
    )
    if dry_run:
        print("Dry run: no API updates, no backups (--execute to write).")
    if csv_mode:
        print(f"Folders from CSV ({len(targets)}): {', '.join(t[0] or '' for t in targets)}")

    grand_changed = 0
    grand_unchanged = 0
    grand_failed = 0
    folder_resolve_failed = 0

    for idx, (folder_name, doc_id_override) in enumerate(targets):
        try:
            doc_id, doc_label = resolve_document_type(
                args.api_base,
                headers,
                folder_name=folder_name,
                document_type_id=doc_id_override,
            )
        except ValueError as e:
            label = folder_name or doc_id_override or "?"
            print(f"\nerror resolving document type {label!r}: {e}", file=sys.stderr)
            folder_resolve_failed += 1
            grand_failed += 1
            continue

        if len(targets) > 1:
            print(f"\n{'=' * 60}")
            print(f"Folder {idx + 1}/{len(targets)}: {doc_label!r} ({doc_id})")
            print(f"{'=' * 60}")

        type_backup_root: Optional[Path] = None
        if backup_base is not None:
            type_backup_root = backup_base / doc_id

        ch, un, fa = process_document_type(
            api_base=args.api_base,
            headers=headers,
            doc_id=doc_id,
            doc_label=doc_label,
            field_map=field_map,
            dry_run=dry_run,
            replace_strings=args.replace_strings,
            publish_env=args.publish_env,
            limit=args.limit,
            audit_log=args.audit_log,
            type_backup_root=type_backup_root,
        )
        grand_changed += ch
        grand_unchanged += un
        grand_failed += fa

        if len(targets) > 1:
            print(
                f"\nFolder subtotal — changed: {ch}, unchanged: {un}, failed: {fa}"
            )

    print("\n=== SUMMARY ===")
    if len(targets) > 1:
        print(f"Document types in run:   {len(targets)}")
        if folder_resolve_failed:
            print(f"Resolve failures:        {folder_resolve_failed}")
    print(f"Changed (or would change): {grand_changed}")
    print(f"Unchanged:                 {grand_unchanged}")
    print(f"Failed:                    {grand_failed}")
    if backup_base and not dry_run:
        print(f"Backups under: {backup_base}")

    return 0 if grand_failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
