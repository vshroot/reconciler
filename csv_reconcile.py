#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Optional


@dataclass(frozen=True)
class ColumnRef:
    """
    Column reference by name (CSV header) or by index.

    index is 0-based by default. If index_base is set to 1, index is treated as 1-based.
    """

    name: Optional[str] = None
    index: Optional[int] = None
    index_base: int = 0


@dataclass(frozen=True)
class ColumnSpec:
    id_col: ColumnRef
    amount_col: ColumnRef
    status_col: ColumnRef
    keep_cols: tuple[str, ...]


@dataclass(frozen=True)
class ImportSpec:
    path: Path
    delimiter: str
    encoding: str
    decimal_comma: bool
    amount_scale: int
    columns: ColumnSpec
    name: str = "file"


@dataclass(frozen=True)
class ConfigSpec:
    out_dir: Path
    amount_scale: int
    amount_tolerance: int
    files: tuple[ImportSpec, ...]
    primary: str


def _sanitize_sql_ident(name: str) -> str:
    # SQLite identifiers in CREATE TABLE can't be safely parameterized.
    # We keep it conservative and stable.
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", name.strip())
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "col"
    if s[0].isdigit():
        return f"c_{s}"
    return s


def _parse_amount_scaled(raw: str, *, scale: int, decimal_comma: bool) -> Optional[int]:
    s = (raw or "").strip()
    if not s:
        return None

    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    s = s.replace("\u00a0", "").replace(" ", "")

    if decimal_comma:
        # Treat comma as decimal separator; dots are likely thousands separators.
        s = s.replace(".", "").replace(",", ".")
    else:
        if "," in s and "." in s:
            # Assume comma is thousands separator.
            s = s.replace(",", "")
        elif "," in s and "." not in s:
            # Assume comma is decimal separator.
            s = s.replace(",", ".")

    # Drop currency symbols and any other junk.
    s = re.sub(r"[^0-9.\-+]", "", s)
    if s in {"", "-", "+", ".", "-.", "+."}:
        return None

    try:
        d = Decimal(s)
    except InvalidOperation:
        return None

    if neg:
        d = -d

    factor = Decimal(10) ** scale
    scaled = (d * factor).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    try:
        return int(scaled)
    except (ValueError, OverflowError):
        return None


def _status_norm(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None
    return s.casefold()


def _read_header(path: Path, *, delimiter: str, encoding: str) -> list[str]:
    with path.open("r", newline="", encoding=encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=delimiter)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"CSV is empty: {path}")
    # Strip UTF-8 BOM if present.
    if header and header[0].startswith("\ufeff"):
        header[0] = header[0].lstrip("\ufeff")
    return [h.strip() for h in header]


def _resolve_index(header: list[str], ref: ColumnRef, *, what: str) -> int:
    if ref.index is not None:
        if ref.index_base not in (0, 1):
            raise ValueError(f"Invalid index_base for {what}: {ref.index_base} (must be 0 or 1)")
        idx = int(ref.index) - (1 if ref.index_base == 1 else 0)
        if idx < 0:
            raise ValueError(f"Invalid {what} index: {ref.index} (base {ref.index_base} -> {idx})")
        return idx

    name = (ref.name or "").strip()
    if not name:
        raise ValueError(f"Missing {what} column reference (need name or index)")

    name_to_idx = {h: i for i, h in enumerate(header)}
    if name in name_to_idx:
        return name_to_idx[name]

    # Friendly fallback: case-insensitive match if exact header differs by case/whitespace.
    norm_to_idx: dict[str, int] = {h.strip().casefold(): i for i, h in enumerate(header)}
    key = name.strip().casefold()
    if key in norm_to_idx:
        return norm_to_idx[key]

    raise ValueError(f"Column '{name}' for {what} not found in CSV header. Available: {header}")


def _resolve_indices(header: list[str], columns: ColumnSpec) -> tuple[int, int, int, list[int]]:
    id_i = _resolve_index(header, columns.id_col, what="transaction_id")
    amount_i = _resolve_index(header, columns.amount_col, what="amount")
    status_i = _resolve_index(header, columns.status_col, what="status")

    keep_is: list[int] = []
    for kc in columns.keep_cols:
        if not kc:
            continue
        # keep-cols are name-based (CSV header).
        name_to_idx = {name: i for i, name in enumerate(header)}
        if kc not in name_to_idx:
            # Try case-insensitive.
            norm_to_idx: dict[str, int] = {h.strip().casefold(): i for i, h in enumerate(header)}
            k_norm = kc.strip().casefold()
            if k_norm in norm_to_idx:
                keep_is.append(norm_to_idx[k_norm])
                continue
            raise ValueError(f"Keep column '{kc}' not found in CSV header. Available: {header}")
        keep_is.append(name_to_idx[kc])

    return id_i, amount_i, status_i, keep_is


def _connect_sqlite(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    # Pragmas tuned for bulk load / analytics. This trades durability for speed.
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=OFF;")
    con.execute("PRAGMA temp_store=MEMORY;")
    con.execute("PRAGMA foreign_keys=OFF;")
    con.execute("PRAGMA cache_size=-200000;")  # ~200MB
    return con


def _create_table(
    con: sqlite3.Connection,
    table: str,
    *,
    keep_cols: Iterable[str],
) -> dict[str, str]:
    keep_map: dict[str, str] = {}
    used: set[str] = set()
    for kc in keep_cols:
        base = f"keep__{_sanitize_sql_ident(kc)}"
        col = base
        n = 2
        while col in used:
            col = f"{base}_{n}"
            n += 1
        used.add(col)
        keep_map[kc] = col

    cols_sql = [
        "txid TEXT NOT NULL",
        "amount_raw TEXT",
        "amount_scaled INTEGER",
        "status_raw TEXT",
        "status_norm TEXT",
        "rownum INTEGER NOT NULL",
    ]
    for _, safe_col in keep_map.items():
        cols_sql.append(f"{safe_col} TEXT")

    con.execute(f'DROP TABLE IF EXISTS "{table}";')
    con.execute(f'CREATE TABLE "{table}" ({", ".join(cols_sql)});')
    con.execute(f'CREATE INDEX "{table}__txid" ON "{table}" (txid);')
    return keep_map


def _import_csv_into_table(
    con: sqlite3.Connection,
    table: str,
    spec: ImportSpec,
) -> dict[str, object]:
    header = _read_header(spec.path, delimiter=spec.delimiter, encoding=spec.encoding)
    id_i, amount_i, status_i, keep_is = _resolve_indices(header, spec.columns)

    keep_map = _create_table(con, table, keep_cols=spec.columns.keep_cols)
    keep_safe_cols = [keep_map[kc] for kc in spec.columns.keep_cols]

    insert_cols = ["txid", "amount_raw", "amount_scaled", "status_raw", "status_norm", "rownum"] + keep_safe_cols
    placeholders = ", ".join(["?"] * len(insert_cols))
    insert_sql = f'INSERT INTO "{table}" ({", ".join(insert_cols)}) VALUES ({placeholders});'

    rows_total = 0
    rows_bad_id = 0
    rows_bad_amount = 0

    batch: list[tuple[object, ...]] = []
    batch_size = 5000

    with spec.path.open("r", newline="", encoding=spec.encoding, errors="replace") as f:
        reader = csv.reader(f, delimiter=spec.delimiter)
        # Skip header.
        try:
            next(reader)
        except StopIteration:
            pass

        con.execute("BEGIN;")
        for rownum, row in enumerate(reader, start=2):
            rows_total += 1
            if id_i >= len(row):
                rows_bad_id += 1
                continue
            txid = (row[id_i] or "").strip()
            if not txid:
                rows_bad_id += 1
                continue

            amount_raw = row[amount_i] if amount_i < len(row) else ""
            amount_scaled = _parse_amount_scaled(
                amount_raw,
                scale=spec.amount_scale,
                decimal_comma=spec.decimal_comma,
            )
            if amount_raw and amount_scaled is None:
                rows_bad_amount += 1

            status_raw = row[status_i] if status_i < len(row) else ""
            status_norm = _status_norm(status_raw)

            keep_vals: list[str] = []
            for ki in keep_is:
                keep_vals.append(row[ki] if ki < len(row) else "")

            batch.append(
                (
                    txid,
                    amount_raw,
                    amount_scaled,
                    status_raw,
                    status_norm,
                    rownum,
                    *keep_vals,
                )
            )

            if len(batch) >= batch_size:
                con.executemany(insert_sql, batch)
                batch.clear()

        if batch:
            con.executemany(insert_sql, batch)
            batch.clear()
        con.execute("COMMIT;")

    con.execute(f'ANALYZE "{table}";')

    return {
        "path": str(spec.path),
        "header": header,
        "rows_total": rows_total,
        "rows_bad_id": rows_bad_id,
        "rows_bad_amount": rows_bad_amount,
        "keep_map": keep_map,  # original -> safe
    }


def _export_query_csv(con: sqlite3.Connection, sql: str, params: tuple[object, ...], out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        cur = con.execute(sql, params)
        writer.writerow([d[0] for d in cur.description])
        for row in cur:
            writer.writerow([("" if v is None else v) for v in row])
            n += 1
    return n


def _export_status_totals(con: sqlite3.Connection, table: str, out_path: Path) -> int:
    sql = f"""
    WITH dups AS (
      SELECT txid FROM "{table}" GROUP BY txid HAVING COUNT(*) > 1
    ),
    u AS (
      SELECT * FROM "{table}" t WHERE NOT EXISTS (SELECT 1 FROM dups d WHERE d.txid = t.txid)
    )
    SELECT
      COALESCE(status_norm, '') AS status_norm,
      COUNT(*) AS tx_count,
      SUM(amount_scaled) AS amount_scaled_sum
    FROM u
    GROUP BY COALESCE(status_norm, '')
    ORDER BY status_norm;
    """
    return _export_query_csv(con, sql, (), out_path)


def reconcile(
    *,
    left: ImportSpec,
    right: ImportSpec,
    out_dir: Path,
    amount_tolerance: int,
) -> dict[str, object]:
    out_dir.mkdir(parents=True, exist_ok=True)
    db_path = out_dir / "reconcile.sqlite"

    started = time.time()
    con = _connect_sqlite(db_path)
    try:
        left_meta = _import_csv_into_table(con, "left_tx", left)
        right_meta = _import_csv_into_table(con, "right_tx", right)

        # Optional extra columns to show in reports.
        keep_cols = list(left.columns.keep_cols)
        keep_alias: dict[str, str] = {}
        used_aliases: set[str] = set()
        for kc in keep_cols:
            base = _sanitize_sql_ident(kc)
            alias = base
            n = 2
            while alias in used_aliases:
                alias = f"{base}_{n}"
                n += 1
            used_aliases.add(alias)
            keep_alias[kc] = alias

        left_keep_map: dict[str, str] = left_meta.get("keep_map", {})  # original -> safe
        right_keep_map: dict[str, str] = right_meta.get("keep_map", {})  # original -> safe

        keep_in_join_sql_parts: list[str] = []
        keep_in_final_sql_parts: list[str] = []
        for kc in keep_cols:
            lcol = left_keep_map.get(kc)
            rcol = right_keep_map.get(kc)
            if not lcol or not rcol:
                continue
            a = keep_alias[kc]
            keep_in_join_sql_parts.append(f'l."{lcol}" AS "left__{a}"')
            keep_in_join_sql_parts.append(f'r."{rcol}" AS "right__{a}"')
            keep_in_final_sql_parts.append(f'"left__{a}"')
            keep_in_final_sql_parts.append(f'"right__{a}"')

        keep_in_join_sql = ""
        if keep_in_join_sql_parts:
            keep_in_join_sql = "\n                " + ",\n                ".join(keep_in_join_sql_parts) + ","
        keep_in_final_sql = ""
        if keep_in_final_sql_parts:
            keep_in_final_sql = ",\n              " + ",\n              ".join(keep_in_final_sql_parts)

        # Duplicate txids (we exclude them from 1:1 comparisons).
        con.execute('DROP TABLE IF EXISTS "left_dups";')
        con.execute('DROP TABLE IF EXISTS "right_dups";')
        con.execute('CREATE TABLE "left_dups" AS SELECT txid FROM "left_tx" GROUP BY txid HAVING COUNT(*) > 1;')
        con.execute('CREATE TABLE "right_dups" AS SELECT txid FROM "right_tx" GROUP BY txid HAVING COUNT(*) > 1;')
        con.execute('CREATE INDEX "left_dups__txid" ON "left_dups" (txid);')
        con.execute('CREATE INDEX "right_dups__txid" ON "right_dups" (txid);')

        # Export duplicates (rows, not just ids).
        dups_left_rows = _export_query_csv(
            con,
            """
            SELECT t.*
            FROM left_tx t
            WHERE EXISTS (SELECT 1 FROM left_dups d WHERE d.txid = t.txid)
            ORDER BY t.txid, t.rownum;
            """,
            (),
            out_dir / "duplicates_left.csv",
        )
        dups_right_rows = _export_query_csv(
            con,
            """
            SELECT t.*
            FROM right_tx t
            WHERE EXISTS (SELECT 1 FROM right_dups d WHERE d.txid = t.txid)
            ORDER BY t.txid, t.rownum;
            """,
            (),
            out_dir / "duplicates_right.csv",
        )

        # Missing rows (only among unique txids).
        missing_in_left = _export_query_csv(
            con,
            """
            WITH r AS (
              SELECT * FROM right_tx t
              WHERE NOT EXISTS (SELECT 1 FROM right_dups d WHERE d.txid = t.txid)
            ),
            l AS (
              SELECT txid FROM left_tx t
              WHERE NOT EXISTS (SELECT 1 FROM left_dups d WHERE d.txid = t.txid)
            )
            SELECT r.*
            FROM r
            WHERE NOT EXISTS (SELECT 1 FROM l WHERE l.txid = r.txid)
            ORDER BY r.txid;
            """,
            (),
            out_dir / "missing_in_left.csv",
        )
        missing_in_right = _export_query_csv(
            con,
            """
            WITH l AS (
              SELECT * FROM left_tx t
              WHERE NOT EXISTS (SELECT 1 FROM left_dups d WHERE d.txid = t.txid)
            ),
            r AS (
              SELECT txid FROM right_tx t
              WHERE NOT EXISTS (SELECT 1 FROM right_dups d WHERE d.txid = t.txid)
            )
            SELECT l.*
            FROM l
            WHERE NOT EXISTS (SELECT 1 FROM r WHERE r.txid = l.txid)
            ORDER BY l.txid;
            """,
            (),
            out_dir / "missing_in_right.csv",
        )

        # Mismatches (only among unique txids on both sides).
        mismatches = _export_query_csv(
            con,
            f"""
            WITH l AS (
              SELECT * FROM left_tx t
              WHERE NOT EXISTS (SELECT 1 FROM left_dups d WHERE d.txid = t.txid)
            ),
            r AS (
              SELECT * FROM right_tx t
              WHERE NOT EXISTS (SELECT 1 FROM right_dups d WHERE d.txid = t.txid)
            ),
            j AS (
              SELECT
                l.txid AS txid,
                l.amount_raw AS left_amount_raw,
                l.amount_scaled AS left_amount_scaled,
                l.status_raw AS left_status_raw,
                l.status_norm AS left_status_norm,
                l.rownum AS left_rownum,
                r.amount_raw AS right_amount_raw,
                r.amount_scaled AS right_amount_scaled,
                r.status_raw AS right_status_raw,
                r.status_norm AS right_status_norm,
                r.rownum AS right_rownum,
                {keep_in_join_sql}
                CASE WHEN l.amount_scaled IS NULL OR r.amount_scaled IS NULL THEN 1 ELSE 0 END AS amount_parse_error,
                CASE
                  WHEN l.amount_scaled IS NULL OR r.amount_scaled IS NULL THEN 0
                  WHEN ABS(l.amount_scaled - r.amount_scaled) > ? THEN 1
                  ELSE 0
                END AS amount_mismatch,
                CASE
                  WHEN COALESCE(l.status_norm, '') <> COALESCE(r.status_norm, '') THEN 1
                  ELSE 0
                END AS status_mismatch
              FROM l
              JOIN r ON r.txid = l.txid
            )
            SELECT
              txid,
              CASE
                WHEN amount_parse_error = 1 THEN 'amount_parse_error'
                WHEN amount_mismatch = 1 AND status_mismatch = 1 THEN 'amount_and_status_mismatch'
                WHEN amount_mismatch = 1 THEN 'amount_mismatch'
                WHEN status_mismatch = 1 THEN 'status_mismatch'
                ELSE NULL
              END AS mismatch_type,
              left_amount_raw,
              left_amount_scaled,
              right_amount_raw,
              right_amount_scaled,
              (right_amount_scaled - left_amount_scaled) AS amount_diff_scaled,
              left_status_raw,
              left_status_norm,
              right_status_raw,
              right_status_norm,
              left_rownum,
              right_rownum
              {keep_in_final_sql}
            FROM j
            WHERE mismatch_type IS NOT NULL
            ORDER BY txid;
            """,
            (amount_tolerance,),
            out_dir / "mismatches.csv",
        )

        # Status totals (unique txids only).
        status_left = _export_status_totals(con, "left_tx", out_dir / "status_totals_left.csv")
        status_right = _export_status_totals(con, "right_tx", out_dir / "status_totals_right.csv")

        elapsed_s = round(time.time() - started, 3)

        # Summary counts
        def q1(sql: str) -> int:
            return int(con.execute(sql).fetchone()[0])

        summary = {
            "elapsed_seconds": elapsed_s,
            "out_dir": str(out_dir),
            "db_path": str(db_path),
            "left": left_meta,
            "right": right_meta,
            "counts": {
                "left_rows_in_db": q1('SELECT COUNT(*) FROM "left_tx";'),
                "right_rows_in_db": q1('SELECT COUNT(*) FROM "right_tx";'),
                "left_duplicate_txids": q1('SELECT COUNT(*) FROM "left_dups";'),
                "right_duplicate_txids": q1('SELECT COUNT(*) FROM "right_dups";'),
                "left_duplicate_rows_exported": dups_left_rows,
                "right_duplicate_rows_exported": dups_right_rows,
                "missing_in_left": missing_in_left,
                "missing_in_right": missing_in_right,
                "mismatches": mismatches,
                "status_totals_left_rows": status_left,
                "status_totals_right_rows": status_right,
            },
            "settings": {
                "columns": {
                    "id": {
                        "name": left.columns.id_col.name,
                        "index": left.columns.id_col.index,
                        "index_base": left.columns.id_col.index_base,
                    },
                    "amount": {
                        "name": left.columns.amount_col.name,
                        "index": left.columns.amount_col.index,
                        "index_base": left.columns.amount_col.index_base,
                    },
                    "status": {
                        "name": left.columns.status_col.name,
                        "index": left.columns.status_col.index,
                        "index_base": left.columns.status_col.index_base,
                    },
                },
                "keep_cols": list(left.columns.keep_cols),
                "amount_scale": left.amount_scale,
                "amount_tolerance_scaled": amount_tolerance,
                "decimal_comma": left.decimal_comma,
                "delimiter": left.delimiter,
                "encoding": left.encoding,
            },
        }

        (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary
    finally:
        con.close()


def _parse_column_ref(value: Any, *, index_base: int) -> ColumnRef:
    if isinstance(value, ColumnRef):
        return value
    if isinstance(value, int):
        return ColumnRef(index=value, index_base=index_base)
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            return ColumnRef(index=int(s), index_base=index_base)
        return ColumnRef(name=s)
    if isinstance(value, dict):
        name = value.get("name")
        idx = value.get("index")
        base = value.get("index_base", index_base)
        return ColumnRef(
            name=(str(name).strip() if name is not None else None),
            index=(int(idx) if idx is not None else None),
            index_base=int(base),
        )
    raise ValueError(f"Unsupported column reference: {value!r}")


def _parse_config(path: Path) -> ConfigSpec:
    raw = json.loads(path.read_text(encoding="utf-8"))
    out_dir = Path(str(raw.get("out_dir") or raw.get("out") or "./out")).expanduser()
    amount_scale = int(raw.get("amount_scale", 2))
    amount_tolerance = int(raw.get("amount_tolerance", 0))
    primary = str(raw.get("primary") or "").strip()
    if not primary:
        primary = ""

    files_raw = raw.get("files")
    if not isinstance(files_raw, list) or not files_raw:
        raise ValueError("Config must contain non-empty 'files' array")

    specs: list[ImportSpec] = []
    for i, fr in enumerate(files_raw):
        if not isinstance(fr, dict):
            raise ValueError(f"files[{i}] must be an object")

        name = str(fr.get("name") or fr.get("label") or f"file{i+1}").strip()
        if not name:
            raise ValueError(f"files[{i}].name must be non-empty")

        index_base = int(fr.get("index_base", raw.get("index_base", 0)))

        cols = fr.get("columns") or {}
        if not isinstance(cols, dict):
            raise ValueError(f"files[{i}].columns must be an object")
        id_ref = _parse_column_ref(cols.get("id") or cols.get("transaction_id") or cols.get("txid"), index_base=index_base)
        amount_ref = _parse_column_ref(cols.get("amount"), index_base=index_base)
        status_ref = _parse_column_ref(cols.get("status"), index_base=index_base)

        keep_cols = fr.get("keep_cols") or fr.get("keepCols") or []
        if isinstance(keep_cols, str):
            keep_cols = [x.strip() for x in keep_cols.split(",") if x.strip()]
        if not isinstance(keep_cols, list):
            raise ValueError(f"files[{i}].keep_cols must be a list or a comma-separated string")

        columns = ColumnSpec(
            id_col=id_ref,
            amount_col=amount_ref,
            status_col=status_ref,
            keep_cols=tuple([str(x).strip() for x in keep_cols if str(x).strip()]),
        )

        spec = ImportSpec(
            name=name,
            path=Path(str(fr.get("path"))).expanduser(),
            delimiter=str(fr.get("delimiter", raw.get("delimiter", ","))),
            encoding=str(fr.get("encoding", raw.get("encoding", "utf-8"))),
            decimal_comma=bool(fr.get("decimal_comma", raw.get("decimal_comma", False))),
            amount_scale=int(fr.get("amount_scale", raw.get("amount_scale", amount_scale))),
            columns=columns,
        )
        specs.append(spec)

    if not primary:
        primary = specs[0].name

    names = [s.name for s in specs]
    if len(set(names)) != len(names):
        raise ValueError(f"File names must be unique. Got: {names}")
    if primary not in set(names):
        raise ValueError(f"primary must match one of file names. primary={primary!r}, files={names}")

    if amount_scale < 0 or amount_scale > 18:
        raise ValueError("amount_scale must be between 0 and 18")
    if amount_tolerance < 0:
        raise ValueError("amount_tolerance must be >= 0")

    return ConfigSpec(
        out_dir=out_dir,
        amount_scale=amount_scale,
        amount_tolerance=amount_tolerance,
        files=tuple(specs),
        primary=primary,
    )


def _export_pair_reports(
    con: sqlite3.Connection,
    *,
    base_table: str,
    other_table: str,
    out_dir: Path,
    amount_tolerance: int,
    base_keep_map: dict[str, str],
    other_keep_map: dict[str, str],
    keep_cols: tuple[str, ...],
) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)

    def export(sql: str, params: tuple[object, ...], name: str) -> int:
        return _export_query_csv(con, sql, params, out_dir / name)

    # Duplicates for both sides.
    dups_base_rows = export(
        f"""
        SELECT t.*
        FROM "{base_table}" t
        WHERE EXISTS (SELECT 1 FROM "{base_table}_dups" d WHERE d.txid = t.txid)
        ORDER BY t.txid, t.rownum;
        """,
        (),
        "duplicates_base.csv",
    )
    dups_other_rows = export(
        f"""
        SELECT t.*
        FROM "{other_table}" t
        WHERE EXISTS (SELECT 1 FROM "{other_table}_dups" d WHERE d.txid = t.txid)
        ORDER BY t.txid, t.rownum;
        """,
        (),
        "duplicates_other.csv",
    )

    # Optional extra columns to show in reports (only if provided in BOTH files).
    keep_alias: dict[str, str] = {}
    used_aliases: set[str] = set()
    for kc in keep_cols:
        base = _sanitize_sql_ident(kc)
        alias = base
        n = 2
        while alias in used_aliases:
            alias = f"{base}_{n}"
            n += 1
        used_aliases.add(alias)
        keep_alias[kc] = alias

    keep_in_join_sql_parts: list[str] = []
    keep_in_final_sql_parts: list[str] = []
    for kc in keep_cols:
        lcol = base_keep_map.get(kc)
        rcol = other_keep_map.get(kc)
        if not lcol or not rcol:
            continue
        a = keep_alias[kc]
        keep_in_join_sql_parts.append(f'l."{lcol}" AS "base__{a}"')
        keep_in_join_sql_parts.append(f'r."{rcol}" AS "other__{a}"')
        keep_in_final_sql_parts.append(f'"base__{a}"')
        keep_in_final_sql_parts.append(f'"other__{a}"')

    keep_in_join_sql = ""
    if keep_in_join_sql_parts:
        keep_in_join_sql = "\n            " + ",\n            ".join(keep_in_join_sql_parts) + ","
    keep_in_final_sql = ""
    if keep_in_final_sql_parts:
        keep_in_final_sql = ",\n          " + ",\n          ".join(keep_in_final_sql_parts)

    missing_in_base = export(
        f"""
        WITH o AS (
          SELECT * FROM "{other_table}" t
          WHERE NOT EXISTS (SELECT 1 FROM "{other_table}_dups" d WHERE d.txid = t.txid)
        ),
        b AS (
          SELECT txid FROM "{base_table}" t
          WHERE NOT EXISTS (SELECT 1 FROM "{base_table}_dups" d WHERE d.txid = t.txid)
        )
        SELECT o.*
        FROM o
        WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.txid = o.txid)
        ORDER BY o.txid;
        """,
        (),
        "missing_in_base.csv",
    )

    missing_in_other = export(
        f"""
        WITH b AS (
          SELECT * FROM "{base_table}" t
          WHERE NOT EXISTS (SELECT 1 FROM "{base_table}_dups" d WHERE d.txid = t.txid)
        ),
        o AS (
          SELECT txid FROM "{other_table}" t
          WHERE NOT EXISTS (SELECT 1 FROM "{other_table}_dups" d WHERE d.txid = t.txid)
        )
        SELECT b.*
        FROM b
        WHERE NOT EXISTS (SELECT 1 FROM o WHERE o.txid = b.txid)
        ORDER BY b.txid;
        """,
        (),
        "missing_in_other.csv",
    )

    mismatches = export(
        f"""
        WITH b AS (
          SELECT * FROM "{base_table}" t
          WHERE NOT EXISTS (SELECT 1 FROM "{base_table}_dups" d WHERE d.txid = t.txid)
        ),
        o AS (
          SELECT * FROM "{other_table}" t
          WHERE NOT EXISTS (SELECT 1 FROM "{other_table}_dups" d WHERE d.txid = t.txid)
        ),
        j AS (
          SELECT
            b.txid AS txid,
            b.amount_raw AS base_amount_raw,
            b.amount_scaled AS base_amount_scaled,
            b.status_raw AS base_status_raw,
            b.status_norm AS base_status_norm,
            b.rownum AS base_rownum,
            o.amount_raw AS other_amount_raw,
            o.amount_scaled AS other_amount_scaled,
            o.status_raw AS other_status_raw,
            o.status_norm AS other_status_norm,
            o.rownum AS other_rownum,
            {keep_in_join_sql}
            CASE WHEN b.amount_scaled IS NULL OR o.amount_scaled IS NULL THEN 1 ELSE 0 END AS amount_parse_error,
            CASE
              WHEN b.amount_scaled IS NULL OR o.amount_scaled IS NULL THEN 0
              WHEN ABS(b.amount_scaled - o.amount_scaled) > ? THEN 1
              ELSE 0
            END AS amount_mismatch,
            CASE
              WHEN COALESCE(b.status_norm, '') <> COALESCE(o.status_norm, '') THEN 1
              ELSE 0
            END AS status_mismatch
          FROM b
          JOIN o ON o.txid = b.txid
        )
        SELECT
          txid,
          CASE
            WHEN amount_parse_error = 1 THEN 'amount_parse_error'
            WHEN amount_mismatch = 1 AND status_mismatch = 1 THEN 'amount_and_status_mismatch'
            WHEN amount_mismatch = 1 THEN 'amount_mismatch'
            WHEN status_mismatch = 1 THEN 'status_mismatch'
            ELSE NULL
          END AS mismatch_type,
          base_amount_raw,
          base_amount_scaled,
          other_amount_raw,
          other_amount_scaled,
          (other_amount_scaled - base_amount_scaled) AS amount_diff_scaled,
          base_status_raw,
          base_status_norm,
          other_status_raw,
          other_status_norm,
          base_rownum,
          other_rownum
          {keep_in_final_sql}
        FROM j
        WHERE mismatch_type IS NOT NULL
        ORDER BY txid;
        """,
        (amount_tolerance,),
        "mismatches.csv",
    )

    return {
        "duplicates_base_rows": dups_base_rows,
        "duplicates_other_rows": dups_other_rows,
        "missing_in_base": missing_in_base,
        "missing_in_other": missing_in_other,
        "mismatches": mismatches,
    }


def reconcile_many(*, cfg: ConfigSpec) -> dict[str, object]:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    db_path = cfg.out_dir / "reconcile.sqlite"

    started = time.time()
    con = _connect_sqlite(db_path)
    try:
        table_by_name: dict[str, str] = {}
        meta_by_name: dict[str, dict[str, object]] = {}

        for spec in cfg.files:
            if not spec.path.exists():
                raise ValueError(f"File not found: {spec.path}")
            t = f"t__{_sanitize_sql_ident(spec.name)}"
            table_by_name[spec.name] = t
            meta_by_name[spec.name] = _import_csv_into_table(con, t, spec)

            # dups table per file
            con.execute(f'DROP TABLE IF EXISTS "{t}_dups";')
            con.execute(f'CREATE TABLE "{t}_dups" AS SELECT txid FROM "{t}" GROUP BY txid HAVING COUNT(*) > 1;')
            con.execute(f'CREATE INDEX "{t}_dups__txid" ON "{t}_dups" (txid);')

            _export_status_totals(con, t, cfg.out_dir / f"status_totals__{spec.name}.csv")

        base_name = cfg.primary
        base_table = table_by_name[base_name]
        base_keep_map: dict[str, str] = meta_by_name[base_name].get("keep_map", {})  # original -> safe
        base_spec = next(s for s in cfg.files if s.name == base_name)

        pairs: dict[str, dict[str, int]] = {}
        for spec in cfg.files:
            if spec.name == base_name:
                continue
            other_name = spec.name
            other_table = table_by_name[other_name]
            other_keep_map: dict[str, str] = meta_by_name[other_name].get("keep_map", {})
            out_pair_dir = cfg.out_dir / f"{base_name}__vs__{other_name}"
            keep_cols_union = tuple(dict.fromkeys(base_spec.columns.keep_cols + spec.columns.keep_cols))

            pairs[other_name] = _export_pair_reports(
                con,
                base_table=base_table,
                other_table=other_table,
                out_dir=out_pair_dir,
                amount_tolerance=cfg.amount_tolerance,
                base_keep_map=base_keep_map,
                other_keep_map=other_keep_map,
                keep_cols=keep_cols_union,
            )

        elapsed_s = round(time.time() - started, 3)
        summary = {
            "elapsed_seconds": elapsed_s,
            "out_dir": str(cfg.out_dir),
            "db_path": str(db_path),
            "primary": base_name,
            "files": {name: meta for name, meta in meta_by_name.items()},
            "pairs": pairs,
            "settings": {
                "amount_scale": cfg.amount_scale,
                "amount_tolerance_scaled": cfg.amount_tolerance,
            },
        }

        (cfg.out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        return summary
    finally:
        con.close()


def _parse_keep_cols(value: str) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple([x.strip() for x in value.split(",") if x.strip()])


def main() -> int:
    p = argparse.ArgumentParser(description="Reconcile two CSV transaction files by id/amount/status.")
    p.add_argument("--config", default="", help="Path to JSON config for multi-file reconcile")
    p.add_argument("--left", default="", help="Path to left CSV (two-file mode)")
    p.add_argument("--right", default="", help="Path to right CSV (two-file mode)")
    p.add_argument("--out", default="", help="Output directory (two-file mode)")

    p.add_argument("--id-col", default="transaction_id", help="Transaction id column name")
    p.add_argument("--amount-col", default="amount", help="Amount column name")
    p.add_argument("--status-col", default="status", help="Status column name")
    p.add_argument("--keep-cols", default="", help="Extra columns to keep (comma-separated)")

    p.add_argument("--delimiter", default=",", help="CSV delimiter (default: ,)")
    p.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8)")
    p.add_argument("--decimal-comma", action="store_true", help="Treat comma as decimal separator")

    p.add_argument("--amount-scale", type=int, default=2, help="Decimal places for amount scaling (default: 2)")
    p.add_argument(
        "--amount-tolerance",
        type=int,
        default=0,
        help="Allowed diff in scaled units (e.g. scale=2 -> 1 means 0.01). Default: 0",
    )

    args = p.parse_args()

    if args.config:
        cfg_path = Path(args.config).expanduser()
        if not cfg_path.exists():
            raise SystemExit(f"Config file not found: {cfg_path}")
        cfg = _parse_config(cfg_path)
        reconcile_many(cfg=cfg)
        return 0

    if not args.left or not args.right:
        raise SystemExit("Two-file mode requires --left and --right (or use --config)")

    if len(args.delimiter) != 1:
        raise SystemExit("--delimiter must be a single character")
    if args.amount_scale < 0 or args.amount_scale > 18:
        raise SystemExit("--amount-scale must be between 0 and 18")
    if args.amount_tolerance < 0:
        raise SystemExit("--amount-tolerance must be >= 0")

    columns = ColumnSpec(
        id_col=ColumnRef(name=args.id_col),
        amount_col=ColumnRef(name=args.amount_col),
        status_col=ColumnRef(name=args.status_col),
        keep_cols=_parse_keep_cols(args.keep_cols),
    )

    left = ImportSpec(
        path=Path(args.left).expanduser(),
        delimiter=args.delimiter,
        encoding=args.encoding,
        decimal_comma=bool(args.decimal_comma),
        amount_scale=args.amount_scale,
        columns=columns,
        name="left",
    )
    right = ImportSpec(
        path=Path(args.right).expanduser(),
        delimiter=args.delimiter,
        encoding=args.encoding,
        decimal_comma=bool(args.decimal_comma),
        amount_scale=args.amount_scale,
        columns=columns,
        name="right",
    )

    for s in (left, right):
        if not s.path.exists():
            raise SystemExit(f"File not found: {s.path}")

    out_dir = Path(args.out).expanduser() if args.out else (Path(os.getcwd()) / "out")
    # Avoid accidental writes to weird locations like "/" when users pass empty values.
    if str(out_dir).strip() in {"", ".", ".."}:
        out_dir = Path(os.getcwd()) / "out"

    reconcile(
        left=left,
        right=right,
        out_dir=out_dir,
        amount_tolerance=args.amount_tolerance,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

