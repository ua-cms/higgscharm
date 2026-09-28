#!/usr/bin/env python3
"""
Inspect the MERGED parquet outputs (post merge_cr_parquets_fixed.py) to see
what's actually in them before deciding how compute_an_ss_yield.py should
read from them.

Checks, per era/category file:
    - existence, row/column count
    - full schema (name: type)
    - whether any column looks like a dataset/process identifier
      (so we know if data vs. MC vs. ZZ is still distinguishable post-merge)
    - null counts per column (top offenders)
    - basic value sanity: min/max/mean for numeric columns, unique values
      for small-cardinality columns (e.g. pdgId/flavor columns)
    - row count comparison against the raw (unmerged) per-dataset files,
      if the raw base dir is provided, to confirm nothing was dropped

Usage:
    # merged-only inspection
    python inspect_merged_parquets.py \\
        --merged-os  /eos/user/p/pkatris/higgscharm/outputs/hplus_FR_merged \\
        --merged-ss  /eos/user/p/pkatris/higgscharm/outputs/hplus_FR_merged_SS \\
        --year all

    # also cross-check row counts against the raw (pre-merge) trees
    python inspect_merged_parquets.py \\
        --merged-os  /eos/user/p/pkatris/higgscharm/outputs/hplus_FR_merged \\
        --merged-ss  /eos/user/p/pkatris/higgscharm/outputs/hplus_FR_merged_SS \\
        --raw-os     /eos/user/p/pkatris/higgscharm/outputs/hplus_FR \\
        --raw-ss     /eos/user/p/pkatris/higgscharm/outputs/hplus_FR_SS \\
        --year 2022postEE
"""

import argparse
import glob
import os

import numpy as np
import pyarrow.parquet as pq

ERAS = ["2022postEE", "2022preEE", "2023preBPix", "2023postBPix"]

# categories expected in each merged tree
OS_CATEGORIES = ["CR_3P1F", "CR_2P2F"]
SS_CATEGORIES = ["CR_SS"]

# column names that would indicate dataset/process identity survived the merge
_DATASET_LIKE_COL_HINTS = (
    "dataset", "process", "sample", "sample_name", "dataset_name", "proc",
)


def _fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def inspect_one_file(path, max_str_cols=6, max_num_cols=12):
    print(f"  File: {path}")
    if not os.path.exists(path):
        print("    MISSING\n")
        return None

    size = os.path.getsize(path)
    t = pq.read_table(path)
    n_rows, n_cols = t.num_rows, t.num_columns
    print(f"    Size: {_fmt_bytes(size)}   Rows: {n_rows:,}   Columns: {n_cols}")

    schema_names = t.schema.names
    print(f"    Schema ({n_cols} columns):")
    for f in t.schema:
        print(f"      {f.name:<30} {f.type}")

    # Does anything look like a dataset/process identity column?
    hits = [c for c in schema_names
            if any(h in c.lower() for h in _DATASET_LIKE_COL_HINTS)]
    if hits:
        print(f"    Dataset-identity-like column(s) FOUND: {hits}")
        for c in hits:
            vals = t.column(c).to_pylist()
            uniq = sorted(set(str(v) for v in vals))[:20]
            print(f"      unique values in '{c}' (up to 20): {uniq}")
    else:
        print("    Dataset-identity-like column: NONE FOUND "
              "(data/MC/ZZ are NOT distinguishable in this merged file)")

    # Null counts (top offenders)
    print("    Null counts (columns with >0 nulls, up to 15 shown):")
    null_info = []
    for i, f in enumerate(t.schema):
        col = t.column(i)
        n_null = col.null_count
        if n_null > 0:
            null_info.append((f.name, n_null, n_null / n_rows if n_rows else 0))
    null_info.sort(key=lambda x: -x[1])
    if null_info:
        for name, n_null, frac in null_info[:15]:
            print(f"      {name:<30} {n_null:>10,}  ({frac:.1%})")
    else:
        print("      none")

    # Quick numeric sanity (min/max/mean) for a handful of numeric columns
    print("    Numeric sanity (min / mean / max), sample of columns:")
    shown = 0
    for f in t.schema:
        if shown >= max_num_cols:
            break
        if str(f.type) not in ("double", "float", "float64", "int64", "int32", "bool"):
            continue
        arr = t.column(f.name).to_pandas()
        arr = arr.dropna()
        if len(arr) == 0:
            continue
        try:
            print(f"      {f.name:<30} min={arr.min():.4g}  mean={arr.mean():.4g}  max={arr.max():.4g}")
        except Exception:
            continue
        shown += 1

    # Small-cardinality columns (likely categorical, e.g. pdgId/flavor)
    print("    Small-cardinality columns (<=10 unique values), sample:")
    shown = 0
    for f in t.schema:
        if shown >= max_str_cols:
            break
        try:
            col_pd = t.column(f.name).to_pandas()
        except Exception:
            continue
        uniq = col_pd.dropna().unique()
        if 0 < len(uniq) <= 10:
            print(f"      {f.name:<30} {sorted(uniq.tolist(), key=str)}")
            shown += 1

    print()
    return {"rows": n_rows, "cols": n_cols, "columns": schema_names, "has_dataset_col": bool(hits)}


def raw_row_count(raw_base, era, category):
    """Sum row counts across all per-dataset files for this era/category in
    the RAW (unmerged) tree, for cross-checking against the merged total."""
    pattern = os.path.join(raw_base, era, "*", category, "*.parquet")
    files = glob.glob(pattern)
    total = 0
    per_dataset = {}
    for fpath in files:
        dataset_dir = os.path.basename(os.path.dirname(os.path.dirname(fpath)))
        try:
            n = pq.read_metadata(fpath).num_rows
        except Exception as e:
            print(f"    Warning: could not read metadata for {fpath}: {e}")
            continue
        total += n
        per_dataset[dataset_dir] = per_dataset.get(dataset_dir, 0) + n
    return total, per_dataset, len(files)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--merged-os", required=True, help="hplus_FR_merged dir")
    parser.add_argument("--merged-ss", required=True, help="hplus_FR_merged_SS dir")
    parser.add_argument("--raw-os", default=None, help="hplus_FR dir (pre-merge), optional cross-check")
    parser.add_argument("--raw-ss", default=None, help="hplus_FR_SS dir (pre-merge), optional cross-check")
    parser.add_argument("--year", default="all", help="Era or 'all'")
    args = parser.parse_args()

    eras = ERAS if args.year == "all" else [args.year]

    for era in eras:
        print(f"\n{'='*70}")
        print(f"  Era: {era}")
        print(f"{'='*70}")

        print(f"\n--- OS merged tree: {args.merged_os} ---")
        for cat in OS_CATEGORIES:
            path = os.path.join(args.merged_os, era, f"{cat}.parquet")
            info = inspect_one_file(path)
            if info and args.raw_os:
                raw_total, per_ds, n_files = raw_row_count(args.raw_os, era, cat)
                print(f"    Cross-check vs RAW {args.raw_os}/{era}/*/{cat}/*.parquet "
                      f"({n_files} files):")
                print(f"      raw total rows:    {raw_total:,}")
                print(f"      merged total rows: {info['rows']:,}")
                match = "OK match" if raw_total == info["rows"] else "MISMATCH"
                print(f"      -> {match}")
                if per_ds:
                    print(f"      raw per-dataset breakdown: "
                          f"{ {k: v for k, v in sorted(per_ds.items())} }")
                print()

        print(f"\n--- SS merged tree: {args.merged_ss} ---")
        for cat in SS_CATEGORIES:
            path = os.path.join(args.merged_ss, era, f"{cat}.parquet")
            info = inspect_one_file(path)
            if info and args.raw_ss:
                raw_total, per_ds, n_files = raw_row_count(args.raw_ss, era, cat)
                print(f"    Cross-check vs RAW {args.raw_ss}/{era}/*/{cat}/*.parquet "
                      f"({n_files} files):")
                print(f"      raw total rows:    {raw_total:,}")
                print(f"      merged total rows: {info['rows']:,}")
                match = "OK match" if raw_total == info["rows"] else "MISMATCH"
                print(f"      -> {match}")
                if per_ds:
                    print(f"      raw per-dataset breakdown: "
                          f"{ {k: v for k, v in sorted(per_ds.items())} }")
                print()

    print("\nDone.")


if __name__ == "__main__":
    main()