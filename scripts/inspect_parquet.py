#!/usr/bin/env python3
"""
Inspect parquet files from zplusl workflow output.

Directory structure expected:
    <input>/<workflow>/<year>/<dataset>/<flavor>/*.parquet

Usage:
    python inspect_parquet.py \
        --input /eos/user/p/pkatris/higgscharm/outputs/parquet \
        --workflow zplusl_os \
        --year 2022postEE \
        --max-files 3
"""

import os
import glob
import argparse
import numpy as np
import pyarrow.parquet as pq
import pandas as pd


def _is_list_col(series):
    """Return True if the column contains lists/arrays rather than scalars."""
    first = series.dropna().iloc[0] if len(series.dropna()) > 0 else None
    return isinstance(first, (list, np.ndarray))


def inspect_parquet_files(input_dir, workflow, year, max_files=5):
    base_path = os.path.join(input_dir, workflow, year)
    if not os.path.exists(base_path):
        raise FileNotFoundError(f"Path not found: {base_path}")

    # Structure: <base_path>/<dataset>/<flavor>/*.parquet
    parquet_files = glob.glob(os.path.join(base_path, "*", "*", "*.parquet"))
    print(f"Found {len(parquet_files)} parquet files under {base_path}")

    if not parquet_files:
        print("No parquet files found. Listing what IS there:")
        for root, dirs, files in os.walk(base_path):
            level = root.replace(base_path, '').count(os.sep)
            indent = '  ' * level
            print(f"{indent}{os.path.basename(root)}/")
            if level >= 3:
                break
            for f in files[:5]:
                print(f"{indent}  {f}")
        return

    # ── Directory tree summary ────────────────────────────────────────────────
    print("\nDirectory tree summary:")
    datasets = sorted(set(
        os.path.basename(os.path.dirname(os.path.dirname(f)))
        for f in parquet_files
    ))
    for ds in datasets:
        flavors = sorted(set(
            os.path.basename(os.path.dirname(f))
            for f in parquet_files
            if os.path.basename(os.path.dirname(os.path.dirname(f))) == ds
        ))
        n_files = sum(
            1 for f in parquet_files
            if os.path.basename(os.path.dirname(os.path.dirname(f))) == ds
        )
        print(f"  {ds}/  flavors={flavors}  ({n_files} files)")
    print()

    # ── Per-file inspection ───────────────────────────────────────────────────
    seen_schemas = []

    for i, fpath in enumerate(sorted(parquet_files)[:max_files]):
        flavor  = os.path.basename(os.path.dirname(fpath))
        dataset = os.path.basename(os.path.dirname(os.path.dirname(fpath)))
        fname   = os.path.basename(fpath)

        print("=" * 70)
        print(f"[{i+1}/{min(max_files, len(parquet_files))}]  dataset={dataset}  flavor={flavor}")
        print(f"  file: {fname}")
        print("=" * 70)

        pf = pq.read_table(fpath)
        schema = pf.schema

        # Schema / columns
        print(f"\nColumns ({len(schema)}):")
        for field in schema:
            print(f"  {field.name:50s}  {field.type}")

        # Track unique schemas
        col_names = frozenset(schema.names)
        if col_names not in seen_schemas:
            seen_schemas.append(col_names)
            print(f"\n  *** New unique schema (#{len(seen_schemas)}) ***")

        df = pf.to_pandas()
        n_rows = len(df)
        print(f"\nRows (events): {n_rows:,}    Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")

        # Split columns into list-type vs scalar
        list_cols   = [c for c in df.columns if _is_list_col(df[c])]
        scalar_cols = [c for c in df.columns if c not in list_cols]

        print(f"\nList columns (per-event arrays):  {list_cols}")
        print(f"Scalar columns (per-event values): {scalar_cols}")

        # ── Exploded list columns ──────────────────────────────────────────
        if list_cols:
            print("\n--- Exploded list-column stats (all probe leptons across events) ---")
            exploded = {}
            for col in list_cols:
                exploded[col] = np.concatenate(df[col].values)
            exp_df = pd.DataFrame(exploded)
            print(f"Total probe leptons (rows after explode): {len(exp_df):,}")
            print(f"\nSample probe leptons (first 5):")
            with pd.option_context('display.max_columns', None, 'display.width', 220,
                                   'display.float_format', '{:.4f}'.format):
                print(exp_df.head(5).to_string(index=False))

            print("\nExploded column stats:")
            print(exp_df.describe().to_string())

            # Unique values for boolean/low-cardinality list columns
            print("\nUnique values in list columns (≤20 unique):")
            for col in list_cols:
                vals = exp_df[col].dropna()
                n_unique = vals.nunique()
                if n_unique <= 20:
                    print(f"  {col}: {sorted(vals.unique().tolist())}")

        # ── Scalar columns ────────────────────────────────────────────────
        if scalar_cols:
            scalar_df = df[scalar_cols]
            print("\n--- Scalar column stats (per-event weights etc.) ---")
            print(scalar_df.describe().to_string())

        print()

    if len(parquet_files) > max_files:
        print(f"(Showed {max_files} of {len(parquet_files)} files. Pass --max-files N to see more.)")

    # Schema consistency report
    print(f"\n{'='*70}")
    print(f"Total unique schemas seen: {len(seen_schemas)}")
    if len(seen_schemas) > 1:
        print("WARNING: schemas differ across files!")
        base = seen_schemas[0]
        for j, s in enumerate(seen_schemas[1:], start=2):
            only_in_base = base - s
            only_in_new  = s - base
            print(f"\n  Schema #1 vs Schema #{j}:")
            if only_in_base:
                print(f"    Only in schema #1 : {sorted(only_in_base)}")
            if only_in_new:
                print(f"    Only in schema #{j}: {sorted(only_in_new)}")
    else:
        print("All inspected files share the same schema.")


def main():
    parser = argparse.ArgumentParser(
        description="Inspect parquet files. Expects <input>/<workflow>/<year>/<dataset>/<flavor>/*.parquet"
    )
    parser.add_argument("--input",    required=True,
                        help="Input directory, e.g. .../outputs/parquet")
    parser.add_argument("--workflow", default="zplusl_os",
                        choices=["zplusl_ss", "zplusl_os"],
                        help="Workflow name (default: zplusl_os)")
    parser.add_argument("--year",     default="2022postEE",
                        help="Year/era (default: 2022postEE)")
    parser.add_argument("--max-files", type=int, default=5,
                        help="Max files to inspect in detail (default: 5)")
    args = parser.parse_args()

    inspect_parquet_files(args.input, args.workflow, args.year, args.max_files)


if __name__ == "__main__":
    main()