#!/usr/bin/env python3
"""
Merge hplusc_mva_4class_CR parquet files per era and category into single files.

Some columns (the *_2p2f* branches) were written inconsistently across files:
scalar `double`/`int64` in most files, but `list<double>`/`list<int64>` (length 0 or 1)
in a minority of files. This script detects any list-typed column per file and
flattens it to a scalar (first element, or null if the list is empty) before
concatenating, so all files end up with a uniform schema.

Usage:
    python merge_cr_parquets_fixed.py --year 2022postEE
    python merge_cr_parquets_fixed.py --year all
"""
import argparse
import glob
from collections import defaultdict
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pathlib import Path

CR_BASE = "/eos/user/p/pkatris/higgscharm/outputs/hplus_FR"
MERGE_OUT = "/eos/user/p/pkatris/higgscharm/outputs/hplus_FR_merged"
ERAS = ["2022postEE", "2022preEE", "2023preBPix", "2023postBPix"]
CATEGORIES = ["CR_3P1F", "CR_2P2F"]


def flatten_list_columns(table: pa.Table) -> pa.Table:
    """Replace any list<...>-typed column with a scalar column (first element,
    null if the list is empty/None). Leaves already-scalar columns untouched.

    Implemented via to_pylist() rather than pc.list_element() because some
    PyArrow versions (e.g. the LCG_104 build) raise ArrowInvalid on
    out-of-bounds indices for empty lists instead of returning null.
    """
    for i, field in enumerate(table.schema):
        if pa.types.is_list(field.type) or pa.types.is_large_list(field.type):
            col = table.column(i)
            py_list = col.to_pylist()  # list of (list-or-None) per row
            flat_vals = [(v[0] if v else None) for v in py_list]
            value_type = field.type.value_type
            flat = pa.array(flat_vals, type=value_type)
            new_field = pa.field(field.name, value_type, nullable=True)
            table = table.set_column(i, new_field, flat)
    return table


def unify_column_types(tables):
    """Ensure every column has the same type across all per-file tables.

    After flattening, some columns are still genuinely typed differently
    across files (e.g. l3_pdgid_2p2f is int64 in some files, double in
    others) because the upstream writer was inconsistent. int<->double
    mismatches are resolved by promoting to double (safe, no precision
    loss for small integers like PDG IDs / charges). Any other kind of
    mismatch is reported explicitly rather than silently mis-cast.
    """
    field_types = defaultdict(set)
    for t in tables:
        for f in t.schema:
            field_types[f.name].add(str(f.type))

    target_type = {}
    for name, types in field_types.items():
        if len(types) == 1:
            continue
        if types <= {"double", "int64", "int32", "float", "float64", "bool"}:
            target_type[name] = pa.float64()
        else:
            raise TypeError(
                f"Column '{name}' has incompatible types across files that "
                f"aren't a simple int/double mismatch: {types}. "
                f"Inspect these files manually before merging."
            )

    if not target_type:
        return tables

    print(f"  Unifying types for {len(target_type)} column(s): {list(target_type)}")
    fixed_tables = []
    for t in tables:
        for name, ttype in target_type.items():
            idx = t.schema.get_field_index(name)
            if idx == -1:
                continue
            if t.schema.field(idx).type != ttype:
                col = pc.cast(t.column(idx), ttype)
                t = t.set_column(idx, pa.field(name, ttype, nullable=True), col)
        fixed_tables.append(t)
    return fixed_tables


def merge_era_category(era, category):
    files = glob.glob(f"{CR_BASE}/{era}/*/{category}/*.parquet")
    if not files:
        print(f"  No files found for {era}/{category}")
        return

    out_path = Path(MERGE_OUT) / era / f"{category}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if out_path.exists():
        print(f"  Already exists, skipping: {out_path}")
        return

    print(f"  Merging {len(files)} files -> {out_path}")

    tables = []
    for f in files:
        t = pq.read_table(f)
        t = flatten_list_columns(t)
        tables.append(t)

    tables = unify_column_types(tables)

    try:
        # PyArrow >= 14
        table = pa.concat_tables(tables, promote_options="permissive")
    except TypeError:
        # Older PyArrow (e.g. LCG_104): use the legacy 'promote' kwarg
        table = pa.concat_tables(tables, promote=True)
    print(f"  Rows: {table.num_rows:,}  Columns: {table.num_columns}")

    # Allow nulls in all fields to avoid schema conflicts across files
    new_schema = pa.schema([f.with_nullable(True) for f in table.schema])
    table = table.cast(new_schema)
    pq.write_table(table, out_path, compression="snappy")
    print(f"  Done: {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", default="all", help="Era or 'all'")
    args = parser.parse_args()

    eras = ERAS if args.year == "all" else [args.year]

    for era in eras:
        print(f"\n=== {era} ===")
        for cat in CATEGORIES:
            merge_era_category(era, cat)

    print("\nAll done!")


if __name__ == "__main__":
    main()