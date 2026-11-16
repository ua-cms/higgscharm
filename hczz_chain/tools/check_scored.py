#!/usr/bin/env python3
"""Scoring completeness (2026-11-16, user: "make sure we do not lose any of the output parquets"): every raw parquet the scorer
reads (the same selection: --datasets prefixes, dedupe_parquet_files_by_source) must have its scored copy at the same relative path,
readable, with the same number of rows and the MVA columns. Catches files lost to EOS quota / write errors, which the scorer does
not always report (2026-10-28: scoring exited 0 on quota errors). Exit 1 and list the bad files otherwise.
  check_scored.py --raw-dir <prod>/<workflow> --scored-dir <..._scored_model> --era <era> [--datasets p1 p2 ...]"""
import argparse, sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pyarrow.parquet as pq
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))   # repository root
from analysis.postprocess.run_mva_postprocess import dedupe_parquet_files_by_source

ap = argparse.ArgumentParser()
ap.add_argument("--raw-dir", required=True)
ap.add_argument("--scored-dir", required=True)
ap.add_argument("--era", required=True)
ap.add_argument("--datasets", nargs="*", default=None)
ap.add_argument("--workers", type=int, default=32)
a = ap.parse_args()
era_in, era_out = Path(a.raw_dir) / a.era, Path(a.scored_dir) / a.era
raw = list(era_in.glob("**/*.parquet"))
if a.datasets:
    raw = [f for f in raw if f.relative_to(era_in).parts[0].startswith(tuple(a.datasets))]
files = dedupe_parquet_files_by_source(raw)


def check(f):
    """(rows, problem or None) of one raw parquet and its scored copy (parquet footers only)."""
    out = era_out / f.relative_to(era_in)
    try:
        n_in = pq.ParquetFile(f).metadata.num_rows
    except Exception as e:
        return 0, f"RAW UNREADABLE {f}: {e}"
    if not out.exists():
        return n_in, f"MISSING {out}"
    try:
        pf = pq.ParquetFile(out)
        n_out, cols = pf.metadata.num_rows, set(pf.schema_arrow.names)
    except Exception as e:
        return n_in, f"UNREADABLE {out}: {e}"
    if n_out != n_in:
        return n_in, f"ROWS {out}: {n_out} scored vs {n_in} raw"
    if "mva_score_Signal" not in cols:
        return n_in, f"NO SCORE COLUMN {out}"
    return n_in, None


with ThreadPoolExecutor(max_workers=a.workers) as ex:   # EOS FUSE metadata reads are latency bound
    res = list(ex.map(check, files))
rows = sum(r for r, _ in res)
bad = [b for _, b in res if b]
print(f"[check_scored] {a.era} {a.scored_dir}: {len(files)} raw parquets ({len(raw)} before dedupe), {rows:,} rows, {len(bad)} bad")
for b in bad[:50]:
    print("  ", b)
if not files:
    sys.exit("[check_scored] no raw parquets found")
sys.exit(1 if bad else 0)
