#!/usr/bin/env python3
"""
Print event counts (rows) in the merged CR_2P2F / CR_3P1F parquet files.
Run this after merge_cr_parquets_fixed.py has produced the merged files.
"""
import pyarrow.parquet as pq
from pathlib import Path

MERGE_OUT = "/eos/user/p/pkatris/higgscharm/outputs/hplus_FR_merged"
ERAS = ["2022postEE", "2022preEE", "2023preBPix", "2023postBPix"]
CATEGORIES = ["CR_3P1F", "CR_2P2F"]

totals = {cat: 0 for cat in CATEGORIES}

for era in ERAS:
    print(f"\n=== {era} ===")
    for cat in CATEGORIES:
        path = Path(MERGE_OUT) / era / f"{cat}.parquet"
        if not path.exists():
            print(f"  {cat}: file not found ({path})")
            continue
        n = pq.read_metadata(path).num_rows
        totals[cat] += n
        print(f"  {cat}: {n:,} events")

print("\n=== Totals across all eras ===")
for cat in CATEGORIES:
    print(f"  {cat}: {totals[cat]:,} events")