#!/usr/bin/env python3
"""2026-11-09: predict what check_missing_syst() (create_datacards_ctag2d_100150.py) will flag, from parquet SCHEMAS only
(one file per sample dir). Run with the same HCZZ_* environment as the card build.
Usage: scan_missing_syst.py <production dir> <era> [<era> ...]
"""
import os
import re
import sys
from collections import defaultdict

import pyarrow.parquet as pq

sys.argv, ARGS = sys.argv[:1], sys.argv[1:]
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import create_datacards_ctag2d_100150 as dc   # noqa: E402
import eos_io as E                             # noqa: E402

prod, eras = ARGS[0], ARGS[1:]
miss = defaultdict(set)
nsamp = defaultdict(set)
for era in eras:
    era_dir = os.path.join(prod, era)
    for sample in E.listdir(era_dir):
        proc = dc.get_process_from_path(sample)
        if proc is None:
            continue
        files = E.files(os.path.join(era_dir, sample, "base"), ".parquet")
        if not files:
            continue
        cols = set(pq.read_schema(files[0]).names)
        base = re.sub(r"_\d+$", "", sample)
        nsamp[proc].add(base)
        usable = set(dc.USABLE_SYST.get(proc, set()))
        if proc != "Signal" and dc.is_old_hplusbottom(sample):
            usable -= dc.HPLUSBOTTOM_BUGGY_SYST
        for s in usable:
            for d in ("Up", "Down"):
                if dc.syst_col(s, d, era) not in cols:
                    miss[(proc, s + d)].add(f"{era}:{base}")
if not miss:
    print("no missing systematic columns")
for (proc, sd), samples in sorted(miss.items()):
    print(f"{proc:12s} {sd:34s} missing in {len(samples)} sample-eras (of {len(nsamp[proc])} samples): "
          f"{', '.join(sorted(samples)[:4])}{' ...' if len(samples) > 4 else ''}")
