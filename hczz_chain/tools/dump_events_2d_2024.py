#!/usr/bin/env python3
# 2026-11-06: copy of opt2d_2026_10_27/dump_events_2d.py on the 2024 builder (card2024/dump_events_2024.py, no Z+X).
"""2026-10-27: per-event dump with m4l, for the joint (m4l, MVA score) bin optimisation.

Same as Analysis/unbinned_mva_2026_10_18/dump_events.py (card-K builder, identical weights / systematics /
JES-JER shifts / Z+X), with eos_io.read_parquet patched as in
nom4l_card_2026_10_27/create_datacards_unroll_m4l.py, but on a FINE m4l grid:

    u = (i + s) / N,  i = index of m4l in FINE (105..160 GeV, 0.25 GeV steps, N = 220), s = mva_score_Signal
    decode: i = floor(u * N), s = u * N - i, m4l = FINE[i] + 0.125 (bin centre)

s is clipped to [1e-6, 1 - 1e-6] so floor(u * N) is safe against rounding (score change <= 1e-6).
Usage: python3 dump_events_2d.py --dump events_2d.npz <stitched_decorr CLI>
"""
import sys, runpy
import numpy as np
import os as _os; REPO = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))  # repository root (hczz_chain/tools/<this file>)
SCRIPTS = _os.path.join(REPO, "combine", "scripts")
FINE = np.round(np.arange(105.0, 160.0 + 1e-9, 0.25), 4)
N = len(FINE) - 1
sys.path.insert(0, SCRIPTS)
sys.path.insert(0, REPO)
import eos_io as E
import pyarrow as pa
_orig_read = E.read_parquet
STATS = {"files": 0, "events": 0}


def _mass(tab):
    if "zz_mass_inclusive" in tab.column_names:
        return np.asarray(tab.column("zz_mass_inclusive").to_numpy(zero_copy_only=False), dtype=float)
    if "m4l" in tab.column_names:
        vals = tab.column("m4l").to_pylist()
        return np.array([(v[0] if len(v) else np.nan) if isinstance(v, (list, tuple)) else
                         (np.nan if v is None else v) for v in vals], dtype=float)
    raise KeyError("no m4l column")


def read_parquet(path, columns=None):
    req = None if columns is None else list(columns)
    if req is not None and "mva_score_Signal" in req:
        avail = set(E.parquet_columns(path))
        for c in ("zz_mass_inclusive", "m4l"):
            if c in avail and c not in req:
                req.append(c)
                break
    tab = _orig_read(path, req)
    if "mva_score_Signal" not in tab.column_names:
        return tab
    s = np.asarray(tab.column("mva_score_Signal").to_numpy(zero_copy_only=False), dtype=float)
    m = _mass(tab)
    i = np.searchsorted(FINE, m, side="right") - 1
    ok = (s >= 0) & (i >= 0) & (i < N) & np.isfinite(m)
    u = np.where(ok, (np.clip(i, 0, N - 1) + np.clip(s, 1e-6, 1.0 - 1e-6)) / N, -1.0)
    STATS["files"] += 1; STATS["events"] += len(u)
    idx = tab.column_names.index("mva_score_Signal")
    tab = tab.set_column(idx, "mva_score_Signal", pa.array(u))
    if columns is not None:
        tab = tab.select([c for c in tab.column_names if c in columns])
    return tab


E.read_parquet = read_parquet
print(f"[dump2d] fine m4l grid {FINE[0]}..{FINE[-1]} GeV, {N} bins; mva_score_Signal -> (i + s) / {N}")
sys.argv[0] = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "dump_events_2024.py")
runpy.run_path(sys.argv[0], run_name="__main__")
print(f"[dump2d] patched {STATS['files']} parquet reads, {STATS['events']} events")
