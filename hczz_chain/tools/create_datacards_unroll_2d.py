#!/usr/bin/env python3
"""
2026-10-27 (opt2d): generalisation of nom4l_card_2026_10_27/create_datacards_unroll_m4l.py with a second, score-major
layout for score categories that each have their own m4l binning:

  --unroll-score-cats c0,c1,...,cK   u = (j + (m4l - 105) / 55) / K,  j = score category of s (K categories)
                                      --bin-edges in u: category j with m4l edges e -> (j + (e - 105) / 55) / K
                                      (python3 create_datacards_unroll_2d.py --print-cat-edges layout.json)
  --unroll-m4l-edges ...             unchanged m4l-major layout (rectangular grids), as below.

Original description (m4l-major):
2D (m4l x MVA score) HcZZ card with the production builder chain, unchanged.

Runs create_datacards_ctag2d_100150_stitched_decorr.py with eos_io.read_parquet patched so that
every parquet read (MC nominal, JES/JER shifts, stitched signal, Z+X) returns, instead of
mva_score_Signal, the unrolled observable

    u = (i + s) / N,   i = m4l bin index of --unroll-m4l-edges (N bins), s = mva_score_Signal in [0, 1)

Events with s < 0 (not scored) or m4l outside the edges keep s = -1 (dropped by the builders' s >= 0
cut). The m4l edges must cover the builder's --mass-window. Pass --bin-edges in u: for score edges
e_0..e_k, the unrolled edges are (i + e_j) / N (see unrolled_edges() / --print-edges).

Usage: the stitched_decorr CLI plus --unroll-m4l-edges 105,115,...,160
       python3 create_datacards_unroll_m4l.py --print-edges M4L_EDGES SCORE_EDGES   (prints --bin-edges)
"""
import sys
import numpy as np

import os as _os; REPO = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))  # repository root (hczz_chain/tools/<this file>)
SCRIPTS = _os.path.join(REPO, "combine", "scripts")


def unrolled_edges(m4l_edges, score_edges):
    n = len(m4l_edges) - 1
    u = sorted({round((i + e) / n, 9) for i in range(n) for e in score_edges})
    return u


M0, M1 = 105.0, 160.0


def cat_edges(cats):
    """cats: [{"score": [lo, hi], "m4l_edges": [...]}, ...] -> (score category edges, unrolled --bin-edges)"""
    K = len(cats)
    sc = [cats[0]["score"][0]] + [c["score"][1] for c in cats]
    u = sorted({round((j + (e - M0) / (M1 - M0)) / K, 9) for j, c in enumerate(cats) for e in c["m4l_edges"]})
    return sc, u


if "--print-cat-edges" in sys.argv:
    import json
    k = sys.argv.index("--print-cat-edges")
    lay = json.load(open(sys.argv[k + 1]))
    sc, u = cat_edges(lay["cats"])
    print(",".join(f"{x:.9f}" for x in sc))
    print(",".join(f"{x:.9f}" for x in u))
    sys.exit(0)

if "--print-edges" in sys.argv:
    k = sys.argv.index("--print-edges")
    m = [float(x) for x in sys.argv[k + 1].split(",")]
    s = [float(x) for x in sys.argv[k + 2].split(",")]
    print(",".join(f"{x:.9f}" for x in unrolled_edges(m, s)))
    sys.exit(0)

MODE = "m4l" if "--unroll-m4l-edges" in sys.argv else "cat"
if MODE == "m4l":
    k = sys.argv.index("--unroll-m4l-edges")
    M4L_EDGES = np.array([float(x) for x in sys.argv[k + 1].split(",")])
    N = len(M4L_EDGES) - 1
else:
    k = sys.argv.index("--unroll-score-cats")
    SCAT = np.array([float(x) for x in sys.argv[k + 1].split(",")])
    SCAT[0], SCAT[-1] = 0.0, 1.0 + 1e-9
    M4L_EDGES = np.array([M0, M1])
    N = len(SCAT) - 1
del sys.argv[k:k + 2]
STATS = {"files": 0, "events": 0}

sys.path.insert(0, SCRIPTS)
sys.path.insert(0, REPO)
import eos_io as E
import pyarrow as pa

_orig_read = E.read_parquet


def _mass(tab):
    if "zz_mass_inclusive" in tab.column_names:
        return np.asarray(tab.column("zz_mass_inclusive").to_numpy(zero_copy_only=False), dtype=float)
    if "m4l" in tab.column_names:      # Z+X: may be stored as length-1 lists
        vals = tab.column("m4l").to_pylist()
        return np.array([(v[0] if len(v) else np.nan) if isinstance(v, (list, tuple)) else
                         (np.nan if v is None else v) for v in vals], dtype=float)
    raise KeyError("no m4l column to unroll the score with")


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
    if MODE == "m4l":
        i = np.searchsorted(M4L_EDGES, m, side="right") - 1
        ok = (s >= 0) & (i >= 0) & (i < N) & np.isfinite(m)
        u = np.where(ok, (np.clip(i, 0, N - 1) + np.clip(s, 0.0, 1.0 - 1e-9)) / N, -1.0)
    else:
        j = np.searchsorted(SCAT, np.clip(s, 0.0, 1.0), side="right") - 1
        x = (m - M0) / (M1 - M0)
        ok = (s >= 0) & (j >= 0) & (j < N) & np.isfinite(m) & (x >= 0) & (x < 1)
        u = np.where(ok, (np.clip(j, 0, N - 1) + np.clip(x, 0.0, 1.0 - 1e-9)) / N, -1.0)
    STATS["files"] += 1
    STATS["events"] += len(u)
    idx = tab.column_names.index("mva_score_Signal")
    tab = tab.set_column(idx, "mva_score_Signal", pa.array(u))
    if columns is not None:                 # return only what was asked for (plus nothing extra)
        keep = [c for c in tab.column_names if c in columns]
        tab = tab.select(keep)
    return tab


E.read_parquet = read_parquet

import runpy
if MODE == "m4l":
    print(f"[unroll] m4l edges {M4L_EDGES.tolist()} ({N} bins); mva_score_Signal -> (i + s) / {N}")
else:
    print(f"[unroll] score categories {SCAT.tolist()} ({N}); mva_score_Signal -> (j + (m4l-105)/55) / {N}")
sys.argv[0] = f"{SCRIPTS}/create_datacards_ctag2d_100150_stitched_decorr.py"
runpy.run_path(sys.argv[0], run_name="__main__")
print(f"[unroll] patched {STATS['files']} parquet reads, {STATS['events']} events")
