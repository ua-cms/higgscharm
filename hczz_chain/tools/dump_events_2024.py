#!/usr/bin/env python3
"""2026-11-05: dump_events.py (unbinned_mva_2026_10_18) for the 2024 builder: imports create_datacards_2024 (which
patches the shared builder modules for era 2024), then wraps dc.build_histograms with the same per-event dump.
Usage: python3 dump_events_2024.py --dump events.npz <create_datacards_2024.py CLI>"""
import sys, runpy
DUMP = sys.argv[sys.argv.index("--dump") + 1]
del sys.argv[sys.argv.index("--dump"):sys.argv.index("--dump") + 2]
import os as _os; REPO = _os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))))  # repository root (hczz_chain/tools/<this file>)
sys.path.insert(0, _os.path.join(REPO, "combine", "scripts"))
import numpy as np
import create_datacards_2024 as c24  # noqa: E402  (era-2024 patches, no main at import)
dc = c24.dc
_orig = dc.build_histograms
def build_histograms_dump(mc_data, zx_scores, zx_weights, jec_data=None):
    out = {"mva_bins": np.asarray(dc.MVA_BINS), "zx_score": np.asarray(zx_scores), "zx_weight": np.asarray(zx_weights)}
    procs = []
    for proc, (scores, weights, syst) in mc_data.items():
        if len(scores) == 0:
            continue
        procs.append(proc)
        out[f"{proc}|score"], out[f"{proc}|weight"] = scores, weights
        for s, by_dir in syst.items():
            for d, w in by_dir.items():
                if len(w) == len(scores):
                    out[f"{proc}|syst|{s}|{d}"] = w
    out["procs"] = np.array(procs)
    np.savez_compressed(DUMP, **out)
    print(f"\n[dump] {len(procs)} processes -> {DUMP}")
    for p in procs:
        print(f"  {p:<12} n={len(out[p + '|score']):>8}  sumw={out[p + '|weight'].sum():.5f}")
    return _orig(mc_data, zx_scores, zx_weights, jec_data=jec_data)
dc.build_histograms = build_histograms_dump
if __name__ == "__main__":
    print(f"2024 DUMP BUILD eras {dc.ERAS} -> {DUMP}")
    dc.main()
