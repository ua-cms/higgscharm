#!/usr/bin/env python3
"""Run the card-K builder (create_datacards_ctag2d_100150_stitched_decorr.py, same CLI) and
dump the per-event inputs of the binned templates for the unbinned MVA-score fit (2026-10-18).

dc.build_histograms (outermost wrapper) is wrapped: before it fills the histograms, every
process's (score, nominal weight, per-event syst weights) and Z+X, plus the JES/JER-shifted
(score, weight), go to <--dump> as one compressed npz. The binned card is still written to
--output as usual, so the dump and the card come from the same weights.

Usage: python3 dump_events.py --dump events.npz <stitched_decorr CLI>
"""
import sys

DUMP = sys.argv[sys.argv.index("--dump") + 1]
del sys.argv[sys.argv.index("--dump"):sys.argv.index("--dump") + 2]

sys.path.insert(0, "/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts")
import numpy as np
import create_datacards_ctag2d_100150_stitched_decorr as sd  # noqa: E402  (patches dc at import)

dc = sd.dc
_orig = dc.build_histograms


def build_histograms_dump(mc_data, zx_scores, zx_weights, jec_data=None):
    out = {"mva_bins": np.asarray(dc.MVA_BINS), "zx_score": np.asarray(zx_scores),
           "zx_weight": np.asarray(zx_weights)}
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
        for s, by_dir in (jec_data or {}).get(proc, {}).items():
            for d, (js, jw) in by_dir.items():
                out[f"{proc}|jec|{s}|{d}|score"], out[f"{proc}|jec|{s}|{d}|weight"] = js, jw
    out["procs"] = np.array(procs)
    np.savez_compressed(DUMP, **out)
    print(f"\n[dump] {len(procs)} processes, {len(out)} arrays -> {DUMP}")
    for p in procs:
        print(f"  {p:<10} n={len(out[p + '|score']):>8}  sumw={out[p + '|weight'].sum():.5f}")
    print(f"  ZX         n={len(out['zx_score']):>8}  sumw={out['zx_weight'].sum():.5f}")
    return _orig(mc_data, zx_scores, zx_weights, jec_data=jec_data)


dc.build_histograms = build_histograms_dump

if __name__ == "__main__":
    print(f"DUMP BUILD (card K inputs), capfix {sd.rs.CAPFIX} -> {DUMP}")
    dc.main()
