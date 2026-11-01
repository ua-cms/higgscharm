#!/usr/bin/env python3
"""
Pre-jet-veto GEN c-jet split of the 4FS FxFx signal chunks (2026-10-07).

The 4FS jecshifts_fschad sidecars (production 10-02) predate the processor's
sumw_preveto_genc{0,1,2p} keys (added 10-05), so the direct stitch has no
pre-veto 0c fraction for the 4FS piece. This rebuilds it from NanoAOD for exactly
the chunks that production read: for every deduplicated sidecar (<guid>_..._<a>-<b>.json)
the file with ROOT UUID == guid (path from sumw_fix_2026_10_02/file_info.json) is
opened and genWeight is summed per n_genc10 bin over [a, b), with the processor's
definition (base.count_gen_cjets: GenJet hadronFlavour == 4, pT > 10 GeV, no eta cut).

Closure: per chunk, sum over the bins must equal the sidecar's sumw_preveto.
--closure-3fs runs the same code on 3FS chunks, whose sidecars carry
sumw_preveto_genc*, and compares bin by bin.

Output: <out>/preveto_genc_4fs.json = {era: {"<sidecar basename>": {"sumw_preveto_genc0": ..., ...}}}
"""
import os
import re
import sys
import json
import glob
import argparse
import numpy as np
import uproot

OUT_BASE = "/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class_ctag2d_jecshifts_fschad"
FILE_INFO = "/eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02/file_info.json"
chunk_re = re.compile(r'_(\d+)-(\d+)\.json$')

p = argparse.ArgumentParser()
p.add_argument("--eras", nargs="+", default=["2022preEE", "2022postEE"])
p.add_argument("--prefix", default="HPlusCharm")
p.add_argument("--closure-3fs", type=int, default=0, help="also check N 3FS sidecars (postEE) against their keys")
p.add_argument("--out", default=os.path.dirname(os.path.abspath(__file__)))
args = p.parse_args()


def sidecars(era, prefix):
    """Deduplicated sidecars (latest mtime per basename), as the builder's load_split_sumw."""
    latest = {}
    for jf in glob.glob(f"{OUT_BASE}/{era}/{prefix}*/sumw/*.json"):
        b = os.path.basename(jf)
        mt = os.path.getmtime(jf)
        if b not in latest or mt > latest[b][1]:
            latest[b] = (jf, mt)
    return {b: v[0] for b, v in latest.items()}


def split(path, ranges):
    for attempt in range(3):
        try:
            with uproot.open(path, timeout=300) as f:
                t = f["Events"]
                a = t.arrays(["genWeight", "GenJet_pt", "GenJet_hadronFlavour"])
                gid = str(f.file.uuid)
            break
        except Exception as e:
            err = e
    else:
        raise RuntimeError(f"{path}: {err!r}")
    gw = np.asarray(a["genWeight"], dtype=np.float64)
    import awkward as ak
    n = ak.to_numpy(ak.sum((a["GenJet_hadronFlavour"] == 4) & (a["GenJet_pt"] > 10.0), axis=1))
    out = {}
    for lo, hi in ranges:
        g, m = gw[lo:hi], n[lo:hi]
        out[(lo, hi)] = {"sumw_preveto_genc0": float(g[m == 0].sum()),
                         "sumw_preveto_genc1": float(g[m == 1].sum()),
                         "sumw_preveto_genc2p": float(g[m >= 2].sum())}
    return gid, out


def run(era, prefix, paths_by_guid, limit=None, only_known=False):
    sc = sidecars(era, prefix)
    by_guid = {}
    for b, jf in sc.items():
        m = chunk_re.search(b)
        by_guid.setdefault(b.split("_%2F")[0], []).append((int(m.group(1)), int(m.group(2)), b, jf))
    guids = sorted(g for g in by_guid if g in paths_by_guid) if only_known else sorted(by_guid)
    guids = guids[:limit] if limit else guids
    res, worst = {}, 0.0
    for i, g in enumerate(guids):
        if g not in paths_by_guid:
            raise RuntimeError(f"{era} {prefix}: no path for guid {g}")
        gid, out = split(paths_by_guid[g], [(a, b) for a, b, _, _ in by_guid[g]])
        if gid != g:
            raise RuntimeError(f"UUID mismatch: {paths_by_guid[g]} has {gid}, expected {g}")
        for a, b, base, jf in by_guid[g]:
            rec = json.load(open(jf))
            s = out[(a, b)]
            tot = sum(s.values())
            d = abs(tot - rec["sumw_preveto"]) / abs(rec["sumw_preveto"])
            worst = max(worst, d)
            if d > 1e-6:
                raise RuntimeError(f"closure: {base} sum {tot} != sidecar sumw_preveto {rec['sumw_preveto']}")
            if "sumw_preveto_genc0" in rec:
                for k, v in s.items():
                    if abs(v - rec[k]) > 1e-6 * max(1.0, abs(rec[k])):
                        raise RuntimeError(f"closure: {base} {k} {v} != sidecar {rec[k]}")
            res[base] = s
        if (i + 1) % 25 == 0:
            print(f"  {era} {prefix}: {i + 1}/{len(guids)} files", flush=True)
    print(f"{era} {prefix}: {len(guids)} files, {len(res)} chunks, worst |sum - sumw_preveto|/sumw_preveto = {worst:.2e}", flush=True)
    return res


def paths_3fs(era, n):
    """3FS guid -> URL from the AFS lxplus fileset JSON (the new 3FS is not in file_info.json)."""
    fj = f"/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/analysis/filesets/fileset_{era}_nanov12_lxplus.json"
    urls = json.load(open(fj))[f"Stitch3FS_cH_{era}"]
    urls = urls if isinstance(urls, list) else list(urls.get("files", urls))
    out = {}
    for u in urls[:n]:
        with uproot.open(u, timeout=300) as f:
            out[str(f.file.uuid)] = u
    return out


if args.closure_3fs:
    print("3FS closure (sidecars carry sumw_preveto_genc*):", flush=True)
    p3 = paths_3fs("2022postEE", args.closure_3fs)
    run("2022postEE", "Stitch3FS_cH_", p3, only_known=True)
    print("3FS closure PASSED", flush=True)

info = json.load(open(FILE_INFO))
paths = {g: r["path"] for g, r in info.items()}
result = {era: run(era, args.prefix, paths) for era in args.eras}
of = os.path.join(args.out, "preveto_genc_4fs.json")
json.dump(result, open(of, "w"), indent=1)
for era, r in result.items():
    t = {k: sum(c[k] for c in r.values()) for k in ("sumw_preveto_genc0", "sumw_preveto_genc1", "sumw_preveto_genc2p")}
    s = sum(t.values())
    print(f"{era}: pre-veto f(0/1/2p) = " + " ".join(f"{v / s:.4f}" for v in t.values()) + f"  (sum {s:.1f})")
print(f"wrote {of}")
