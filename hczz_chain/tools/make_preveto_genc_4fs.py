#!/usr/bin/env python3
"""Add eras to the 4FS pre-jet-veto GEN-c Σw input of the stitched builders (--preveto-genc-4fs).
2022/23: from Analysis/fs_stitch_direct_2026_10_07/scan_4fs_preveto_genc.py (those 4FS productions predate the sidecar keys).
2024+: the production sidecars already carry sumw_preveto_genc{0,1,2p} (analysis/processors/base.py, 2026-10-05), so they are
copied per sidecar (same key = sidecar file name the builder looks up). Existing eras are never changed.
  make_preveto_genc_4fs.py <json in> <json out> <production dir> <era> [<era> ...]   (4FS partitions = HPlusCharm*)"""
import glob, json, os, sys
src, dst, prod, eras = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
d = json.load(open(src))
for era in eras:
    if era in d:
        sys.exit(f"{era} already in {src}, not overwriting")
    out, bad = {}, 0
    for jf in sorted(glob.glob(f"{prod}/{era}/HPlusCharm*/sumw/*.json")):
        rec = json.load(open(jf)); b = os.path.basename(jf)
        sc = {k: rec[k] for k in ("sumw_preveto_genc0", "sumw_preveto_genc1", "sumw_preveto_genc2p")}
        if abs(sum(sc.values()) - rec["sumw_preveto"]) > 1e-6 * abs(rec["sumw_preveto"]):
            bad += 1
        if b in out and out[b] != sc:
            sys.exit(f"{era}: two sidecars {b} with different values")
        out[b] = sc
    if not out or bad:
        sys.exit(f"{era}: {len(out)} sidecars, {bad} with a split not adding up to sumw_preveto")
    tot = {k: sum(v[k] for v in out.values()) for k in ("sumw_preveto_genc0", "sumw_preveto_genc1", "sumw_preveto_genc2p")}
    s = sum(tot.values())
    print(f"{era}: {len(out)} 4FS sidecars, pre-veto GEN-c fractions 0c {tot['sumw_preveto_genc0']/s:.4f} 1c {tot['sumw_preveto_genc1']/s:.4f} >=2c {tot['sumw_preveto_genc2p']/s:.4f}")
    d[era] = out
json.dump(d, open(dst, "w"), indent=1)
print("wrote", dst, "eras", list(d))
