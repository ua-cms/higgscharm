#!/usr/bin/env python3
"""Step 2 of the unbinned 2D fit (2026-10-30): a binned Combine card whose templates are the smooth
2D pdfs of make_pdfs_2d.py, integrated in a chosen binning of (m4l, u). As on 10-19 (1D: 200 smooth
bins = unbinned, 453 vs 453), a fine binning is the unbinned limit; the cat40 cells isolate the
effect of the smoothing alone.

Rates, lnN rows and shape rows are the cat40 card's. A shape systematic's variation on the pdf
grid = nominal density x (card Up/nominal ratio of the cat40 cell containing the grid cell), so the
systematics stay at the cat40 card's resolution and keep its rate effect. No autoMCStats (smooth
templates); compare with the cat40 card without autoMCStats.

  --binning fine:<NM>x<NU>   NM equal m4l bins in 105-160 x NU equal u bins (score-major unroll)
  --binning cat40            the cat40 card's 40 cells
Usage: python3 make_card_2d.py <pdfs_2d.npz> <cat40 card dir> <out dir> --binning fine:55x40
"""
import argparse
import os

import numpy as np
import uproot

ap = argparse.ArgumentParser()
ap.add_argument("pdfs")
ap.add_argument("card_dir")
ap.add_argument("out")
ap.add_argument("--binning", default="fine:55x40")
a = ap.parse_args()

P = np.load(a.pdfs, allow_pickle=False)
procs = [str(p) for p in P["procs"]]
xe, ye, cellbin = P["xe"], P["ye"], P["cellbin"]
NX, NY = len(xe) - 1, len(ye) - 1
cell = np.diff(xe)[:, None] * np.diff(ye)[None, :]

if a.binning == "cat40":
    target = cellbin
    NB = int(cellbin.max()) + 1
else:
    nm, nu = (int(v) for v in a.binning.split(":")[1].split("x"))
    assert NX % nm == 0 and NY % nu == 0, "fine binning must divide the pdf grid (220 x 200)"
    im = np.arange(NX) // (NX // nm)
    iu = np.arange(NY) // (NY // nu)
    target = iu[None, :] * nm + im[:, None]
    NB = nm * nu

card = open(os.path.join(a.card_dir, "datacard_mva_with_zx.txt")).read().splitlines()
pnames = rates = None
shapes = []
for l in card:
    t = l.split()
    if not t:
        continue
    if t[0] == "process" and pnames is None:
        pnames = t[1:]
    elif t[0] == "rate":
        rates = dict(zip(pnames, (float(x) for x in t[1:])))
    elif len(t) > 2 and t[1] == "shape":
        shapes.append((t[0], [p for p, v in zip(pnames, t[2:]) if v != "-"]))
assert set(pnames) <= set(procs), set(pnames) - set(procs)
H = uproot.open(os.path.join(a.card_dir, "histograms_mva_with_zx.root"))


def integrate(dens):
    return np.bincount(target.ravel(), weights=(dens * cell).ravel(), minlength=NB)


out = {}
tot = np.zeros(NB)
for p in pnames:
    d = P[f"pdf|{p}"]
    nom = rates[p] * integrate(d)
    out[f"h_{p}"] = nom
    if p != "Signal":
        tot += nom
    hn = H[f"h_{p}"].values()
    for s, plist in shapes:
        if p not in plist:
            continue
        for dname in ("Up", "Down"):
            hv = H[f"h_{p}_{s}{dname}"].values()
            r = np.divide(hv, hn, out=np.ones_like(hn), where=hn != 0)
            # card ratio of the cell holding each grid cell; times the card rate of the nominal
            # (so the integral of the variation = card rate x sum(pdf x r) in the card cells)
            out[f"h_{p}_{s}{dname}"] = rates[p] * integrate(d * r[cellbin])
# extra pdf-level shape systematics (make_pdfs_2d.py --dscb: lepton scale/resolution on the DSCB core)
extra = {}
for k in P.files:
    t = k.split("|")
    if len(t) == 4 and t[0] == "pdf" and t[1] in pnames:
        out[f"h_{t[1]}_{t[2]}{t[3]}"] = rates[t[1]] * integrate(P[k])
        extra.setdefault(t[2], set()).add(t[1])
out["h_data_obs"] = tot

os.makedirs(a.out, exist_ok=True)
import shutil, subprocess, tempfile
# 2026-10-30: uproot files written straight to EOS ended up with a corrupt compressed record (R__unzip_header) in
# 2 of 8 cards; write locally, read every histogram back with ROOT, then copy and compare md5.
_tmpd = tempfile.mkdtemp(prefix="card_", dir=os.environ.get("TMPDIR", "/tmp"))
_loc = os.path.join(_tmpd, "histograms_2d.root")
with uproot.recreate(_loc) as f:
    e = np.arange(NB + 1, dtype=float)
    for k, v in out.items():
        f[k] = (np.asarray(v, dtype=float), e)
_chk = subprocess.run(["python3", "/eos/user/s/snandaku/Analysis/unbinned2d_2026_10_30/check_root.py", _loc])
if _chk.returncode:
    raise SystemExit(f"corrupt histograms in {_loc}")
shutil.copy(_loc, os.path.join(a.out, "histograms_2d.root"))
_md5 = [subprocess.run(["md5sum", x], capture_output=True, text=True).stdout.split()[0] for x in (_loc, os.path.join(a.out, "histograms_2d.root"))]
if _md5[0] != _md5[1]:
    raise SystemExit(f"md5 mismatch after copy to {a.out}")
shutil.rmtree(_tmpd)
L = [l.replace("histograms_mva_with_zx.root", "histograms_2d.root") for l in card if "autoMCStats" not in l]
L.insert(0, f"# 2026-10-30 smooth 2D (m4l, u) card, binning {a.binning}, from {a.pdfs} + {a.card_dir}")
for sy, plist in sorted(extra.items()):
    L.append(f"{sy:<24} shape   " + " ".join("1" if p in plist else "-" for p in pnames))
open(os.path.join(a.out, "datacard_2d.txt"), "w").write("\n".join(L) + "\n")
print(f"{a.out}: {a.binning} -> {NB} bins, bkg {tot.sum():.3f}, signal {out['h_Signal'].sum():.5f}, "
      f"{len(out)} histograms")
