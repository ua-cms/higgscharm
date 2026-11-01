#!/usr/bin/env python3
"""KDE-free cross-check (2026-10-19): stat-only binned cards straight from the MC/Z+X events,
with the 20 qqZZ-quantile bins and the top ones split finer (in qqZZ-u). Shows how much
information sits inside the nominal last bins. Z+X bins <= 0 set to 1e-6 (as in no event).
Usage: python3 make_mc_fine.py <events.npz> <binned card dir> <out dir> <u edges, comma sep>"""
import os, sys
import numpy as np
import ROOT
NPZ, CARD, OUT, UE = sys.argv[1:5]
ev = np.load(NPZ)
ue = np.array([float(x) for x in UE.split(",")])
s, w = ev["qqZZ|score"], np.abs(ev["qqZZ|weight"]); o = np.argsort(s); s, c = s[o], np.cumsum(w[o]); c /= c[-1]
edges = np.interp(ue, c, s); edges[0], edges[-1] = 0.0, 1.0
os.makedirs(OUT, exist_ok=True)
card = open(os.path.join(CARD, "datacard_mva_with_zx.txt")).read().splitlines()
procs = next(l.split()[1:] for l in card if l.startswith("process"))
f = ROOT.TFile.Open(os.path.join(OUT, "h.root"), "RECREATE")
tot = np.zeros(len(edges) - 1); rates = []
for p in procs:
    sc, wt = (ev["zx_score"], ev["zx_weight"]) if p == "ZX" else (ev[f"{p}|score"], ev[f"{p}|weight"])
    h, _ = np.histogram(sc, bins=edges, weights=wt)
    h = np.where(h <= 0, 1e-6, h)
    H = ROOT.TH1D(f"h_{p}", "", len(h), 0, len(h))
    for i, v in enumerate(h): H.SetBinContent(i + 1, v)
    H.Write(); rates.append(h.sum())
    if p != "Signal": tot += h
H = ROOT.TH1D("h_data_obs", "", len(tot), 0, len(tot))
for i, v in enumerate(tot): H.SetBinContent(i + 1, v)
H.Write(); f.Close()
L = ["imax 1", f"jmax {len(procs)-1}", "kmax *", "shapes * * h.root h_$PROCESS", "bin hczz", "observation -1",
     "bin " + " ".join(["hczz"] * len(procs)), "process " + " ".join(procs),
     "process " + " ".join(str(i) for i in range(len(procs))), "rate " + " ".join("-1" for _ in procs)]
open(os.path.join(OUT, "card.txt"), "w").write("\n".join(L) + "\n")
print(OUT, len(edges) - 1, "bins")
