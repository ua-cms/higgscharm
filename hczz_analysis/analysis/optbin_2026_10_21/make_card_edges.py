#!/usr/bin/env python3
"""Stat-only binned card (+ autoMCStats 10) from the event dump with explicit p_sig edges.
Usage: python3 make_card_edges.py <events.npz> <out dir> <edges comma-sep>"""
import os, sys, numpy as np, ROOT
NPZ, OUT, E = sys.argv[1:4]
ev = np.load(NPZ); edges = np.array([float(x) for x in E.split(",")])
procs = ["Signal", "ggZZ", "qqZZ", "ggH", "VBF", "WminusH", "WplusH", "ZH", "ttH", "HPlusB", "ZX"]
os.makedirs(OUT, exist_ok=True)
f = ROOT.TFile.Open(os.path.join(OUT, "histograms_opt.root"), "RECREATE")
n = len(edges) - 1; tot = np.zeros(n)
for p in procs:
    sc, wt = (ev["zx_score"], ev["zx_weight"]) if p == "ZX" else (ev[p + "|score"], ev[p + "|weight"])
    h = np.histogram(sc, bins=edges, weights=wt)[0]; h2 = np.histogram(sc, bins=edges, weights=wt ** 2)[0]
    H = ROOT.TH1D(f"h_{p}", "", n, 0, n); H.Sumw2()
    for i in range(n):
        H.SetBinContent(i + 1, max(h[i], 1e-6)); H.SetBinError(i + 1, np.sqrt(h2[i]))
    H.Write()
    if p != "Signal": tot += np.maximum(h, 1e-6)
H = ROOT.TH1D("h_data_obs", "", n, 0, n)
for i in range(n): H.SetBinContent(i + 1, tot[i])
H.Write(); f.Close()
L = ["imax 1", f"jmax {len(procs)-1}", "kmax *", "shapes * * histograms_opt.root h_$PROCESS", "bin hczz", "observation -1",
     "bin " + " ".join(["hczz"] * len(procs)), "process " + " ".join(procs),
     "process " + " ".join(str(i) for i in range(len(procs))), "rate " + " ".join("-1" for _ in procs),
     "hczz autoMCStats 10"]
open(os.path.join(OUT, "datacard.txt"), "w").write("\n".join(L) + "\n")
print(OUT, n, "bins")
