#!/usr/bin/env python3
"""Cross-check of the unbinned fit (2026-10-18): a BINNED card whose templates are the
unbinned fit's smooth pdfs (pdfs.json) integrated in N equal bins of u, times the card rate
(times kappa for the systematic variations). Same rows as the binned card, no autoMCStats.
  N = 20  -> isolates the effect of smoothing (vs the binned card without autoMCStats);
  N large -> should converge to the unbinned result (isolates the effect of binning).
Optionally keep the binned card's own Z+X template (--zx-binned, N = 20 only).
Usage: python3 make_binned_smooth.py <pdfs.json> <binned card dir> <out dir> <N> [--zx-binned]
"""
import json
import os
import sys
from math import comb

import numpy as np
import ROOT

PJ, CARD_DIR, OUT, N = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
ZX_BINNED = "--zx-binned" in sys.argv
P = json.load(open(PJ))
os.makedirs(OUT, exist_ok=True)
FINE = 200 * N
x = (np.arange(FINE) + 0.5) / FINE


def bins_of(spec):
    n, c = spec["order"], np.array(spec["coef"])
    i = np.arange(n + 1)
    b = np.array([comb(n, k) for k in i])[None, :] * x[:, None] ** i * (1 - x[:, None]) ** (n - i)
    d = b @ c
    return d.reshape(N, 200).sum(axis=1) / FINE


card = open(os.path.join(CARD_DIR, "datacard_mva_with_zx.txt")).read().splitlines()
fin = ROOT.TFile.Open(os.path.join(CARD_DIR, "histograms_mva_with_zx.root"))
f = ROOT.TFile.Open(os.path.join(OUT, "histograms_smooth.root"), "RECREATE")
rates = P["rates"]
tot = np.zeros(N)


def write(name, vals):
    h = ROOT.TH1D(name, name, N, 0.0, 1.0)
    for k, v in enumerate(vals):
        h.SetBinContent(k + 1, v)
        h.SetBinError(k + 1, 0.0)
    h.Write()


for name, spec in P["pdfs"].items():
    proc = name if name in rates else next(p for p in rates if name.startswith(p + "_"))
    if ZX_BINNED and proc == "ZX":
        h = fin.Get(f"h_{name}")
        h.SetDirectory(f)
        f.cd()
        h.Write(f"h_{name}")
        if name == "ZX":
            tot += np.array([h.GetBinContent(k + 1) for k in range(N)])
        continue
    if name == proc:
        k = 1.0
    else:
        syst, d = name[len(proc) + 1:-2], name[-2:]
        if d != "Up":
            syst, d = name[len(proc) + 1:-4], "Down"
        k = P["kappa"][proc][syst][d]
    v = rates[proc] * k * bins_of(spec)
    write(f"h_{name}", v)
    if name == proc and proc != "Signal":
        tot += v
write("h_data_obs", tot)
f.Close()
L = [l.replace("histograms_mva_with_zx.root", "histograms_smooth.root") for l in card if "autoMCStats" not in l]
open(os.path.join(OUT, "datacard_smooth.txt"), "w").write("\n".join(L) + "\n")
print(f"{OUT}: N = {N}, ZX binned {ZX_BINNED}, background {tot.sum():.3f}")
