#!/usr/bin/env python3
"""Step 2 of the unbinned MVA-score fit (2026-10-18), run in the CMSSW combine env:
RooWorkspace + datacard from pdfs.json (make_pdfs.py).

* observable u in [0,1] (qqZZ CDF of mva_score_Signal);
* pdf_<proc> and pdf_<proc>_<syst>{Up,Down}: RooBernstein with constant coefficients.
  Combine morphs them with VerticalInterpPdf for the card's `shape` rows (shape only, as
  pdfs carry no normalisation);
* pdf_<proc>_norm: ProcessNormalization with the card's asymmetric integral ratio
  (kappa Down/Up) of every shape systematic, so the rate effect of the binned card's
  templates is kept; the card's lnN rows are copied unchanged;
* data_obs: weighted RooDataSet = pre-fit total background on 2000 points (an Asimov-type
  stand-in; all limits are --run blind / -t -1 as for the binned card);
* no autoMCStats (no unbinned equivalent): compare with the binned card without it.
Usage: python3 make_ws.py <pdfs.json> <binned card dir> <out dir> [--stat-only]
"""
import json
import os
import sys

import ROOT

ROOT.gSystem.Load("libHiggsAnalysisCombinedLimit")
ROOT.RooMsgService.instance().setGlobalKillBelow(ROOT.RooFit.WARNING)

PJ, CARD_DIR, OUT = sys.argv[1:4]
STAT_ONLY = "--stat-only" in sys.argv
P = json.load(open(PJ))
os.makedirs(OUT, exist_ok=True)

w = ROOT.RooWorkspace("w", "w")
u = ROOT.RooRealVar("u", "qqZZ CDF of P(H+c)", 0.0, 1.0)
getattr(w, "import")(u)
u = w.var("u")
keep = []


def bern(name, spec):
    cl = ROOT.RooArgList()
    for i, c in enumerate(spec["coef"]):
        v = ROOT.RooRealVar(f"{name}_c{i}", "", c)
        v.setConstant(True)
        keep.append(v)
        cl.add(v)
    pdf = ROOT.RooBernstein(name, name, u, cl)
    # ROOT 6.30 RooBernstein analytic integral is numerically wrong above order ~22 (checked
    # 2026-10-18; evaluation is exact). Coefficients are constant, so the numeric norm is cached.
    pdf.forceNumInt(True)
    getattr(w, "import")(pdf, ROOT.RooFit.RecycleConflictNodes(), ROOT.RooFit.Silence())


card = open(os.path.join(CARD_DIR, "datacard_mva_with_zx.txt")).read().splitlines()
procs, lnn, shapes = None, [], []
for l in card:
    t = l.split()
    if not t:
        continue
    if t[0] == "process" and procs is None:
        procs = t[1:]
    elif len(t) > 2 and t[1] == "lnN":
        lnn.append(l)
    elif len(t) > 2 and t[1] == "shape":
        shapes.append((t[0], t[2:]))
rates = P["rates"]

for name, spec in P["pdfs"].items():
    if STAT_ONLY and name not in procs:
        continue
    bern(f"pdf_{name}", spec)

for proc in procs:
    norm = ROOT.ProcessNormalization(f"pdf_{proc}_norm", "", 1.0)
    if not STAT_ONLY:
        for syst, kd in P["kappa"].get(proc, {}).items():
            th = w.var(syst) or ROOT.RooRealVar(syst, syst, 0.0, -7.0, 7.0)
            keep.append(th)
            # ProcessNormalization asym: kappaLo applies at theta = -1, kappaHi at +1
            norm.addAsymmLogNormal(kd["Down"], kd["Up"], th)
    getattr(w, "import")(norm, ROOT.RooFit.RecycleConflictNodes(), ROOT.RooFit.Silence())

# data_obs: pre-fit background expectation as a weighted dataset in u
wv = ROOT.RooRealVar("wgt", "", 1.0)
ds = ROOT.RooDataSet("data_obs", "", ROOT.RooArgSet(u, wv), ROOT.RooFit.WeightVar("wgt"))
NP = 2000
for i in range(NP):
    x = (i + 0.5) / NP
    u.setVal(x)
    tot = 0.0
    for proc in procs:
        if proc == "Signal":
            continue
        tot += rates[proc] * w.pdf(f"pdf_{proc}").getVal(ROOT.RooArgSet(u)) / NP
    ds.add(ROOT.RooArgSet(u), tot)
getattr(w, "import")(ds)
ws_file = os.path.join(OUT, "ws_unbinned.root")
w.writeToFile(ws_file)
print(f"workspace -> {ws_file}: {len(P['pdfs'])} pdfs, data_obs sumw {ds.sumEntries():.3f}")

L = ["# H+c -> ZZ -> 4l: UNBINNED fit in u = qqZZ CDF of mva_score_Signal (2026-10-18)",
     f"# from {CARD_DIR}", "imax 1", f"jmax {len(procs) - 1}", "kmax *", "-" * 40,
     "shapes * * ws_unbinned.root w:pdf_$PROCESS w:pdf_$PROCESS_$SYSTEMATIC",
     "shapes data_obs * ws_unbinned.root w:data_obs", "-" * 40,
     "bin hczz", "observation -1", "-" * 40,
     "bin " + " ".join(["hczz"] * len(procs)),
     "process " + " ".join(procs),
     "process " + " ".join(str(i) for i in range(len(procs))),
     "rate " + " ".join(f"{rates[p]:.6f}" for p in procs), "-" * 40]
if not STAT_ONLY:
    L += lnn
    L += [f"{s} shape " + " ".join(v) for s, v in shapes]
open(os.path.join(OUT, "datacard_unbinned.txt"), "w").write("\n".join(L) + "\n")
print(f"datacard -> {OUT}/datacard_unbinned.txt ({len(lnn)} lnN, {0 if STAT_ONLY else len(shapes)} shape)")
