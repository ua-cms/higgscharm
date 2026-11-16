#!/usr/bin/env python3
"""2026-11-08: copy a 2024 card (shapes h_$PROCESS h_$PROCESS_$SYSTEMATIC) and suffix the per-year
experimental nuisances with _2024, so combineCards with the 2022/23 card does NOT correlate them.
Theory (QCDscale/pdf/scalevar/ps/BR/kfactor/higgs_plus_c/lhe) and lumi_Run3 stay correlated.
usage: decorrelate_2024.py <in card dir> <datacard> <out dir>"""
import sys, os, re, shutil, ROOT
EXP = ["CMS_pileup", "CMS_scale_j", "CMS_res_j", "CMS_eff_m_id", "CMS_eff_e_id", "CMS_eff_e_reco_20to75",
       "CMS_eff_e_reco_above75", "CMS_eff_e_reco_below20", "CMS_scale_m", "CMS_res_m", "CMS_ctag2d_resid"]
src, card, out = sys.argv[1:4]
os.makedirs(out, exist_ok=True)
lines = open(os.path.join(src, card)).read().split("\n")
rootf = [l.split()[3] for l in lines if l.startswith("shapes")][0]
present = {l.split()[0] for l in lines if len(l.split()) > 1 and l.split()[1] in ("shape", "lnN")}
ren = {n: n + "_2024" for n in EXP if n in present}
newl = []
for l in lines:
    t = l.split()
    if t and t[0] in ren:
        l = ren[t[0]] + l[len(t[0]):]
    newl.append(l)
open(os.path.join(out, card), "w").write("\n".join(newl))
fi = ROOT.TFile.Open(os.path.join(src, rootf)); fo = ROOT.TFile.Open(os.path.join(out, rootf), "RECREATE")
n_ren = 0
for k in fi.GetListOfKeys():
    name = k.GetName(); obj = k.ReadObj(); new = name
    for o, nn in ren.items():
        m = re.match(rf"^(h_.+)_{o}(Up|Down)$", name)
        if m:
            new = f"{m.group(1)}_{nn}{m.group(2)}"; n_ren += 1; break
    fo.cd(); obj.SetName(new); obj.Write(new)
fo.Close(); fi.Close()
print(f"renamed nuisances {sorted(ren)} ; {n_ren} histograms")
