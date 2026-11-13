#!/usr/bin/env python3
"""
2026-11-03: convert the CJLST ZZAnalysis Run-3 HZZ lepton scale factors (ROOT TH2) to correctionlib JSON.

The HZZ selection (muons: loose + SIP < 4 + PF/highPt, no isolation; electrons: HZZ BDT `mvaHZZIso` + SIP < 4)
has no POG/EGM scale factors; HIG-24-013 uses these HZZ-measured ones. Files and logic follow
CJLST/ZZAnalysis (branch Run3) AnalysisStep/src/LeptonSFHelper.cc:
  - muons: TH2 "FINAL" (SF) and "ERROR" (absolute uncertainty) in (eta, pT), pT clamped to 199 (last bin = overflow)
  - electrons (selection = ID+SIP; the reco SF stays the EGM one): TH2 "EGamma_SF2D", SF = content,
    uncertainty = bin error, in (SC eta, pT), pT clamped to 499
      2023: electrons in the ECAL gap (1.444 < |SC eta| < 1.556) with pT < 20 use the gap map
            (SF2023eleID_Gap_preBPix.root for both 2023 eras); postBPix: gap uncertainty x 2
      2023postBPix: BPix hole (-1.5 < SC eta < 0, -1.2 < phi < -0.8) uses the hole map
      2024 (added 2026-11-09): main + gap map (SF2024eleID_Gap.root, pT < 20, uncertainty not scaled), no hole;
            muons HZZ_HZZ_SF_2024_RMS_mupogsystsC.root (cut-based, not the Muon_LowPtMVA one); the electron
            SFs are for the NanoAOD v15 bool Electron_mvaIso_WPHZZ (working_points.electron_id with year)
The (region) choice is made in analysis/corrections/hzz_lepton.py; this file stores one map per region.

usage: python3 scripts/make_hzz_lepton_sf_json.py <CJLST LeptonEffScaleFactors dir> [out dir] [era,era,...]
"""
import hashlib
import json
import os
import sys

import ROOT

SRC = sys.argv[1]
ONLY = sys.argv[3].split(",") if len(sys.argv) > 3 else None
OUT = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] else os.path.join(os.path.dirname(__file__), "..", "analysis", "data", "lepton_hzz")

ERAS = {
    "2022preEE": {"mu": "final_HZZ_SF_Run3_2022_mupogsysts_newLoose_abseta3_fix_BCD_RMS.root",
                  "ele": "SF2022eleID_preEE.root"},
    "2022postEE": {"mu": "final_HZZ_SF_Run3_2022_mupogsysts_newLoose_abseta3_fix_EFG_RMS.root",
                   "ele": "SF2022eleID_postEE.root"},
    "2023preBPix": {"mu": "final_HZZ_SF_2023C_RMS_mupogsysts.root",
                    "ele": "SF2023eleID_preBPix.root", "ele_gap": "SF2023eleID_Gap_preBPix.root"},
    "2023postBPix": {"mu": "final_HZZ_SF_2023D_RMS_mupogsysts.root",
                     "ele": "SF2023eleID_postBPix.root", "ele_gap": "SF2023eleID_Gap_preBPix.root",
                     "ele_hole": "SF2023eleID_postBPix_Hole.root", "gap_unc_scale": 2.0},
    "2024": {"mu": "HZZ_HZZ_SF_2024_RMS_mupogsystsC.root",
             "ele": "SF2024eleID.root", "ele_gap": "SF2024eleID_Gap.root"},
}


def md5(path):
    return hashlib.md5(open(path, "rb").read()).hexdigest()


def edges(axis):
    return [axis.GetBinLowEdge(i) for i in range(1, axis.GetNbins() + 2)]


def read_th2(path, sf_name, err_name=None, err_scale=1.0):
    """(x edges, y edges, sf[x][y], unc[x][y]); unc = bin error of sf_name, or content of err_name."""
    f = ROOT.TFile.Open(path)
    h = f.Get(sf_name)
    he = f.Get(err_name) if err_name else None
    xe, ye = edges(h.GetXaxis()), edges(h.GetYaxis())
    sf, unc = [], []
    for i in range(1, h.GetNbinsX() + 1):
        for j in range(1, h.GetNbinsY() + 1):
            s = h.GetBinContent(i, j)
            e = he.GetBinContent(i, j) if he else h.GetBinError(i, j)
            if not s > 0:
                raise ValueError(f"{path}:{sf_name} bin ({i},{j}) SF {s}")
            sf.append(s)
            unc.append(e * err_scale)
    f.Close()
    return xe, ye, sf, unc


def multibinning(xvar, yvar, xe, ye, values):
    # correctionlib MultiBinning: content is row-major in the order of `inputs` (x outer, y inner)
    return {"nodetype": "multibinning", "inputs": [xvar, yvar], "edges": [xe, ye],
            "content": values, "flow": "clamp"}


def syst_category(xvar, yvar, xe, ye, sf, unc):
    return {"nodetype": "category", "input": "syst", "content": [
        {"key": "nominal", "value": multibinning(xvar, yvar, xe, ye, sf)},
        {"key": "up", "value": multibinning(xvar, yvar, xe, ye, [s + u for s, u in zip(sf, unc)])},
        {"key": "down", "value": multibinning(xvar, yvar, xe, ye, [s - u for s, u in zip(sf, unc)])},
    ]}


def main():
    os.makedirs(OUT, exist_ok=True)
    for era, files in ERAS.items():
        if ONLY and era not in ONLY:
            continue
        src_md5 = {k: md5(os.path.join(SRC, v)) for k, v in files.items() if isinstance(v, str)}
        xe, ye, sf, unc = read_th2(os.path.join(SRC, files["mu"]), "FINAL", "ERROR")
        mu = {"name": "HZZ-Muon-SF", "version": 1,
              "description": f"HZZ muon selection SF ({files['mu']}, CJLST ZZAnalysis Run3); "
                             "uncertainty = ERROR map; eta signed, pT clamped",
              "inputs": [{"name": "syst", "type": "string", "description": "nominal/up/down"},
                         {"name": "eta", "type": "real"}, {"name": "pt", "type": "real"}],
              "output": {"name": "sf", "type": "real"},
              "data": syst_category("eta", "pt", xe, ye, sf, unc)}

        regions = [("main", files["ele"], 1.0)]
        if "ele_gap" in files:
            regions.append(("gap", files["ele_gap"], files.get("gap_unc_scale", 1.0)))
        if "ele_hole" in files:
            regions.append(("hole", files["ele_hole"], 1.0))
        reg_content = []
        for key, fname, scale in regions:
            xe, ye, sf, unc = read_th2(os.path.join(SRC, fname), "EGamma_SF2D", err_scale=scale)
            reg_content.append({"key": key, "value": syst_category("sceta", "pt", xe, ye, sf, unc)})
        ele = {"name": "HZZ-Electron-ID-SF", "version": 1,
               "description": "HZZ electron selection (BDT ID + SIP) SF, CJLST ZZAnalysis Run3; "
                              f"regions {[r[0] for r in regions]} ({', '.join(r[1] for r in regions)}); "
                              "uncertainty = EGamma_SF2D bin error (x2 for the postBPix gap); SC eta signed, pT clamped",
               "inputs": [{"name": "region", "type": "string", "description": "main/gap/hole"},
                          {"name": "syst", "type": "string", "description": "nominal/up/down"},
                          {"name": "sceta", "type": "real"}, {"name": "pt", "type": "real"}],
               "output": {"name": "sf", "type": "real"},
               "data": {"nodetype": "category", "input": "region", "content": reg_content}}

        cset = {"schema_version": 2,
                "description": f"HZZ lepton SFs {era}; sources (CJLST ZZAnalysis Run3 "
                               f"AnalysisStep/data/LeptonEffScaleFactors) md5: {json.dumps(src_md5)}",
                "corrections": [mu, ele]}
        out = os.path.join(OUT, f"hzz_lepton_sf_{era}.json")
        json.dump(cset, open(out, "w"), indent=1)
        print(f"{era}: {out}  {src_md5}")


if __name__ == "__main__":
    main()
