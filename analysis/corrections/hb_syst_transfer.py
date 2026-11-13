"""
2026-11-09: scale / PDF / alpha_s weights for the OLD-gridpack private H+b -> ZZ -> 4l samples, transferred from the
official H+b -> gamma gamma 5FS FxFx NanoAOD.

Why: the old private H+b gridpack (md5 0390716d, Apr 2026) was built with reweight_scale/reweight_PDF False, so its
LHEScaleWeight/LHEPdfWeight are unusable (all scale points 1.4-2.3 in the same direction, PDF members ~1.85 x member 0);
they were excluded in the card builder (HPLUSBOTTOM_BUGGY_SYST). Production-level weights factorise from the narrow
Higgs decay and our 5FS FxFx cards equal the official H+b -> gg ones apart from the decay (validated 2026-10-13: 112
weights within 1.75 sigma); gen-level jet-class / Higgs-pT composition of old H+b -> ZZ and official H+b -> gg agree.

Map: Analysis/hb_syst_transfer_2026_11_09/make_hb_syst_map.py -> analysis/data/hb_syst_transfer/hb_syst_map_gg5fsfxfx.json
(sign(genWeight)-weighted sums per (Higgs pT bin, jet class); jet class from GenJets pT>20 |eta|<2.5 cleaned dR>0.4
against hard-process photons/leptons: 0 none, 1 light, 2 >=1 c no b, 3 >=1 b). Ratio per bin = sum(w r)/sum(w);
bins with N_eff < MIN_NEFF fall back to all Higgs pT of the same jet class, then to the inclusive ratio.
Same names/conventions as lhescale.py / lhepdf.py: scalevar_muR [7]/[1], scalevar_muF [5]/[3], lhe_pdf (per-event
Hessian quadrature over 325300 members 1-100), lhe_alphaS 0.5*([102]-[101]); scalevar_muR_muF is set to nominal.
Only for is_old_hplusbottom() datasets; the fixed 4FS samples (HPlusBottom4FS_*) keep their own LHE weights.
"""
import re
import json
import importlib.resources
import numpy as np
import awkward as ak

MIN_NEFF = 2000.0
_TABLE = None


def is_old_hplusbottom(dataset):
    """Same rule as combine/scripts/create_datacards_ctag2d_100150.py is_old_hplusbottom()."""
    base = re.sub(r"_\d+$", "", dataset)
    return bool(re.match(r"^HPlusBottom(_new)?_\d{4}", base) or re.match(r"^\d{4}\w*HB$", base))


def _table():
    global _TABLE
    if _TABLE is None:
        with importlib.resources.open_text("analysis.data.hb_syst_transfer", "hb_syst_map_gg5fsfxfx.json") as f:
            d = json.load(f)
        sw, sw2 = np.array(d["sumw"]), np.array(d["sumw2"])
        neff = np.divide(sw ** 2, sw2, out=np.zeros_like(sw), where=sw2 > 0)
        cls_sw, cls_sw2 = sw.sum(0), sw2.sum(0)
        cls_neff = np.divide(cls_sw ** 2, cls_sw2, out=np.zeros_like(cls_sw), where=cls_sw2 > 0)
        ratios = {}
        for v in d["variations"]:
            s = np.array(d[v])
            r_bin = np.divide(s, sw, out=np.ones_like(s), where=sw != 0)
            r_cls = np.divide(s.sum(0), cls_sw, out=np.ones_like(cls_sw), where=cls_sw != 0)
            r_cls = np.where(cls_neff >= MIN_NEFF, r_cls, s.sum() / sw.sum())
            ratios[v] = np.where(neff >= MIN_NEFF, r_bin, r_cls[None, :])
        _TABLE = {"hpt_edges": np.array(d["hpt_edges"]), "ratios": ratios}
    return _TABLE


def gen_bins(events):
    """(Higgs-pT bin, jet class) per event; identical to make_hb_syst_map.gen_bins on NanoAOD branches."""
    gp, gj = events.GenPart, events.GenJet
    last = (gp.statusFlags & (1 << 13)) != 0
    hard = (gp.statusFlags & (1 << 8)) != 0
    apdg = abs(gp.pdgId)
    hpt = ak.fill_none(ak.firsts(gp.pt[(gp.pdgId == 25) & last]), -1.0)
    dec = gp[hard & last & ((apdg == 22) | (apdg == 11) | (apdg == 13) | (apdg == 15))]
    jets = gj[(gj.pt > 20) & (abs(gj.eta) < 2.5)]
    pe = ak.cartesian([jets.eta, dec.eta], nested=True)
    pp = ak.cartesian([jets.phi, dec.phi], nested=True)
    dphi = (pp["0"] - pp["1"] + np.pi) % (2 * np.pi) - np.pi
    clean = ak.fill_none(ak.all((pe["0"] - pe["1"]) ** 2 + dphi ** 2 > 0.16, axis=2), True)
    fl = jets.hadronFlavour[clean]
    nj = ak.to_numpy(ak.num(fl))
    hasb = ak.to_numpy(ak.any(fl == 5, axis=1))
    hasc = ak.to_numpy(ak.any(fl == 4, axis=1))
    jc = np.where(nj == 0, 0, np.where(hasb, 3, np.where(hasc, 2, 1)))
    edges = _table()["hpt_edges"]
    hb = np.clip(np.digitize(ak.to_numpy(hpt), edges) - 1, 0, len(edges) - 2)
    return hb, jc


def add_hb_syst_transfer_weights(events, weights_container):
    """Adds scalevar_muR, scalevar_muF, scalevar_muR_muF (nominal), lhe_pdf, lhe_alphaS, lhe_pdf_alphaS from the map."""
    hb, jc = gen_bins(events)
    r = {k: v[hb, jc] for k, v in _table()["ratios"].items()}
    one = np.ones(len(hb))
    weights_container.add("scalevar_muR", one, r["scalevar_muRUp"], r["scalevar_muRDown"])
    weights_container.add("scalevar_muF", one, r["scalevar_muFUp"], r["scalevar_muFDown"])
    weights_container.add("scalevar_muR_muF", one, one, one)
    weights_container.add("lhe_pdf", weight=one, weightUp=r["lhe_pdfUp"], weightDown=r["lhe_pdfDown"])
    weights_container.add("lhe_alphaS", weight=one, weightUp=r["lhe_alphaSUp"], weightDown=r["lhe_alphaSDown"])
    tot = np.sqrt((r["lhe_pdfUp"] - 1) ** 2 + (r["lhe_alphaSUp"] - 1) ** 2)   # as lhepdf.py delta_total
    weights_container.add("lhe_pdf_alphaS", weight=one, weightUp=1 + tot, weightDown=1 - tot)
