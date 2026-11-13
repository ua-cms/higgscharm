"""
m(ZZ)-dependent higher-order k-factors for the ZZ continuum, as event weights.

2026-11-09: switched to the CJLST ZZAnalysis Run-3 (13.6 TeV) tables used by CJLST NanoAnalysis/python/weightFiller.py
(previous version, 13 TeV ggZZ curves + MATRIX m(ZZ)-only qqZZ table: zz_kfactor.py.bak_20261109_before_run3kfac).
Tables: data/zz_kfactor/{ggzz_kfactor_nnlo_run3,qqzz_kfactor_run3}.json (scripts/make_zz_kfactor_run3_json.py).
Interpolation is linear with linear extrapolation, as scipy make_interp_spline(k=1) in CJLST.

  gg->ZZ (MCFM LO, GluGluToContinto*/GluGlutoContinto*/GluGlu2Zto*): NNLO/LO k vs generator m(ZZ) (CJLST
      APPLY_K_NNLOQCD_ZZGG=1). Weight kfactor_ggZZ; variations (Up/Down = 1 +/- relative value, as CJLST):
      kfactor_ggZZ_QCDScale (QCD_mu), kfactor_ggZZ_PDFScale (PDF), kfactor_ggZZ_As (PDF_aS). The card builder
      (HCZZ_ZZ_KFACTOR_WEIGHTS=1) drops its flat 2.27 and uses these shapes.
  qq->ZZ (POWHEG NLO, ZZto4L): k (NNLO QCD x NLO EW / NLO, CJLST APPLY_K_NNLOQCD_NLOEW_ZZQQB) vs generator m(ZZ) in
      4 bins of |cos theta*|. Weight kfactor_qqZZ; variations stored for reference (not used by the card builder):
      kfactor_qqZZ_QCD (QCD_up/QCD_dn over nominal), kfactor_qqZZ_EW, kfactor_qqZZ_smoothing, kfactor_qqZZ_As
      (1 +/- relative value). |cos theta*| bins: [0,0.25) cos3, [0.25,0.5) cos2, [0.5,0.75) cos1, [0.75,1] cos0
      (CJLST intended mapping kfac_m4l__cos{3-i}; their list aliasing bug makes every bin use cos0, not copied).

Generator m(ZZ) = LHE four-lepton mass (CJLST: GenZZ_mass, the four pre-FSR leptons from the Z decays; equal at
generator level). cos theta* = cosine of the polar angle (w.r.t. the beam axis) of Z1 in the 4l rest frame, as MELA
computeDecayAngles on the LHE leptons (CJLST LHEMela_costhetastar); only |cos| is used, so Z1/Z2 order does not
matter. Pairing: 2e2mu (and any mixed flavour) is unique; for 4e/4mu/4tau the OS pairing whose closest-to-mZ pair
is nearest mZ (best-Z1, as MELA).
"""
import json
from pathlib import Path

import awkward as ak
import numpy as np

DATA = Path(__file__).resolve().parent.parent / "data" / "zz_kfactor"
GGZZ_KEYS = ("GluGluToContinto", "GluGlutoContinto", "GluGlu2Zto")
QQZZ_KEYS = ("ZZto4L",)
MZ = 91.1876
COS_BINS = (("cos3", 0.25), ("cos2", 0.5), ("cos1", 0.75), ("cos0", np.inf))  # upper edges in |cos theta*|

_GGZZ = None
_QQZZ = None


def _tables():
    global _GGZZ, _QQZZ
    if _GGZZ is None:
        _GGZZ = json.loads((DATA / "ggzz_kfactor_nnlo_run3.json").read_text())
    if _QQZZ is None:
        _QQZZ = json.loads((DATA / "qqzz_kfactor_run3.json").read_text())
    return _GGZZ, _QQZZ


def interp_linear(x, xp, fp):
    """Piecewise-linear interpolation with linear extrapolation beyond both ends (scipy make_interp_spline k=1)."""
    xp = np.asarray(xp, dtype=float)
    fp = np.asarray(fp, dtype=float)
    x = np.asarray(x, dtype=float)
    out = np.interp(x, xp, fp)
    lo, hi = x < xp[0], x > xp[-1]
    out[lo] = fp[0] + (x[lo] - xp[0]) * (fp[1] - fp[0]) / (xp[1] - xp[0])
    out[hi] = fp[-1] + (x[hi] - xp[-1]) * (fp[-1] - fp[-2]) / (xp[-1] - xp[-2])
    return out


def _p4(lep):
    """(k, 4) arrays px, py, pz, E from pt/eta/phi/mass of exactly 4 leptons per event."""
    pt, eta, phi, m = (ak.to_numpy(ak.flatten(getattr(lep, f))).astype(float).reshape(-1, 4)
                       for f in ("pt", "eta", "phi", "mass"))
    px, py, pz = pt * np.cos(phi), pt * np.sin(phi), pt * np.sinh(eta)
    return px, py, pz, np.sqrt(px ** 2 + py ** 2 + pz ** 2 + m ** 2)


def _mass(px, py, pz, e):
    return np.sqrt(np.maximum(e ** 2 - px ** 2 - py ** 2 - pz ** 2, 0.0))


def lhe_four_lepton(events):
    """
    LHE four-lepton system: (m4l, same-flavour flag, |cos theta*|); -1 for events without exactly 4 LHE leptons
    (outgoing LHEPart e/mu/tau).
    """
    lhe = events.LHEPart
    lep = lhe[(lhe.status == 1) & ((abs(lhe.pdgId) == 11) | (abs(lhe.pdgId) == 13) | (abs(lhe.pdgId) == 15))]
    four = ak.to_numpy(ak.num(lep) == 4)
    n = len(four)
    m4l = np.full(n, -1.0)
    sameflav = np.full(n, -1)
    acos = np.full(n, -1.0)
    if not four.any():
        return m4l, sameflav, acos
    l4 = lep[four]
    px, py, pz, e = _p4(l4)
    pid = ak.to_numpy(ak.flatten(l4.pdgId)).reshape(-1, 4)
    k = len(pid)
    X = (px.sum(1), py.sum(1), pz.sum(1), e.sum(1))
    m4l[four] = _mass(*X)
    apid = np.abs(pid)
    sf = np.all(apid == apid[:, :1], axis=1)
    sameflav[four] = sf.astype(int)

    # candidate pairings (i, j | k, l) of the 4 leptons; a pairing is valid if both pairs are opposite-sign same-flavour
    pairings = ((0, 1, 2, 3), (0, 2, 1, 3), (0, 3, 1, 2))
    best_d = np.full(k, np.inf)
    z1 = np.zeros((k, 4))
    for a, b, c, d in pairings:
        ok = (pid[:, a] == -pid[:, b]) & (pid[:, c] == -pid[:, d])
        za = np.stack([px[:, a] + px[:, b], py[:, a] + py[:, b], pz[:, a] + pz[:, b], e[:, a] + e[:, b]], axis=1)
        zb = np.stack([px[:, c] + px[:, d], py[:, c] + py[:, d], pz[:, c] + pz[:, d], e[:, c] + e[:, d]], axis=1)
        da = np.abs(_mass(*za.T) - MZ)
        db = np.abs(_mass(*zb.T) - MZ)
        dmin = np.minimum(da, db)
        take = ok & (dmin < best_d)
        best_d = np.where(take, dmin, best_d)
        z1 = np.where(take[:, None], np.where((da <= db)[:, None], za, zb), z1)
    has_pair = np.isfinite(best_d)

    # boost Z1 into the 4l rest frame, cos of its polar angle
    bx, by, bz = X[0] / X[3], X[1] / X[3], X[2] / X[3]
    b2 = bx ** 2 + by ** 2 + bz ** 2
    gamma = 1.0 / np.sqrt(np.maximum(1.0 - b2, 1e-300))
    bp = bx * z1[:, 0] + by * z1[:, 1] + bz * z1[:, 2]
    g2 = np.where(b2 > 0, (gamma - 1.0) / np.where(b2 > 0, b2, 1.0), 0.0)
    pxr = z1[:, 0] + g2 * bp * bx - gamma * bx * z1[:, 3]
    pyr = z1[:, 1] + g2 * bp * by - gamma * by * z1[:, 3]
    pzr = z1[:, 2] + g2 * bp * bz - gamma * bz * z1[:, 3]
    pabs = np.sqrt(pxr ** 2 + pyr ** 2 + pzr ** 2)
    c = np.where(has_pair & (pabs > 0), np.abs(pzr) / np.where(pabs > 0, pabs, 1.0), -1.0)
    acos[four] = c
    return m4l, sameflav, acos


def ggzz_kfactor(m):
    """(nominal, {source: relative variation}) for gg->ZZ at generator m(ZZ)."""
    g, _ = _tables()
    nom = interp_linear(m, g["hmass"], g["nominal"])
    rel = {name: interp_linear(m, g["hmass"], g[key]) for name, key in
           (("QCDScale", "QCD_mu"), ("PDFScale", "PDF"), ("As", "PDF_aS"))}
    return nom, rel


def qqzz_kfactor(m, acos):
    """dict of per-event arrays for qq->ZZ: nominal, QCD_up, QCD_dn (absolute), EW/smoothing/As (relative)."""
    _, q = _tables()
    keys = ("nominal", "QCD_up", "QCD_dn", "EW_factor", "smoothing_factor", "alpha_S_factor")
    out = {kk: np.full(len(m), np.nan) for kk in keys}
    lo = -np.inf
    for name, hi in COS_BINS:
        sel = (acos >= lo) & (acos < hi) if lo > -np.inf else (acos < hi)
        sel &= acos >= 0
        t = q["tables"][name]
        for kk in keys:
            out[kk][sel] = interp_linear(m[sel], t["zzmass"], t[kk])
        lo = hi
    return out


def add_zz_kfactor_weights(events, weights_container, dataset, variation="nominal"):
    """Adds kfactor_ggZZ or kfactor_qqZZ (+ variations) to the weights; no-op for other samples."""
    is_gg = any(k in dataset for k in GGZZ_KEYS)
    is_qq = (not is_gg) and any(k in dataset for k in QQZZ_KEYS) and "GluGluH" not in dataset
    if not (is_gg or is_qq):
        return
    if "LHEPart" not in events.fields:
        raise KeyError(f"zz_kfactor: {dataset} has no LHEPart, cannot compute the generator m(ZZ)")
    m, sameflav, acos = lhe_four_lepton(events)
    if np.any(m < 0):
        raise ValueError(f"zz_kfactor: {np.sum(m < 0)} events of {dataset} without 4 LHE leptons")
    one = np.ones_like(m)
    if is_gg:
        k, rel = ggzz_kfactor(m)
        weights_container.add("kfactor_ggZZ", k)
        if variation == "nominal":
            for src, r in rel.items():
                weights_container.add(f"kfactor_ggZZ_{src}", one, weightUp=1.0 + r, weightDown=1.0 - r)
    else:
        if np.any(acos < 0):
            raise ValueError(f"zz_kfactor: {np.sum(acos < 0)} events of {dataset} without an OSSF pairing of the LHE leptons")
        t = qqzz_kfactor(m, acos)
        weights_container.add("kfactor_qqZZ", t["nominal"])
        if variation == "nominal":
            weights_container.add("kfactor_qqZZ_QCD", one, weightUp=t["QCD_up"] / t["nominal"],
                                  weightDown=t["QCD_dn"] / t["nominal"])
            for src, key in (("EW", "EW_factor"), ("smoothing", "smoothing_factor"), ("As", "alpha_S_factor")):
                weights_container.add(f"kfactor_qqZZ_{src}", one, weightUp=1.0 + t[key], weightDown=1.0 - t[key])
