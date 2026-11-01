#!/usr/bin/env python3
"""2026-10-30: final-state-split ATLAS-style (m4l, u) card: three Combine channels (4e, 4mu, 2e2mu).

Per final state c and process p (pdf on a 220 (0.25 GeV) x 200 (u) grid, then integrated in the chosen binning):
  * Higgs processes (ATLAS signal model, HIG-24-013 non-resonant part):
      P = f_{p,c}(u) x [ (1 - fL_{p,c}) DSCB_c(m4l; mu_c(u), sigma_c(u), tails_c) + fL_{p,c} Landau_{p,c}(m4l) ]
    f_{p,c}(u): marginal in u of the process's 2D KDE in that channel; DSCB_c from dscb_ch.py (quadratic in u);
  * qqZZ, ggZZ: 2D Gaussian KDE per channel (ATLAS: backgrounds from MC with Gaussian kernels);
  * Z+X: the CR has n_eff ~ 64 in total, so its (m4l, u) shape is the inclusive 2D KDE; per-channel yield from the CR.
Rates: sum of the dump weights per channel (the dump weights = the card's; totals checked against the cat40 card).
Systematics (ours; the cat40 card's list of rows):
  * lnN rows: copied, same nuisance in all channels;
  * shape rows with per-event weights in the dump (|syst|): per channel and cat40 cell, sum(w_var)/sum(w_nom),
    applied to the pdf on the grid cells inside the cell (cells with n_eff < 20 in a channel use the all-channel
    ratio of that cell); includes the rate effect;
  * shape rows from shifted samples (|jec|: CMS_scale_j, CMS_res_j, CMS_scale_m, CMS_res_m): same, from the
    shifted events (per channel and cell); EXCEPT CMS_scale_m / CMS_res_m on Higgs processes: DSCB mean/sigma
    shifted by the per-channel relative shifts measured on the shifted samples (dscb_ch.py) and the channel rate
    ratio (a cat40 cell cannot resolve a 0.05% mass shift);
  * rows without event-level variations in the dump: the cat40 card's inclusive cell ratio (as before).
  No electron scale/resolution (not in our production).
Usage: python3 make_card_ch.py <events_2d_ch.npz> <pdfs_2d.npz (u)> <dscb_ch.json> <cat40 card dir> <out> --binning fine:55x40
"""
import argparse
import json
import os
import re

import numpy as np
import uproot
from scipy.stats import landau

ap = argparse.ArgumentParser()
ap.add_argument("events"); ap.add_argument("pdfs"); ap.add_argument("dscb"); ap.add_argument("card_dir"); ap.add_argument("out")
ap.add_argument("--binning", default="fine:55x40")
a = ap.parse_args()

FINE = np.round(np.arange(105.0, 160.0 + 1e-9, 0.25), 4)
NX, NY = len(FINE) - 1, 200
XE, YE = FINE, np.linspace(0, 1, NY + 1)
XC, YC = 0.5 * (XE[1:] + XE[:-1]), 0.5 * (YE[1:] + YE[:-1])
DX, DY = 0.25, 1.0 / NY
GX = np.linspace(105, 160, NX * 8 + 1); GXC = 0.5 * (GX[1:] + GX[:-1])
CH = ["4e", "4mu", "2e2mu"]
HIGGS = ["Signal", "ggH", "VBF", "WminusH", "WplusH", "ZH", "ttH", "HPlusB"]
MUSYST = ["CMS_scale_m", "CMS_res_m", "CMS_scale_e", "CMS_res_e"]
# 2026-10-30: only lepton-flavour sources get per-channel ratios; every other shape source (theory, PS, pileup,
# ctag2d, JES/JER, ...) uses the all-channel ratio of the cell in every channel. Per-channel ratios from ~1/3 of
# the MC fluctuated and cost ~5% in the expected limit (diag_split / diag_stat: stat-only split 2% better than
# inclusive, split worse only once these shapes were added).
LEPTON_FLAVOUR = re.compile(r"^CMS_(eff_e|eff_m|scale_e|res_e|scale_m|res_m)")

ev = np.load(a.events, allow_pickle=False)
PU = np.load(a.pdfs, allow_pickle=False)
us, uc = PU["u_s"], PU["u_c"]
DJ = json.load(open(a.dscb))["channels"]
to_u = lambda s: np.clip(np.interp(s, us, uc), 0, 1)
from_u = lambda u: np.interp(u, uc, us)

# --- cat40 layout + card
lay = open(os.path.join(a.card_dir, "layout.txt")).read()
cats = np.array([float(x) for x in re.search(r"--unroll-score-cats (\S+)", lay).group(1).strip("[]").split(",")])
edges = np.array([float(x) for x in re.search(r"bin-edges (\S+)", lay).group(1).strip("[]").split(",")])
NC, NCELL = len(cats) - 1, len(edges) - 1


def cell_of(m4l, s):
    j = np.clip(np.searchsorted(cats, s, side="right") - 1, 0, NC - 1)
    return np.clip(np.searchsorted(edges, (j + (m4l - 105.0) / 55.0) / NC, side="right") - 1, 0, NCELL - 1)


GRIDCELL = cell_of(XC[:, None], from_u(YC)[None, :])
card = open(os.path.join(a.card_dir, "datacard_mva_with_zx.txt")).read().splitlines()
pnames = None; lnn = []; shapes = []
for l in card:
    t = l.split()
    if not t:
        continue
    if t[0] == "process" and pnames is None:
        pnames = t[1:]
    elif t[0] == "rate":
        card_rate = dict(zip(pnames, (float(x) for x in t[1:])))
    elif len(t) > 2 and t[1] == "lnN":
        lnn.append((t[0], t[2:]))
    elif len(t) > 2 and t[1] == "shape":
        shapes.append((t[0], [p for p, v in zip(pnames, t[2:]) if v != "-"]))
HC = uproot.open(os.path.join(a.card_dir, "histograms_mva_with_zx.root"))
# electron scale/resolution (2026-10-30 production): rows added when the dump has the shifted samples but the
# cat40 card predates them
for _sy in ("CMS_scale_e", "CMS_res_e"):
    if _sy not in [x for x, _ in shapes]:
        _pl = [p for p in pnames if f"{p}|jec|{_sy}|Up|score" in ev.files]
        if _pl:
            shapes.append((_sy, _pl))


def decode(v):
    k = np.floor(v * 3 * NX).astype(int)
    ok = (v >= 0) & (k >= 0) & (k < 3 * NX)
    k = np.clip(k, 0, 3 * NX - 1)
    return k // NX, k % NX, v * 3 * NX - k, ok


def load(key_s, key_w, extra=()):
    c, i, s, ok = decode(ev[key_s])
    out = {"c": c[ok], "i": i[ok], "s": s[ok], "w": ev[key_w][ok]}
    for k in extra:
        out[k] = ev[k][ok]
    return out


# --- KDE (as make_pdfs_2d.py)
def wquant(x, w, q):
    o = np.argsort(x); c = np.cumsum(w[o])
    return np.interp(q * c[-1], c, x[o])


def smooth_axis(h, bw_cells, axis):
    n = h.shape[axis]; k = int(np.ceil(5 * bw_cells))
    if k < 1:
        return h
    x = np.arange(-k, k + 1); g = np.exp(-0.5 * (x / bw_cells) ** 2); g /= g.sum()
    h = np.moveaxis(h, axis, 0); pad = min(k, n)
    ext = np.concatenate([h[:pad][::-1], h, h[-pad:][::-1]], axis=0)
    o = np.apply_along_axis(lambda c: np.convolve(c, g, mode="same"), 0, ext)[pad:pad + n]
    return np.moveaxis(o, 0, axis)


def kde2d(mx, uy, w):
    aw = np.abs(w); neff = w.sum() ** 2 / (w ** 2).sum(); bws = []
    for arr, d in ((mx, DX), (uy, DY)):
        mu = np.average(arr, weights=aw); sd = np.sqrt(np.average((arr - mu) ** 2, weights=aw))
        iqr = wquant(arr, aw, 0.75) - wquant(arr, aw, 0.25)
        bws.append(max(0.9 * min(sd, iqr / 1.34) * neff ** (-1.0 / 6.0), d))
    h, _, _ = np.histogram2d(mx, uy, bins=[XE, YE], weights=w)
    sm = np.clip(smooth_axis(smooth_axis(h, bws[0] / DX, 0), bws[1] / DY, 1), 0, None)
    return sm / (sm.sum() * DX * DY), bws, neff


def dscb(x, mu, sig, aL, nL, aR, nR):
    t = (x - mu) / sig; o = np.exp(-0.5 * t * t); lo, hi = t < -aL, t > aR
    o[lo] = (nL / aL) ** nL * np.exp(-0.5 * aL * aL) * (nL / aL - aL - t[lo]) ** (-nL)
    o[hi] = (nR / aR) ** nR * np.exp(-0.5 * aR * aR) * (nR / aR - aR + t[hi]) ** (-nR)
    return o


def core_grid(cn, fmu=1.0, fsg=1.0):
    r = DJ[cn]; mu = np.polyval(r["poly"]["mu"]["coef"], YC) * fmu; sg = np.polyval(r["poly"]["sigma"]["coef"], YC) * fsg
    g = np.empty((NX, NY))
    for k in range(NY):
        f = dscb(GXC, mu[k], sg[k], *r["tails"]).reshape(NX, 8).mean(1)
        g[:, k] = f / (f.sum() * DX)
    return g


def higgs_pdf(cn, p, marg_u, core):
    nr = DJ[cn]["nonres"][p]
    d = marg_u[None, :] * core
    if nr["fL"] > 0:
        l = landau.pdf(GXC, loc=nr["loc"], scale=nr["scale"]).reshape(NX, 8).mean(1); l /= l.sum() * DX
        d = (1 - nr["fL"]) * d + nr["fL"] * marg_u[None, :] * l[:, None]
    return d / (d.sum() * DX * DY)


# --- target binning
if a.binning == "cat40":
    target, NB = GRIDCELL, NCELL
else:
    nm, nu = (int(v) for v in a.binning.split(":")[1].split("x"))
    target = (np.arange(NY) // (NY // nu))[None, :] * nm + (np.arange(NX) // (NX // nm))[:, None]
    NB = nm * nu
cellarea = DX * DY
integ = lambda d: np.bincount(target.ravel(), weights=(d * cellarea).ravel(), minlength=NB)


def cell_sums(e, m=None):
    sel = np.ones(len(e["w"]), bool) if m is None else m
    cl = cell_of(XC[e["i"][sel]], e["s"][sel])
    return (np.bincount(cl, weights=e["w"][sel], minlength=NCELL),
            np.bincount(cl, weights=e["w"][sel] ** 2, minlength=NCELL))


hists = {}; rates = {cn: {} for cn in CH}; extra_rows = {}; report = []
cores = {cn: {"nom": core_grid(cn)} for cn in CH}
for cn in CH:
    for sy in MUSYST:
        for dname in ("Up", "Down"):
            ms = DJ[cn]["muon_syst"].get(sy, {}).get(dname)
            if ms is not None:
                cores[cn][f"{sy}|{dname}"] = core_grid(cn, 1 + ms["dmu_rel"], 1 + ms["dsig_rel"])

for p in pnames:
    if p == "ZX":
        e = load("zx_score", "zx_weight")
    else:
        keys = [k for k in ev.files if k.startswith(f"{p}|syst|")]
        e = load(f"{p}|score", f"{p}|weight", extra=keys)
    e["u"] = to_u(e["s"]); e["m"] = XC[e["i"]]
    if p == "ZX":
        dz, _, _ = kde2d(e["m"], e["u"], e["w"])
    nom_cell_all, nom_cell_all2 = cell_sums(e)
    for c, cn in enumerate(CH):
        m = e["c"] == c
        rate = float(e["w"][m].sum()); rates[cn][p] = rate
        if p == "ZX":
            d = dz
        else:
            d2, bws, neff = kde2d(e["m"][m], e["u"][m], e["w"][m])
            d = higgs_pdf(cn, p, d2.sum(0) * DX, cores[cn]["nom"]) if p in HIGGS else d2
        hists[f"h_{cn}_{p}"] = rate * integ(d)
        nom_cell, nom_cell2 = cell_sums(e, m)
        neff_cell = np.divide(nom_cell ** 2, nom_cell2, out=np.zeros(NCELL), where=nom_cell2 > 0)
        for sy, plist in shapes:
            if p not in plist:
                continue
            for dname in ("Up", "Down"):
                wk, jk = f"{p}|syst|{sy}|{dname}", f"{p}|jec|{sy}|{dname}|score"
                if p in HIGGS and sy in MUSYST and f"{sy}|{dname}" in cores[cn]:
                    js = load(jk, f"{p}|jec|{sy}|{dname}|weight")
                    rr = js["w"][js["c"] == c].sum() / rate if rate != 0 else 1.0
                    hists[f"h_{cn}_{p}_{sy}{dname}"] = rate * rr * integ(higgs_pdf(cn, p, d2.sum(0) * DX, cores[cn][f"{sy}|{dname}"]))
                    continue
                if wk in ev.files:
                    var_c = np.bincount(cell_of(e["m"][m], e["s"][m]), weights=e[wk][m], minlength=NCELL)
                    var_all = np.bincount(cell_of(e["m"], e["s"]), weights=e[wk], minlength=NCELL)
                    src = "w"
                elif jk in ev.files:
                    js = load(jk, f"{p}|jec|{sy}|{dname}|weight")
                    var_c = np.bincount(cell_of(XC[js["i"][js["c"] == c]], js["s"][js["c"] == c]), weights=js["w"][js["c"] == c], minlength=NCELL)
                    var_all = np.bincount(cell_of(XC[js["i"]], js["s"]), weights=js["w"], minlength=NCELL)
                    src = "jec"
                else:
                    hn = HC[f"h_{p}"].values(); hv = HC[f"h_{p}_{sy}{dname}"].values()
                    r = np.divide(hv, hn, out=np.ones_like(hn), where=hn != 0)
                    hists[f"h_{cn}_{p}_{sy}{dname}"] = rate * integ(d * r[GRIDCELL])
                    continue
                r_c = np.divide(var_c, nom_cell, out=np.ones(NCELL), where=nom_cell != 0)
                r_a = np.divide(var_all, nom_cell_all, out=np.ones(NCELL), where=nom_cell_all != 0)
                r = np.where(neff_cell >= 20, r_c, r_a) if LEPTON_FLAVOUR.match(sy) else r_a
                hists[f"h_{cn}_{p}_{sy}{dname}"] = rate * integ(d * r[GRIDCELL])
    report.append(f"{p:<8} rate 4e/4mu/2e2mu {rates['4e'][p]:.5f} / {rates['4mu'][p]:.5f} / {rates['2e2mu'][p]:.5f} "
                  f"sum {sum(rates[cn][p] for cn in CH):.5f} card {card_rate[p]:.5f}")
    print(report[-1], flush=True)

# 2026-10-30: the total (all-channel) rate effect of every shape row follows the cat40 card, as the inclusive 2D
# card does. The binned builder normalises the background mu_R/mu_F rows to shape-only (HIG-24-013
# --higgs-scale-norm; the rate is QCDscale_* lnN), but the per-event dump weights keep the rate (ggH mu_R
# -17%/+23%, on top of QCDscale_ggH): that double count made the split card ~5% worse than the inclusive one
# (grpscan.txt: scalevar alone cost +60 split vs +20 inclusive). Per-channel split of the effect is kept.
_norm_log = []
for sy, plist in shapes:
    for p in plist:
        for dname in ("Up", "Down"):
            hk = f"h_{p}_{sy}{dname}"
            if hk not in HC or f"h_{p}" not in HC:
                continue
            k = HC[hk].values().sum() / HC[f"h_{p}"].values().sum()
            tot_v = sum(hists[f"h_{cn}_{p}_{sy}{dname}"].sum() for cn in CH)
            tot_n = sum(hists[f"h_{cn}_{p}"].sum() for cn in CH)
            if tot_v <= 0 or tot_n <= 0:
                continue
            f = k * tot_n / tot_v
            for cn in CH:
                hists[f"h_{cn}_{p}_{sy}{dname}"] = hists[f"h_{cn}_{p}_{sy}{dname}"] * f
            if abs(f - 1) > 0.003:
                _norm_log.append(f"{p} {sy}{dname}: rate {tot_v / tot_n:.4f} -> cat40 {k:.4f}")
print("rate-normalised to cat40:", len(_norm_log), "rows changed > 0.3%"); print("\n".join(_norm_log), flush=True)

for cn in CH:
    hists[f"h_{cn}_data_obs"] = sum(hists[f"h_{cn}_{p}"] for p in pnames if p != "Signal")

os.makedirs(a.out, exist_ok=True)
import shutil, subprocess, tempfile
# 2026-10-30: uproot files written straight to EOS ended up with a corrupt compressed record (R__unzip_header) in
# 2 of 8 cards; write locally, read every histogram back with ROOT, then copy and compare md5.
_tmpd = tempfile.mkdtemp(prefix="card_", dir=os.environ.get("TMPDIR", "/tmp"))
_loc = os.path.join(_tmpd, "histograms_ch.root")
with uproot.recreate(_loc) as f:
    eb = np.arange(NB + 1, dtype=float)
    for k, v in hists.items():
        f[k] = (np.asarray(v, dtype=float), eb)
_chk = subprocess.run(["python3", "/eos/user/s/snandaku/Analysis/unbinned2d_2026_10_30/check_root.py", _loc])
if _chk.returncode:
    raise SystemExit(f"corrupt histograms in {_loc}")
shutil.copy(_loc, os.path.join(a.out, "histograms_ch.root"))
_md5 = [subprocess.run(["md5sum", x], capture_output=True, text=True).stdout.split()[0] for x in (_loc, os.path.join(a.out, "histograms_ch.root"))]
if _md5[0] != _md5[1]:
    raise SystemExit(f"md5 mismatch after copy to {a.out}")
shutil.rmtree(_tmpd)
nP = len(pnames)
L = [f"# 2026-10-30 final-state-split ATLAS-style (m4l, u) card, binning {a.binning}; DSCB(m4l|u) per final state",
     f"# from {a.events}, {a.dscb}, systematics from the dump per final state / {a.card_dir}",
     "imax 3", f"jmax {nP - 1}", "kmax *", "-" * 40]
L += [f"shapes * hczz_{cn} histograms_ch.root h_{cn}_$PROCESS h_{cn}_$PROCESS_$SYSTEMATIC" for cn in CH]
L += ["-" * 40, "bin " + " ".join(f"hczz_{cn}" for cn in CH), "observation -1 -1 -1", "-" * 40,
      "bin " + " ".join(f"hczz_{cn}" for cn in CH for _ in pnames),
      "process " + " ".join(p for _ in CH for p in pnames),
      "process " + " ".join(str(i) for _ in CH for i in range(nP)),
      "rate " + " ".join(f"{max(rates[cn][p], 1e-9):.6f}" for cn in CH for p in pnames), "-" * 40]
for nm_, vals in lnn:
    L.append(f"{nm_:<28} lnN " + " ".join(vals * 3))
for sy, plist in shapes:
    L.append(f"{sy:<28} shape " + " ".join(("1" if p in plist else "-") for _ in CH for p in pnames))
open(os.path.join(a.out, "datacard_ch.txt"), "w").write("\n".join(L) + "\n")
open(os.path.join(a.out, "rates.txt"), "w").write("\n".join(report) + "\n")
print(f"{a.out}: {a.binning} x 3 channels -> {NB} bins each, {len(hists)} histograms; "
      f"bkg {sum(rates[cn][p] for cn in CH for p in pnames if p != 'Signal'):.3f}, "
      f"signal {sum(rates[cn]['Signal'] for cn in CH):.5f}")
