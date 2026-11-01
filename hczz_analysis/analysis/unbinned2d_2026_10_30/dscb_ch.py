#!/usr/bin/env python3
"""2026-10-30: per-final-state Higgs m4l lineshape for the final-state-split ATLAS-style (m4l, u) fit.

Input: events_2d_ch.npz (dump_events_2d_ch.py: score = (c N + i + s)/(3N), c = 0 4e / 1 4mu / 2 2e2mu).
Per final state c (as ATLAS per subchannel and HIG-24-013 per final state):
  * resonant core: DSCB fitted to the clean resonant sample Signal + ggH + VBF; tails from the inclusive (in u)
    fit of that channel; mean and sigma in NS u slices, then a 2nd-order polynomial in u (ATLAS: b(D_NN)
    quadratic, sigma parameterised in D_NN); the slice points are kept for the check of the polynomial;
  * non-resonant part per process: fraction fL and Landau(loc, scale), with the u-averaged core fixed
    (HIG-24-013 non-resonant signal);
  * our muon scale/resolution systematics (CMS_scale_m, CMS_res_m: reprocessed muon-shifted samples in the
    dump, |jec|): DSCB mean and sigma refitted (tails fixed, inclusive in u) on the shifted clean sample;
    relative shifts dmu/mu, dsigma/sigma per channel and direction. No electron scale/resolution variations
    exist in our production (only the nominal electron scale/smearing corrections).
u = S+B-mix CDF of the score (from pdfs_2d.npz of the inclusive chain, same as all other cards).
Output: <out>/dscb_ch.json + dscb_ch.png.  Usage: python3 dscb_ch.py <events_2d_ch.npz> <pdfs_2d.npz> <out> [NS]
"""
import json
import os
import sys

import numpy as np
from scipy.optimize import minimize
from scipy.stats import landau

NPZ, PDFS, OUT = sys.argv[1:4]
NS = int(sys.argv[4]) if len(sys.argv) > 4 else 6
FINE = np.round(np.arange(105.0, 160.0 + 1e-9, 0.25), 4)
NX = len(FINE) - 1
GX = np.linspace(105, 160, NX * 8 + 1)
GC = 0.5 * (GX[1:] + GX[:-1])
CH = ["4e", "4mu", "2e2mu"]
CLEAN = ["Signal", "ggH", "VBF"]
ALLH = ["Signal", "ggH", "VBF", "WminusH", "WplusH", "ZH", "ttH", "HPlusB"]
# lepton scale/resolution measured on the DSCB of the shifted samples (HIG-24-013 method: refit the peak with
# shifted lepton momenta); electron rows (2026-10-30 production) are used when present in the dump
MUSYST = ["CMS_scale_m", "CMS_res_m", "CMS_scale_e", "CMS_res_e"]

ev = np.load(NPZ, allow_pickle=False)
P = np.load(PDFS, allow_pickle=False)
us, uc = P["u_s"], P["u_c"]


def decode(v):
    k = np.floor(v * 3 * NX).astype(int)
    ok = (v >= 0) & (k >= 0) & (k < 3 * NX)
    k = np.clip(k, 0, 3 * NX - 1)
    return k // NX, k % NX, v * 3 * NX - k, ok


def load(key_s, key_w):
    c, i, s, ok = decode(ev[key_s])
    return c[ok], i[ok], np.clip(np.interp(s[ok], us, uc), 0, 1), ev[key_w][ok]


def dscb(x, mu, sig, aL, nL, aR, nR):
    t = (x - mu) / sig
    o = np.exp(-0.5 * t * t)
    lo, hi = t < -aL, t > aR
    o[lo] = (nL / aL) ** nL * np.exp(-0.5 * aL * aL) * (nL / aL - aL - t[lo]) ** (-nL)
    o[hi] = (nR / aR) ** nR * np.exp(-0.5 * aR * aR) * (nR / aR - aR + t[hi]) ** (-nR)
    return o


def binn(f):
    f = f.reshape(NX, 8).mean(1)
    return f / f.sum()


def wfit(h, h2, model, p0s, ok):
    tot = h.sum()
    neff = tot ** 2 / h2.sum()
    n = h * neff / tot

    def nll(q):
        if not ok(q):
            return 1e12
        m = model(q) * n.sum()
        if not np.all(np.isfinite(m)):
            return 1e12
        return np.sum(m - n * np.log(np.clip(m, 1e-300, None)))

    best = min((minimize(nll, p0, method="Nelder-Mead", options={"maxiter": 40000, "xatol": 1e-7, "fatol": 1e-8})
                for p0 in p0s), key=lambda r: r.fun)
    q = best.x
    m = model(q) * tot
    big = m > 1e-3 * m.max()
    var = np.maximum(h2, m * h2.sum() / tot)
    chi2 = float(np.sum((h - m)[big] ** 2 / var[big]))
    eps = np.maximum(np.abs(q) * 1e-4, 1e-5)
    Hs = np.zeros((len(q), len(q)))
    for a in range(len(q)):
        for b in range(a, len(q)):
            def f(da, db):
                z = q.copy(); z[a] += da; z[b] += db
                return nll(z)
            Hs[a, b] = Hs[b, a] = (f(eps[a], eps[b]) - f(eps[a], -eps[b]) - f(-eps[a], eps[b]) + f(-eps[a], -eps[b])) / (4 * eps[a] * eps[b])
    try:
        err = np.sqrt(np.clip(np.diag(np.linalg.inv(Hs)), 0, None))
    except np.linalg.LinAlgError:
        err = np.full(len(q), np.nan)
    return q, err, chi2, int(big.sum()) - len(q), neff


ok6 = lambda q: 120 < q[0] < 130 and 0.3 < q[1] < 6 and 0.2 < q[2] < 6 and 1.01 < q[3] < 80 and 0.2 < q[4] < 6 and 1.01 < q[5] < 80
ok2 = lambda q: 120 < q[0] < 130 and 0.3 < q[1] < 6
P0 = [(124.75, 1.3, 0.8, 4.0, 1.3, 8.0), (124.7, 1.6, 1.0, 3.0, 1.5, 5.0), (124.8, 1.1, 0.7, 6.0, 1.2, 12.0)]
hist = lambda i, w: (np.bincount(i, weights=w, minlength=NX), np.bincount(i, weights=w * w, minlength=NX))

D = {p: load(f"{p}|score", f"{p}|weight") for p in ALLH}
res = {"channels": {}, "u_def": "S+B-mix CDF of mva_score_Signal (inclusive pdfs_2d.npz)", "NS": NS}
for c, cn in enumerate(CH):
    sel = {p: D[p][0] == c for p in ALLH}
    ci = np.concatenate([D[p][1][sel[p]] for p in CLEAN]); cu = np.concatenate([D[p][2][sel[p]] for p in CLEAN])
    cw = np.concatenate([D[p][3][sel[p]] for p in CLEAN])
    q0, e0, c0, n0, ne0 = wfit(*hist(ci, cw), lambda q: binn(dscb(GC, *q)), P0, ok6)
    tails = q0[2:]
    print(f"[{cn}] clean inclusive neff {ne0:.0f}: mu {q0[0]:.3f} sig {q0[1]:.3f} aL {q0[2]:.2f} nL {q0[3]:.1f} "
          f"aR {q0[4]:.2f} nR {q0[5]:.1f} chi2/ndf {c0:.0f}/{n0}", flush=True)
    o = np.argsort(cu); cc = np.cumsum(np.abs(cw[o])); cc /= cc[-1]
    ed = np.concatenate([[0.0], [cu[o][np.searchsorted(cc, k / NS)] for k in range(1, NS)], [1.0]])
    sl = []
    for k in range(NS):
        m = (cu >= ed[k]) & (cu <= ed[k + 1])
        q, e, c2, nd, ne = wfit(*hist(ci[m], cw[m]), lambda q: binn(dscb(GC, q[0], q[1], *tails)),
                                [(q0[0], q0[1]), (q0[0] - 0.1, q0[1] * 1.15), (q0[0] + 0.1, q0[1] * 0.87)], ok2)
        sl.append({"u_lo": float(ed[k]), "u_hi": float(ed[k + 1]), "u_mean": float(np.average(cu[m], weights=np.abs(cw[m]))),
                   "mu": q[0], "mu_err": e[0], "sigma": q[1], "sigma_err": e[1], "chi2": c2, "ndf": nd, "neff": ne})
        print(f"[{cn}] u [{ed[k]:.3f},{ed[k+1]:.3f}] neff {ne:6.0f} mu {q[0]:.3f}+-{e[0]:.3f} sig {q[1]:.3f}+-{e[1]:.3f} "
              f"chi2/ndf {c2:.0f}/{nd}", flush=True)
    x = np.array([s["u_mean"] for s in sl])
    poly = {}
    for key in ("mu", "sigma"):
        y = np.array([s[key] for s in sl]); ey = np.array([s[key + "_err"] for s in sl])
        cf = np.polyfit(x, y, 2, w=1 / ey)
        ch2 = float(np.sum(((y - np.polyval(cf, x)) / ey) ** 2))
        poly[key] = {"coef": cf.tolist(), "chi2": ch2, "ndf": NS - 3}
        print(f"[{cn}] {key}(u) quadratic {np.round(cf, 4).tolist()} chi2/ndf {ch2:.1f}/{NS - 3}")
    mu_of = lambda u: np.polyval(poly["mu"]["coef"], u)
    sg_of = lambda u: np.polyval(poly["sigma"]["coef"], u)

    def core_for(u, w):
        hu, eu = np.histogram(u, bins=50, range=(0, 1), weights=w)
        um = 0.5 * (eu[1:] + eu[:-1])
        f = sum(max(hk, 0) * binn(dscb(GC, mu_of(uk), sg_of(uk), *tails)) for hk, uk in zip(hu, um))
        return f / f.sum()

    nonres = {}
    for p in ALLH:
        i, u, w = D[p][1][sel[p]], D[p][2][sel[p]], D[p][3][sel[p]]
        core = core_for(u, w)
        h, h2 = hist(i, w)

        def model(q, core=core):
            return (1 - q[0]) * core + q[0] * binn(landau.pdf(GC, loc=q[1], scale=q[2]))

        q, e, c2, nd, ne = wfit(h, h2, model, [(0.02, 140.0, 15.0), (0.1, 150.0, 30.0), (0.3, 160.0, 30.0)],
                                lambda q: 0 <= q[0] < 0.8 and 100 < q[1] < 250 and 1 < q[2] < 100)
        mc = core * h.sum(); bg = mc > 1e-3 * mc.max()
        c2c = float(np.sum((h - mc)[bg] ** 2 / np.maximum(h2, mc * h2.sum() / h.sum())[bg]))
        fl = q[0] if (np.isfinite(e[0]) and q[0] > 3 * e[0]) else 0.0
        nonres[p] = {"fL": fl, "fL_fit": q[0], "fL_err": e[0], "loc": q[1], "scale": q[2], "chi2": c2, "ndf": nd,
                     "chi2_core_only": c2c, "neff": ne, "sumw": float(w.sum())}
        print(f"[{cn}] {p:<8} neff {ne:7.0f} sumw {w.sum():.4f} fL {q[0]:.3f}+-{e[0]:.3f} (used {fl:.3f}) "
              f"chi2/ndf {c2:.0f}/{nd} core only {c2c:.0f}", flush=True)

    # our muon scale/resolution: shifted clean samples, inclusive in u, tails fixed
    nom_q, nom_e, *_ = wfit(*hist(ci, cw), lambda q: binn(dscb(GC, q[0], q[1], *tails)), [(q0[0], q0[1])], ok2)
    mus = {}
    for sy in MUSYST:
        for dname in ("Up", "Down"):
            parts = []
            for p in CLEAN:
                ks, kw = f"{p}|jec|{sy}|{dname}|score", f"{p}|jec|{sy}|{dname}|weight"
                if ks in ev.files:
                    cc_, ii, uu, ww = load(ks, kw)
                    parts.append((ii[cc_ == c], ww[cc_ == c]))
            if not parts:
                continue
            ii = np.concatenate([a for a, _ in parts]); ww = np.concatenate([b for _, b in parts])
            q, e, *_ = wfit(*hist(ii, ww), lambda q: binn(dscb(GC, q[0], q[1], *tails)), [(nom_q[0], nom_q[1])], ok2)
            mus.setdefault(sy, {})[dname] = {"dmu_rel": (q[0] - nom_q[0]) / nom_q[0], "dsig_rel": (q[1] - nom_q[1]) / nom_q[1],
                                             "mu": q[0], "sigma": q[1]}
            print(f"[{cn}] {sy}{dname}: dmu/mu {(q[0] - nom_q[0]) / nom_q[0] * 100:+.4f}% dsig/sig "
                  f"{(q[1] - nom_q[1]) / nom_q[1] * 100:+.2f}%  (nominal mu {nom_q[0]:.4f}+-{nom_e[0]:.4f} sig {nom_q[1]:.4f})", flush=True)
    res["channels"][cn] = {"tails": tails.tolist(), "inclusive": {"par": q0.tolist(), "chi2": c0, "ndf": n0, "neff": ne0},
                           "slices": sl, "poly": poly, "nonres": nonres, "muon_syst": mus,
                           "nominal_incl_mu_sigma": nom_q.tolist()}

os.makedirs(OUT, exist_ok=True)
json.dump(res, open(os.path.join(OUT, "dscb_ch.json"), "w"), indent=1, default=float)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
fig, axs = plt.subplots(1, 2, figsize=(11, 4))
g = np.linspace(0, 1, 101)
for c, cn in enumerate(CH):
    r = res["channels"][cn]
    for ax, key in ((axs[0], "mu"), (axs[1], "sigma")):
        ax.errorbar([s["u_mean"] for s in r["slices"]], [s[key] for s in r["slices"]],
                    yerr=[s[key + "_err"] for s in r["slices"]], fmt="o", color=f"C{c}", label=f"{cn} slices")
        ax.plot(g, np.polyval(r["poly"][key]["coef"], g), color=f"C{c}",
                label=f"{cn} quadratic ({r['poly'][key]['chi2']:.1f}/{r['poly'][key]['ndf']})")
for ax, key in ((axs[0], "mu"), (axs[1], "sigma")):
    ax.set_xlabel("u = S+B CDF of p_sig"); ax.set_ylabel(f"DSCB {key} [GeV]"); ax.legend(fontsize=7)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "dscb_ch.png"), dpi=120)
