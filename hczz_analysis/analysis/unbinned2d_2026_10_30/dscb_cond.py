#!/usr/bin/env python3
"""2026-10-30: conditional Higgs m4l lineshape for the ATLAS-style (m4l, u) fit.

dscb_fit.py showed (massinvlep DisCo 0.5): DSCB + Landau (HIG-24-013 resonant + non-resonant signal)
fits every Higgs process; the DSCB core depends on the score (ggH sigma 1.29 / 1.16 / 1.53 GeV in u
terciles, signal 1.37 / 1.07 / 1.51), so P(m4l, u) != DSCB(m4l) f(u). As ATLAS conditions the DSCB width
on the per-event resolution, the DSCB here is conditional on u:
  * resonant core: one DSCB for all Higgs processes (HIG-24-013: one P_res), fitted to the clean
    resonant sample Signal + ggH + VBF in NS u slices; mu(u), sigma(u) are then smooth (quadratic in u,
    weighted by the slice errors); tails (aL, nL, aR, nR) from the inclusive fit, common;
  * non-resonant part per process: fraction fL and Landau(loc, scale) from a fit of
    (1 - fL) DSCB_core(u-averaged) + fL Landau to that process (core fixed), inclusive in u.
Output: <out>/dscb_cond.json; plot of mu(u), sigma(u).
Usage: python3 dscb_cond.py <events_2d.npz> <pdfs_2d.npz> <out dir> [NS]
"""
import json
import os
import sys

import numpy as np
from scipy.optimize import minimize
from scipy.stats import landau

NPZ, PDFS, OUT = sys.argv[1:4]
NS = int(sys.argv[4]) if len(sys.argv) > 4 else 8
FINE = np.round(np.arange(105.0, 160.0 + 1e-9, 0.25), 4)
NX = len(FINE) - 1
GC = 0.5 * (np.linspace(105, 160, NX * 8 + 1)[1:] + np.linspace(105, 160, NX * 8 + 1)[:-1])
CLEAN = ["Signal", "ggH", "VBF"]
NONRES = ["WminusH", "WplusH", "ZH", "ttH", "HPlusB"]

ev = np.load(NPZ, allow_pickle=False)
P = np.load(PDFS, allow_pickle=False)
us, uc = P["u_s"], P["u_c"]


def load(p):
    v, w = ev[f"{p}|score"], ev[f"{p}|weight"]
    i = np.floor(v * NX).astype(int)
    ok = (v >= 0) & (i >= 0) & (i < NX)
    i = i[ok]
    return i, np.clip(np.interp(v[ok] * NX - i, us, uc), 0, 1), w[ok]


def dscb(x, mu, sig, aL, nL, aR, nR):
    t = (x - mu) / sig
    out = np.exp(-0.5 * t * t)
    lo, hi = t < -aL, t > aR
    out[lo] = (nL / aL) ** nL * np.exp(-0.5 * aL * aL) * (nL / aL - aL - t[lo]) ** (-nL)
    out[hi] = (nR / aR) ** nR * np.exp(-0.5 * aR * aR) * (nR / aR - aR + t[hi]) ** (-nR)
    return out


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

    best = min((minimize(nll, p0, method="Nelder-Mead", options={"maxiter": 40000, "xatol": 1e-6, "fatol": 1e-7})
                for p0 in p0s), key=lambda r: r.fun)
    m = model(best.x) * tot
    big = m > 1e-3 * m.max()
    var = np.maximum(h2, m * h2.sum() / tot)
    chi2 = float(np.sum((h - m)[big] ** 2 / var[big]))
    # parameter errors from a numerical Hessian of the nll (effective counts)
    q = best.x
    eps = np.maximum(np.abs(q) * 1e-4, 1e-5)
    H = np.zeros((len(q), len(q)))
    for a in range(len(q)):
        for b in range(a, len(q)):
            def f(da, db):
                z = q.copy(); z[a] += da; z[b] += db
                return nll(z)
            H[a, b] = H[b, a] = (f(eps[a], eps[b]) - f(eps[a], -eps[b]) - f(-eps[a], eps[b]) + f(-eps[a], -eps[b])) / (4 * eps[a] * eps[b])
    try:
        err = np.sqrt(np.clip(np.diag(np.linalg.inv(H)), 0, None))
    except np.linalg.LinAlgError:
        err = np.full(len(q), np.nan)
    return q, err, chi2, int(big.sum()) - len(q), neff


ok6 = lambda q: 120 < q[0] < 130 and 0.3 < q[1] < 5 and 0.2 < q[2] < 6 and 1.01 < q[3] < 80 and 0.2 < q[4] < 6 and 1.01 < q[5] < 80
P0 = [(124.75, 1.3, 0.8, 4.0, 1.3, 8.0), (124.7, 1.4, 1.0, 3.0, 1.5, 5.0), (124.8, 1.2, 0.7, 6.0, 1.2, 12.0)]
d = {p: load(p) for p in CLEAN + NONRES}
ci = np.concatenate([d[p][0] for p in CLEAN]); cu = np.concatenate([d[p][1] for p in CLEAN])
cw = np.concatenate([d[p][2] for p in CLEAN])
H = lambda m: (np.bincount(ci[m], weights=cw[m], minlength=NX), np.bincount(ci[m], weights=cw[m] ** 2, minlength=NX))

# 1. inclusive clean DSCB -> common tails
q0, e0, c0, n0, ne0 = wfit(*H(np.ones(len(ci), bool)), lambda q: binn(dscb(GC, *q)), P0, ok6)
print(f"clean inclusive: mu {q0[0]:.3f} sig {q0[1]:.3f} aL {q0[2]:.2f} nL {q0[3]:.1f} aR {q0[4]:.2f} nR {q0[5]:.1f} chi2/ndf {c0:.0f}/{n0}")
tails = q0[2:]

# 2. u slices (equal clean weight): mu, sigma with tails fixed
o = np.argsort(cu); c = np.cumsum(np.abs(cw[o])); c /= c[-1]
edges = np.concatenate([[0.0], [cu[o][np.searchsorted(c, k / NS)] for k in range(1, NS)], [1.0]])
sl = []
for k in range(NS):
    m = (cu >= edges[k]) & (cu <= edges[k + 1])
    q, e, c2, nd, ne = wfit(*H(m), lambda q: binn(dscb(GC, q[0], q[1], *tails)),
                            [(124.75, 1.3), (124.6, 1.5), (124.9, 1.1)], lambda q: 120 < q[0] < 130 and 0.3 < q[1] < 5)
    uc_ = float(np.average(cu[m], weights=np.abs(cw[m])))
    sl.append({"u_lo": float(edges[k]), "u_hi": float(edges[k + 1]), "u_mean": uc_, "mu": q[0], "mu_err": e[0],
               "sigma": q[1], "sigma_err": e[1], "chi2": c2, "ndf": nd, "neff": ne})
    print(f"u [{edges[k]:.3f},{edges[k+1]:.3f}] <u> {uc_:.3f} neff {ne:6.0f} mu {q[0]:.3f}+-{e[0]:.3f} "
          f"sig {q[1]:.3f}+-{e[1]:.3f} chi2/ndf {c2:.0f}/{nd}", flush=True)
x = np.array([s["u_mean"] for s in sl])
cmu = np.polyfit(x, [s["mu"] for s in sl], 2, w=1 / np.array([s["mu_err"] for s in sl]))
csg = np.polyfit(x, [s["sigma"] for s in sl], 2, w=1 / np.array([s["sigma_err"] for s in sl]))
for nm, cc, key in (("mu", cmu, "mu"), ("sigma", csg, "sigma")):
    pr = np.polyval(cc, x)
    ch = np.sum(((np.array([s[key] for s in sl]) - pr) / np.array([s[key + "_err"] for s in sl])) ** 2)
    print(f"{nm}(u) quadratic {np.round(cc, 4).tolist()}  chi2/ndf {ch:.1f}/{NS - 3}")

# 3. non-resonant fraction per process (core = clean DSCB at the process's own u distribution)
MU_X = x
MU_Y = np.array([s["mu"] for s in sl])
SG_Y = np.array([s["sigma"] for s in sl])
mu_of = lambda uu: np.interp(uu, MU_X, MU_Y)
sg_of = lambda uu: np.interp(uu, MU_X, SG_Y)


def core_for(u, w):
    """u-averaged conditional core for a process: sum over its events' u of DSCB(mu(u), sigma(u))."""
    hu, eu = np.histogram(u, bins=50, range=(0, 1), weights=w)
    um = 0.5 * (eu[1:] + eu[:-1])
    f = sum(max(hk, 0) * binn(dscb(GC, mu_of(uk), sg_of(uk), *tails)) for hk, uk in zip(hu, um))
    return f / f.sum()


nonres = {}
for p in CLEAN + NONRES:
    i, u, w = d[p]
    core = core_for(u, w)
    h, h2 = np.bincount(i, weights=w, minlength=NX), np.bincount(i, weights=w * w, minlength=NX)

    def model(q, core=core):
        l = binn(landau.pdf(GC, loc=q[1], scale=q[2]))
        return (1 - q[0]) * core + q[0] * l

    q, e, c2, nd, ne = wfit(h, h2, model, [(0.02, 140.0, 15.0), (0.1, 150.0, 30.0), (0.3, 160.0, 30.0)],
                            lambda q: 0 <= q[0] < 0.8 and 100 < q[1] < 250 and 1 < q[2] < 100)
    mc = core * h.sum(); bg = mc > 1e-3 * mc.max()
    c2c = float(np.sum((h - mc)[bg] ** 2 / np.maximum(h2, mc * h2.sum() / h.sum())[bg]))
    nonres[p] = {"fL": q[0], "fL_err": e[0], "loc": q[1], "scale": q[2], "chi2": c2, "ndf": nd,
                 "chi2_core_only": c2c, "neff": ne}
    print(f"{p:<8} neff {ne:7.0f} fL {q[0]:.3f}+-{e[0]:.3f} Landau({q[1]:.0f},{q[2]:.1f}) chi2/ndf {c2:.0f}/{nd}"
          f"  (core only {c2c:.0f})", flush=True)

os.makedirs(OUT, exist_ok=True)
json.dump({"tails": tails.tolist(), "interp": "linear in <u> of the slices", "u_nodes": MU_X.tolist(),
           "mu_nodes": MU_Y.tolist(), "sigma_nodes": SG_Y.tolist(), "slices": sl,
           "nonres": nonres, "inclusive_clean": {"par": q0.tolist(), "chi2": c0, "ndf": n0}},
          open(os.path.join(OUT, "dscb_cond.json"), "w"), indent=1, default=float)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
fig, axs = plt.subplots(1, 2, figsize=(11, 4))
g = np.linspace(0, 1, 101)
for ax, key, cc in ((axs[0], "mu", cmu), (axs[1], "sigma", csg)):
    ax.errorbar(x, [s[key] for s in sl], yerr=[s[key + "_err"] for s in sl], xerr=[(s["u_hi"] - s["u_lo"]) / 2 for s in sl],
                fmt="o", color="k", label="Signal+ggH+VBF, u slices")
    ax.plot(g, np.interp(g, MU_X, MU_Y if key == "mu" else SG_Y), color="C3", label="linear interpolation (used)")
    ax.plot(g, np.polyval(cc, g), color="C0", ls="--", label="quadratic (not used)")
    ax.set_xlabel("u = S+B CDF of p_sig"); ax.set_ylabel(f"DSCB {key} [GeV]"); ax.legend(fontsize=8)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "dscb_cond.png"), dpi=120)
