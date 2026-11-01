#!/usr/bin/env python3
"""2026-10-30: double-sided Crystal Ball (DSCB) fits of the Higgs m4l lineshape, for the ATLAS-style
unbinned (m4l, u) fit (ATLAS H->4l mass: DSCB signal x NN pdf, ZZ from MC KDE).

Weighted binned likelihood fit (0.25 GeV = the dump's m4l grid) of a DSCB normalised on 105-160 GeV:
  * all Higgs processes together (nominal shape);
  * each process alone, and the pooled Higgs in u terciles (checks: one shape for all processes, and
    m4l independent of the score, so that P(m4l, u) = DSCB(m4l) x f(u)).
Fit quality: chi2 of the pooled fit with the MC weights' sum w^2.
Output: <out>/dscb.json, <out>/dscb_fit.png.
Usage: python3 dscb_fit.py <events_2d.npz> <pdfs_2d.npz (for u)> <out dir>
"""
import json
import os
import sys

import numpy as np
from scipy.optimize import minimize

NPZ, PDFS, OUT = sys.argv[1:4]
FINE = np.round(np.arange(105.0, 160.0 + 1e-9, 0.25), 4)
NX = len(FINE) - 1
XC = 0.5 * (FINE[1:] + FINE[:-1])
HIGGS = ["Signal", "ggH", "VBF", "WminusH", "WplusH", "ZH", "ttH", "HPlusB"]

ev = np.load(NPZ, allow_pickle=False)
P = np.load(PDFS, allow_pickle=False)
us, uc = P["u_s"], P["u_c"]


def load(p):
    v, w = ev[f"{p}|score"], ev[f"{p}|weight"]
    i = np.floor(v * NX).astype(int)
    ok = (v >= 0) & (i >= 0) & (i < NX)
    i = i[ok]
    s = v[ok] * NX - i
    return i, np.clip(np.interp(s, us, uc), 0, 1), w[ok]


def dscb(x, mu, sig, aL, nL, aR, nR):
    t = (x - mu) / sig
    out = np.exp(-0.5 * t * t)
    lo, hi = t < -aL, t > aR
    AL = (nL / aL) ** nL * np.exp(-0.5 * aL * aL)
    AR = (nR / aR) ** nR * np.exp(-0.5 * aR * aR)
    out[lo] = AL * (nL / aL - aL - t[lo]) ** (-nL)
    out[hi] = AR * (nR / aR - aR + t[hi]) ** (-nR)
    return out


GX = np.linspace(105.0, 160.0, 220 * 8 + 1)
GC = 0.5 * (GX[1:] + GX[:-1])


from scipy.stats import landau


def binned(par):
    """(1 - f) DSCB + f Landau(loc, scale), each normalised on 105-160 and integrated in the 0.25 GeV bins
    (8 sub-points). HIG-24-013: resonant signal DSCB, non-resonant signal Landau."""
    d = dscb(GC, *par[:6]).reshape(NX, 8).mean(1)
    d /= d.sum()
    if len(par) == 6:
        return d
    f, loc, scl = par[6:9]
    l = landau.pdf(GC, loc=loc, scale=scl).reshape(NX, 8).mean(1)
    l /= l.sum()
    return (1 - f) * d + f * l


def fit(h, h2, p0=(124.9, 1.3, 1.2, 3.0, 1.4, 6.0), land=True):
    tot = h.sum()
    neff = tot ** 2 / h2.sum()
    # weighted Poisson likelihood with effective counts: n_eff,i = h_i * tot_eff/tot (scaled Poisson)
    sc = neff / tot
    n = h * sc

    def nll(q):
        mu, sig, aL, nL, aR, nR = q[:6]
        if not (120 < mu < 130 and 0.3 < sig < 5 and 0.3 < aL < 6 and 1.01 < nL < 60 and 0.3 < aR < 6 and 1.01 < nR < 60):
            return 1e12
        if len(q) > 6 and not (0 <= q[6] < 0.6 and 100 < q[7] < 250 and 1 < q[8] < 100):
            return 1e12
        m = binned(q) * n.sum()
        return np.sum(m - n * np.log(np.clip(m, 1e-300, None)))

    best = None
    starts = [p0, (124.8, 1.1, 1.0, 2.0, 1.2, 4.0), (125.0, 1.5, 1.5, 5.0, 1.8, 10.0)]
    if land:
        starts = [tuple(a) + b for a in starts for b in ((0.02, 140.0, 15.0), (0.08, 160.0, 25.0))]
    for start in starts:
        r = minimize(nll, start, method="Nelder-Mead", options={"maxiter": 40000, "xatol": 1e-5, "fatol": 1e-6})
        if best is None or r.fun < best.fun:
            best = r
    q = best.x
    m = binned(q) * tot
    err2 = np.clip(h2, 1e-12, None)
    big = m > 1e-3 * m.max()
    chi2 = float(np.sum((h - m)[big] ** 2 / err2[big]))
    return q, chi2, int(big.sum()) - len(q), neff


res = {"procs": {}, "pooled": None, "terciles": {}}
pooled = {}
alli, allu, allw = [], [], []
for p in HIGGS:
    i, u, w = load(p)
    alli.append(i); allu.append(u); allw.append(w)
    h = np.bincount(i, weights=w, minlength=NX)
    h2 = np.bincount(i, weights=w * w, minlength=NX)
    q, chi2, ndf, neff = fit(h, h2)
    res["procs"][p] = {"par": q.tolist(), "chi2": chi2, "ndf": ndf, "neff": neff, "sumw": float(w.sum())}
    print(f"{p:<8} neff {neff:8.0f}  mu {q[0]:.3f} sig {q[1]:.3f} aL {q[2]:.2f} nL {q[3]:.1f} aR {q[4]:.2f} nR {q[5]:.1f}"
          f" fL {q[6]:.3f} L({q[7]:.0f},{q[8]:.1f})  chi2/ndf {chi2:.0f}/{ndf}", flush=True)
i, u, w = np.concatenate(alli), np.concatenate(allu), np.concatenate(allw)
h = np.bincount(i, weights=w, minlength=NX)
h2 = np.bincount(i, weights=w * w, minlength=NX)
q, chi2, ndf, neff = fit(h, h2)
res["pooled"] = {"par": q.tolist(), "chi2": chi2, "ndf": ndf, "neff": neff}
print(f"POOLED   neff {neff:8.0f}  mu {q[0]:.3f} sig {q[1]:.3f} aL {q[2]:.2f} nL {q[3]:.1f} aR {q[4]:.2f} nR {q[5]:.1f}"
      f" fL {q[6]:.3f}  chi2/ndf {chi2:.0f}/{ndf}")
# score dependence: pooled Higgs in u terciles (of the pooled Higgs weight) + signal alone in u halves
o = np.argsort(u)
c = np.cumsum(np.abs(w[o])); c /= c[-1]
cuts = [0.0, u[o][np.searchsorted(c, 1 / 3)], u[o][np.searchsorted(c, 2 / 3)], 1.0]
for k in range(3):
    m = (u >= cuts[k]) & (u <= cuts[k + 1])
    hk = np.bincount(i[m], weights=w[m], minlength=NX)
    h2k = np.bincount(i[m], weights=w[m] ** 2, minlength=NX)
    qk, c2, nd, ne = fit(hk, h2k)
    res["terciles"][f"u{cuts[k]:.3f}-{cuts[k+1]:.3f}"] = {"par": qk.tolist(), "chi2": c2, "ndf": nd, "neff": ne}
    print(f"u [{cuts[k]:.3f},{cuts[k+1]:.3f}] neff {ne:8.0f}  mu {qk[0]:.3f} sig {qk[1]:.3f} aL {qk[2]:.2f} nL {qk[3]:.1f}"
          f" aR {qk[4]:.2f} nR {qk[5]:.1f} fL {qk[6]:.3f}  chi2/ndf {c2:.0f}/{nd}")
for p in ("ggH", "Signal"):
    ip, up, wp = load(p)
    for k in range(3):
        m = (up >= cuts[k]) & (up <= cuts[k + 1])
        qk, c2, nd, ne = fit(np.bincount(ip[m], weights=wp[m], minlength=NX), np.bincount(ip[m], weights=wp[m] ** 2, minlength=NX))
        res["terciles"][f"{p}_u{cuts[k]:.3f}-{cuts[k+1]:.3f}"] = {"par": qk.tolist(), "chi2": c2, "ndf": nd, "neff": ne}
        print(f"{p:<6} u [{cuts[k]:.3f},{cuts[k+1]:.3f}] neff {ne:7.0f} mu {qk[0]:.3f} sig {qk[1]:.3f} fL {qk[6]:.3f} chi2/ndf {c2:.0f}/{nd}")
os.makedirs(OUT, exist_ok=True)
json.dump(res, open(os.path.join(OUT, "dscb.json"), "w"), indent=1)

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
fig, axs = plt.subplots(2, 2, figsize=(13, 9))
ax = axs[0, 0]
ax.errorbar(XC, h / h.sum() / 0.25, yerr=np.sqrt(h2) / h.sum() / 0.25, fmt=".", ms=2, color="k", label="Higgs MC (all)")
ax.plot(XC, binned(q) / 0.25, color="C3", label=f"DSCB, chi2/ndf {chi2:.0f}/{ndf}")
ax.set_yscale("log"); ax.set_ylim(1e-5, 1); ax.set_xlabel("m4l [GeV]"); ax.legend(fontsize=8)
ax = axs[0, 1]
mm = binned(q) * h.sum()
pull = np.divide(h - mm, np.sqrt(h2), out=np.zeros_like(h), where=h2 > 0)
ax.plot(XC, pull, ".", ms=3); ax.axhline(0, color="k"); ax.set_ylim(-5, 5)
ax.set_xlabel("m4l [GeV]"); ax.set_ylabel("pull (MC - DSCB)/err")
ax = axs[1, 0]
for p in HIGGS:
    ax.plot(XC, binned(np.array(res["procs"][p]["par"])) / 0.25, lw=1, label=p)
ax.set_xlim(115, 135); ax.set_xlabel("m4l [GeV]"); ax.legend(fontsize=7); ax.set_title("DSCB per process")
ax = axs[1, 1]
for k, (lab, r) in enumerate(res["terciles"].items()):
    ax.plot(XC, binned(np.array(r["par"])) / 0.25, lw=1.2, label=lab)
ax.set_xlim(115, 135); ax.set_xlabel("m4l [GeV]"); ax.legend(fontsize=8); ax.set_title("pooled Higgs DSCB in u terciles")
fig.tight_layout()
fig.savefig(os.path.join(OUT, "dscb_fit.png"), dpi=110)
