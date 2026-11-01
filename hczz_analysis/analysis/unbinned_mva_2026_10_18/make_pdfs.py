#!/usr/bin/env python3
"""Step 1 of the unbinned MVA-score fit (2026-10-18): smooth per-process pdfs in
u = qqZZ-weighted CDF of mva_score_Signal (the variable the binned card's 20 quantile bins
are uniform in, so u in [0,1] and qqZZ is flat in it).

Per process (and Z+X):
  * nominal density: weighted Gaussian KDE in u (linear binning on 4000 cells, reflection at
    u = 0 and 1, Silverman bandwidth from n_eff = (sum w)^2 / sum w^2), then a Bernstein
    polynomial with non-negative coefficients fitted to it (NNLS), so the pdf is analytic,
    positive and continuous;
  * every shape systematic of the binned card: density_nom(u) x R(u), R = the card's
    Up/nominal (Down/nominal) template ratio per bin, interpolated linearly between bin
    centres; normalised, Bernstein again. The rate part is the card's integral ratio
    (kappa), written for ProcessNormalization in step 2. So the systematics are the binned
    card's, at its 20-bin resolution; only the nominal shapes are unbinned.
Inputs: events npz from dump_events.py and the binned card written by the same build.
Output: pdfs.json (+ validation png).
Usage: python3 make_pdfs.py <events.npz> <binned card dir> <out dir>
"""
import json
import os
import re
import sys

import numpy as np
import uproot
from scipy.optimize import nnls
from scipy.special import comb

NPZ, CARD_DIR, OUT = sys.argv[1:4]
# transform reference (2026-10-19 check): qqZZ (default) | bkg (all backgrounds incl. Z+X) |
# sb (equal mix of the normalised signal and total-background shapes)
REF = sys.argv[4] if len(sys.argv) > 4 else "qqZZ"
NCELL = 4000
GRID = (np.arange(NCELL) + 0.5) / NCELL
MAX_ORDER, TOL = 60, 0.02  # step 2 sets forceNumInt: RooBernstein analytic integral (ROOT 6.30) is wrong above n ~ 22; eval is exact

ev = np.load(NPZ, allow_pickle=False)
edges = ev["mva_bins"]
nb = len(edges) - 1


def to_u_fn():
    """Score -> u: weighted CDF of the REF sample (|w|, sorted scores, cumsum). For REF = qqZZ
    this is compute_quantile_bins' construction, so u(edges) = k/20. Any monotonic u gives the
    same likelihood ratio (common Jacobian); only the pdf estimation differs."""
    procs = [str(p) for p in ev["procs"]]
    if REF == "qqZZ":
        parts = [(ev["qqZZ|score"], np.abs(ev["qqZZ|weight"]))]
    else:
        bk = [(ev[f"{p}|score"], np.abs(ev[f"{p}|weight"])) for p in procs if p != "Signal"]
        bk.append((ev["zx_score"], np.abs(ev["zx_weight"])))
        sb, wb = np.concatenate([a for a, _ in bk]), np.concatenate([b for _, b in bk])
        parts = [(sb, wb / wb.sum())]
        if REF == "sb":
            ws = np.abs(ev["Signal|weight"])
            parts.append((ev["Signal|score"], ws / ws.sum()))
    s = np.concatenate([a for a, _ in parts])
    w = np.concatenate([b for _, b in parts])
    o = np.argsort(s)
    s, c = s[o], np.cumsum(w[o])
    c /= c[-1]
    s, i = np.unique(s, return_index=True)
    c = c[i]
    return lambda x: np.clip(np.interp(x, s, c), 0.0, 1.0)


to_u = to_u_fn()
u_edges = to_u(edges)
u_edges[0], u_edges[-1] = 0.0, 1.0


def kde(u, w):
    """Weighted Gaussian KDE on GRID with reflection at both ends (density, integral 1)."""
    h_cells, _ = np.histogram(u, bins=NCELL, range=(0, 1), weights=w)
    sw, sw2 = w.sum(), (w ** 2).sum()
    neff = sw ** 2 / sw2
    mu = np.average(u, weights=np.abs(w))
    sd = np.sqrt(np.average((u - mu) ** 2, weights=np.abs(w)))
    bw = max(0.9 * sd * neff ** -0.2, 2.0 / NCELL)
    ext = np.concatenate([h_cells[::-1], h_cells, h_cells[::-1]])
    k = int(np.ceil(5 * bw * NCELL))
    x = np.arange(-k, k + 1) / NCELL
    g = np.exp(-0.5 * (x / bw) ** 2)
    sm = np.convolve(ext, g / g.sum(), mode="same")[NCELL:2 * NCELL]
    sm = np.clip(sm, 0, None)
    return sm / (sm.sum() / NCELL), bw, neff


BASIS = {}


def basis(n):
    if n not in BASIS:
        i = np.arange(n + 1)
        BASIS[n] = comb(n, i)[None, :] * GRID[:, None] ** i * (1 - GRID[:, None]) ** (n - i)
    return BASIS[n]


def bern_fit(dens):
    """Lowest order n <= MAX_ORDER whose non-negative Bernstein fit stays within TOL of the
    KDE (relative to the density maximum) everywhere; returns normalised coefficients
    (RooBernstein convention: f = sum c_i b_{i,n}; integral of b_{i,n} = 1/(n+1))."""
    best = None
    for n in range(4, MAX_ORDER + 1):
        B = basis(n)
        c, _ = nnls(B, dens)
        dev = np.max(np.abs(B @ c - dens)) / dens.max()
        best = (n, c, dev)
        if dev < TOL:
            break
    n, c, dev = best
    c = c / (c.sum() / (n + 1))
    return n, c, dev


def interp_ratio(r):
    """Per-bin template ratio -> R(u) on GRID, linear between bin centres (flat outside)."""
    cen = 0.5 * (u_edges[1:] + u_edges[:-1])
    return np.interp(GRID, cen, r)


# --- the binned card: processes, rates, which shape systematics act on which process
card = open(os.path.join(CARD_DIR, "datacard_mva_with_zx.txt")).read().splitlines()
procs = rates = None
shape_rows = {}
for l in card:
    t = l.split()
    if not t:
        continue
    if t[0] == "process" and procs is None:
        procs = t[1:]
    elif t[0] == "rate":
        rates = [float(x) for x in t[1:]]
    elif len(t) > 2 and t[1] == "shape":
        shape_rows[t[0]] = [p for p, v in zip(procs, t[2:]) if v != "-"]
root = uproot.open(os.path.join(CARD_DIR, "histograms_mva_with_zx.root"))
H = {k.split(";")[0]: v.values() for k, v in root.items() if k.startswith("h_")}

out = {"u_edges": u_edges.tolist(), "procs": procs, "rates": dict(zip(procs, rates)),
       "pdfs": {}, "kappa": {}, "diag": {}}
for proc in procs:
    if proc == "ZX":
        u, w = to_u(ev["zx_score"]), ev["zx_weight"]
    else:
        u, w = to_u(ev[f"{proc}|score"]), ev[f"{proc}|weight"]
    m = (u >= 0) & (u <= 1)
    dens, bw, neff = kde(u[m], w[m])
    n, c, dev = bern_fit(dens)
    hn = H[f"h_{proc}"]
    # closure: pdf integrated in the card's 20 bins vs the card template (shape only)
    B = basis(n) @ c
    pdf_bins = np.array([B[(GRID >= a) & (GRID < b)].sum() / NCELL for a, b in zip(u_edges[:-1], u_edges[1:])])
    tmpl = hn / hn.sum()
    out["pdfs"][proc] = {"order": n, "coef": c.tolist()}
    out["diag"][proc] = {"bw": bw, "neff": neff, "fitdev": dev, "sumw_events": float(w.sum()),
                         "card_rate": float(hn.sum()), "pdf_bins": pdf_bins.tolist(), "card_bins": tmpl.tolist()}
    print(f"{proc:<8} neff {neff:9.1f} bw {bw:.4f} order {n:2d} dev {dev:.3f} "
          f"sumw {w.sum():.5f} card {hn.sum():.5f} "
          f"max|pdf-card|/card(bins>1%) {np.max(np.abs(pdf_bins - tmpl)[tmpl > .01] / tmpl[tmpl > .01]):.3f}")
    for syst, plist in shape_rows.items():
        if proc not in plist:
            continue
        for d in ("Up", "Down"):
            hv = H.get(f"h_{proc}_{syst}{d}")
            if hv is None:
                raise SystemExit(f"missing h_{proc}_{syst}{d}")
            r = np.divide(hv, hn, out=np.ones_like(hn), where=hn != 0)
            dv = np.clip(dens * interp_ratio(r), 0, None)
            dv /= dv.sum() / NCELL
            nv, cv, devv = bern_fit(dv)
            out["pdfs"][f"{proc}_{syst}{d}"] = {"order": nv, "coef": cv.tolist()}
            out["kappa"].setdefault(proc, {}).setdefault(syst, {})[d] = float(hv.sum() / hn.sum())

os.makedirs(OUT, exist_ok=True)
json.dump(out, open(os.path.join(OUT, "pdfs.json"), "w"))
print(f"REF {REF}; u(edges) = {np.round(u_edges, 3).tolist()}")
print(f"{len(out['pdfs'])} pdfs -> {OUT}/pdfs.json; u(edges) - k/{nb}: "
      f"max {np.max(np.abs(u_edges - np.arange(nb + 1) / nb)):.4f}")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
show = [p for p in ["Signal", "qqZZ", "ggH", "ggZZ", "ZX", "VBF"] if p in procs]
fig, axs = plt.subplots(2, 3, figsize=(13, 7))
for ax, p in zip(axs.flat, show):
    d = out["diag"][p]
    wbin = np.diff(u_edges)
    ax.stairs(np.array(d["card_bins"]) / wbin, u_edges, label="binned card (20 bins)", color="k")
    pd_ = out["pdfs"][p]
    ax.plot(GRID, basis(pd_["order"]) @ np.array(pd_["coef"]), label=f"unbinned pdf (Bernstein {pd_['order']})", color="C3")
    ax.set_title(f"{p}  (n_eff {d['neff']:.0f}, bw {d['bw']:.3f})")
    ax.set_xlabel("u = qqZZ CDF of P(H+c)")
    ax.set_ylabel("density")
    ax.legend(fontsize=7)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "pdf_validation.png"), dpi=130)
