#!/usr/bin/env python3
"""Step 1 of the unbinned 2D (m4l, MVA score) fit (2026-10-30), for the mass-decorrelated
classifiers (massinvlep DisCo 0.5, fsextv2 DisCo 1). 2D version of
../unbinned_mva_2026_10_18/make_pdfs.py; method after the ATLAS H->ZZ*->4l mass fit
(Higgs 2026 talk, slide 22: unbinned fit in (m4l, NN), ZZ from MC smoothed with Gaussian kernels).

Observables: x = m4l in [105, 160] GeV; y = u = CDF of the equal (signal + total background) mix
of mva_score_Signal (the 10-19 choice; the qqZZ CDF smeared the signal peak).

Per process (and Z+X): weighted 2D Gaussian KDE on a NX x NY grid (0.25 GeV = the dump's m4l
resolution, x 1/NY in u), separable, reflection at all four edges, per-axis bandwidth
h = 0.9 min(sd, IQR/1.34) n_eff^(-1/6) (robust Silverman, d = 2), so the Higgs peak (IQR) is not
broadened by its tails. Signed weights are smoothed as they are and the density clipped at 0.

Systematics are NOT built here: step 2 takes the cat40 card's Up/nominal ratio in each of its 2D
cells (piecewise constant on the grid), as the 1D version did with its 20 bins.

Inputs: events_2d.npz (dump_events_2d.py: score = (i + s)/220 on the 0.25 GeV m4l grid) and the
cat40 card dir (layout.txt, histograms). Output: <out>/pdfs_2d.npz + closure printout + png.
Usage: python3 make_pdfs_2d.py <events_2d.npz> <cat40 card dir> <out dir> [--dscb dscb_cond.json]

--dscb (2026-10-30, ATLAS signal model): every Higgs process becomes
  P(m4l, u) = f_p(u) x [ (1 - fL_p) DSCB(m4l; mu(u), sigma(u), common tails) + fL_p Landau_p(m4l) ]
with f_p(u) the process's KDE marginal in u, the u-conditional DSCB core from dscb_cond.py (fitted to
Signal+ggH+VBF in u slices, linear interpolation) and the per-process non-resonant Landau fraction
(HIG-24-013). Lepton scale/resolution nuisances on the DSCB core (HIG-24-013 per-channel values weighted
with the MC final-state mix 4e/4mu/2e2mu = 0.18/0.36/0.46: scale 0.11%, resolution 7.9%) are stored as
pdf|<proc>|CMS_scale_4l|Up etc. ZZ and Z+X stay 2D KDE (ATLAS: ZZ from MC with Gaussian kernels).
"""
import os
import re
import sys

import numpy as np
import uproot

NPZ, CARD_DIR, OUT = sys.argv[1:4]
DSCB_JSON = sys.argv[sys.argv.index("--dscb") + 1] if "--dscb" in sys.argv else None
HIGGS = ["Signal", "ggH", "VBF", "WminusH", "WplusH", "ZH", "ttH", "HPlusB"]
LEP_SYST = {"CMS_scale_4l": ("mu", 0.0011), "CMS_res_4l": ("sigma", 0.079)}
FINE = np.round(np.arange(105.0, 160.0 + 1e-9, 0.25), 4)
NX, NY = len(FINE) - 1, 200
XE, YE = FINE, np.linspace(0.0, 1.0, NY + 1)
XC, YC = 0.5 * (XE[1:] + XE[:-1]), 0.5 * (YE[1:] + YE[:-1])
DX, DY = 0.25, 1.0 / NY

ev = np.load(NPZ, allow_pickle=False)
procs = [str(p) for p in ev["procs"]]


def decode(v):
    i = np.floor(v * NX).astype(int)
    ok = (v >= 0) & (i >= 0) & (i < NX)
    i = np.clip(i, 0, NX - 1)
    return XC[i], v * NX - i, ok


raw = {}
for p in procs:
    m, s, ok = decode(ev[f"{p}|score"])
    raw[p] = (m[ok], s[ok], ev[f"{p}|weight"][ok])
m, s, ok = decode(ev["zx_score"])
raw["ZX"] = (m[ok], s[ok], ev["zx_weight"][ok])

# score -> u: CDF of (normalised signal + normalised total background), |w|
bk = [raw[p] for p in raw if p != "Signal"]
sb = np.concatenate([b[1] for b in bk])
wb = np.abs(np.concatenate([b[2] for b in bk]))
ss, ws = raw["Signal"][1], np.abs(raw["Signal"][2])
sc = np.concatenate([sb, ss])
wc = np.concatenate([wb / wb.sum(), ws / ws.sum()])
o = np.argsort(sc)
sc, cc = sc[o], np.cumsum(wc[o])
cc /= cc[-1]
sc, iu = np.unique(sc, return_index=True)
cc = cc[iu]


def to_u(x):
    return np.clip(np.interp(x, sc, cc), 0.0, 1.0)


def from_u(u):
    return np.interp(u, cc, sc)


def wquant(x, w, q):
    o = np.argsort(x)
    c = np.cumsum(w[o])
    return np.interp(q * c[-1], c, x[o])


def smooth_axis(h, bw_cells, axis):
    """Gaussian convolution along one axis with reflection at both edges."""
    n = h.shape[axis]
    k = int(np.ceil(5 * bw_cells))
    if k < 1:
        return h
    x = np.arange(-k, k + 1)
    g = np.exp(-0.5 * (x / bw_cells) ** 2)
    g /= g.sum()
    h = np.moveaxis(h, axis, 0)
    pad = min(k, n)
    ext = np.concatenate([h[:pad][::-1], h, h[-pad:][::-1]], axis=0)
    out = np.apply_along_axis(lambda c: np.convolve(c, g, mode="same"), 0, ext)[pad:pad + n]
    return np.moveaxis(out, 0, axis)


def kde2d(mx, uy, w):
    aw = np.abs(w)
    neff = w.sum() ** 2 / (w ** 2).sum()
    bws = []
    for a, d in ((mx, DX), (uy, DY)):
        mu = np.average(a, weights=aw)
        sd = np.sqrt(np.average((a - mu) ** 2, weights=aw))
        iqr = wquant(a, aw, 0.75) - wquant(a, aw, 0.25)
        bws.append(max(0.9 * min(sd, iqr / 1.34) * neff ** (-1.0 / 6.0), d))
    h, _, _ = np.histogram2d(mx, uy, bins=[XE, YE], weights=w)
    sm = smooth_axis(smooth_axis(h, bws[0] / DX, 0), bws[1] / DY, 1)
    sm = np.clip(sm, 0.0, None)
    return sm / (sm.sum() * DX * DY), bws, neff


# --- cat40 card: bin layout + nominal templates for the closure
lay = open(os.path.join(CARD_DIR, "layout.txt")).read()
cats = np.array([float(x) for x in re.search(r"--unroll-score-cats (\S+)", lay).group(1).strip("[]").split(",")])
edges = np.array([float(x) for x in re.search(r"bin-edges (\S+)", lay).group(1).strip("[]").split(",")])
NC = len(cats) - 1
root = uproot.open(os.path.join(CARD_DIR, "histograms_mva_with_zx.root"))
# grid cell -> card bin (cell centre; card variable (j + (m4l-105)/55)/NC)
sgrid = from_u(YC)
j = np.clip(np.searchsorted(cats, sgrid, side="right") - 1, 0, NC - 1)
var = (j[None, :] + (XC[:, None] - 105.0) / 55.0) / NC
cellbin = np.clip(np.searchsorted(edges, var, side="right") - 1, 0, len(edges) - 2)

out = {"xe": XE, "ye": YE, "u_s": sc, "u_c": cc, "cellbin": cellbin, "card_edges": edges, "cats": cats}
if DSCB_JSON:
    import json
    from scipy.stats import landau
    DJ = json.load(open(DSCB_JSON))
    TAILS = DJ["tails"]
    MU_Y = np.interp(YC, DJ["u_nodes"], DJ["mu_nodes"])
    SG_Y = np.interp(YC, DJ["u_nodes"], DJ["sigma_nodes"])
    GX = np.linspace(105.0, 160.0, NX * 8 + 1)
    GXC = 0.5 * (GX[1:] + GX[:-1])

    def dscb(x, mu, sig, aL, nL, aR, nR):
        t = (x - mu) / sig
        o = np.exp(-0.5 * t * t)
        lo, hi = t < -aL, t > aR
        o[lo] = (nL / aL) ** nL * np.exp(-0.5 * aL * aL) * (nL / aL - aL - t[lo]) ** (-nL)
        o[hi] = (nR / aR) ** nR * np.exp(-0.5 * aR * aR) * (nR / aR - aR + t[hi]) ** (-nR)
        return o

    def core_grid(mu_scale=1.0, sig_scale=1.0):
        """[NX, NY]: DSCB core in m4l (bin-integrated, normalised on 105-160) for each u cell."""
        g = np.empty((NX, NY))
        for k in range(NY):
            f = dscb(GXC, MU_Y[k] * mu_scale, SG_Y[k] * sig_scale, *TAILS).reshape(NX, 8).mean(1)
            g[:, k] = f / (f.sum() * DX)
        return g

    CORE = {"nom": core_grid()}
    for sy, (par, dl) in LEP_SYST.items():
        for dname, sgn in (("Up", 1), ("Down", -1)):
            CORE[f"{sy}|{dname}"] = core_grid(1 + sgn * dl, 1.0) if par == "mu" else core_grid(1.0, 1 + sgn * dl)


def higgs_pdf(p, marg_u, core):
    nr = DJ["nonres"][p]
    lnd = landau.pdf(GXC, loc=nr["loc"], scale=nr["scale"]).reshape(NX, 8).mean(1)
    lnd /= lnd.sum() * DX
    fl = nr["fL"] if nr["fL"] > 3 * (nr["fL_err"] if np.isfinite(nr["fL_err"]) else 1.0) else 0.0
    d = marg_u[None, :] * ((1 - fl) * core + fl * lnd[:, None])
    return d / (d.sum() * DX * DY)
rows = []
for p in procs + ["ZX"]:
    mx, s, w = raw[p]
    dens, bws, neff = kde2d(mx, to_u(s), w)
    if DSCB_JSON and p in HIGGS:
        mu_marg = dens.sum(0) * DX  # KDE marginal in u (integral 1)
        dens = higgs_pdf(p, mu_marg, CORE["nom"])
        for key in CORE:
            if key != "nom":
                sy, dname = key.split("|")
                out[f"pdf|{p}|{sy}|{dname}"] = higgs_pdf(p, mu_marg, CORE[key])
    out[f"pdf|{p}"] = dens.astype(np.float64)
    hn = root[f"h_{p}"].values()
    tmpl = hn / hn.sum()
    pb = np.bincount(cellbin.ravel(), weights=(dens * DX * DY).ravel(), minlength=len(hn))
    big = tmpl > 0.01
    dev = np.max(np.abs(pb - tmpl)[big] / tmpl[big])
    # in-cell resolution check: fraction of the KDE pdf vs MC events in the top-u 5% of the grid
    top_kde = dens[:, YC > 0.95].sum() * DX * DY
    top_mc = w[to_u(s) > 0.95].sum() / w.sum()
    rows.append(f"{p:<8} n {len(w):>7} neff {neff:9.0f} bw m4l {bws[0]:.3f} GeV u {bws[1]:.4f} "
                f"sumw {w.sum():.5f} card {hn.sum():.5f} max|pdf-card|/card(cells>1%) {dev:.3f} "
                f"u>0.95: kde {top_kde:.4f} mc {top_mc:.4f}")
    print(rows[-1], flush=True)

os.makedirs(OUT, exist_ok=True)
np.savez_compressed(os.path.join(OUT, "pdfs_2d.npz"), procs=np.array(procs + ["ZX"]),
                    model=np.array("dscb" if DSCB_JSON else "kde"), **out)
open(os.path.join(OUT, "closure.txt"), "w").write("\n".join(rows) + "\n")
print(f"-> {OUT}/pdfs_2d.npz ({NX} x {NY} grid)")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
show = ["Signal", "ggH", "qqZZ", "ggZZ", "ZX", "VBF"]
fig, axs = plt.subplots(3, 4, figsize=(17, 11))
for k, p in enumerate(show):
    d = out[f"pdf|{p}"]
    mx, s, w = raw[p]
    u = to_u(s)
    ax = axs.flat[2 * k]
    hm, _ = np.histogram(mx, bins=np.arange(105, 160.1, 1.0), weights=w)
    ax.stairs(hm / w.sum() / 1.0, np.arange(105, 160.1, 1.0), color="k", label="MC (1 GeV)")
    ax.plot(XC, d.sum(1) * DY, color="C3", label="KDE")
    ax.set_title(f"{p}: m4l")
    ax.set_xlabel("m4l [GeV]")
    ax.legend(fontsize=7)
    ax = axs.flat[2 * k + 1]
    hu, _ = np.histogram(u, bins=np.linspace(0, 1, 51), weights=w)
    ax.stairs(hu / w.sum() * 50, np.linspace(0, 1, 51), color="k", label="MC (50 bins)")
    ax.plot(YC, d.sum(0) * DX, color="C3", label="KDE")
    ax.set_title(f"{p}: u")
    ax.set_xlabel("u = S+B CDF of p_sig")
    ax.legend(fontsize=7)
fig.tight_layout()
fig.savefig(os.path.join(OUT, "pdf_validation_2d.png"), dpi=110)
