#!/usr/bin/env python3
"""m4l of qqZZ / ggZZ (2022+2023, 62.4/fb... builder normalisation) in slices of the production SM H+c
MVA score (mva_score_Signal): does a score cut sculpt a peak at 125 GeV?"""
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
CUTS = [0.0, 0.1, 0.3, 0.5, 0.7]
COL = [matplotlib.colors.to_hex(c) for c in plt.cm.Blues(np.linspace(0.35, 1.0, len(CUTS)))]
LUMI = 61.9  # 2022+2023 [fb-1], builder LUMINOSITY_PB

def load(p):
    d = np.load(f"events_{p}.npz")
    return d["zz_mass_inclusive"], d["mva_score_Signal"], d["w"]

def label(ax, txt):
    hep.cms.label(ax=ax, data=False, label="Preliminary", rlabel=f"{LUMI} fb$^{{-1}}$ (13.6 TeV)", fontsize=18)
    ax.text(0.04, 0.95, txt, transform=ax.transAxes, va="top", fontsize=17)

summary = []
for p in ("qqZZ", "ggZZ"):
    m, s, w = load(p)
    for rng, step, tag in (((70, 300), 2.5, "wide"), ((100, 170), 1.0, "zoom")):
        edges = np.arange(rng[0], rng[1] + step, step)
        # 1) unit-normalised shapes, linear and log
        for logy in (False, True):
            fig, ax = plt.subplots(figsize=(10, 8))
            for c, col in zip(CUTS, COL):
                k = s > c
                h, _ = np.histogram(m[k], edges, weights=w[k])
                h2, _ = np.histogram(m[k], edges, weights=w[k] ** 2)
                n = h.sum()
                hep.histplot(h / n, edges, yerr=np.sqrt(h2) / n, ax=ax, color=col, lw=2,
                             label=f"score > {c:g} ({w[k].sum():.2f} ev.)" if c else f"all ({w.sum():.1f} ev.)")
            ax.axvline(125, color="0.5", ls=":", lw=1)
            ax.set_xlabel(r"$m_{4\ell}$ [GeV]"); ax.set_ylabel("Fraction of events / bin")
            ax.set_xlim(*rng)
            if logy: ax.set_yscale("log"); ax.set_ylim(1e-5, 5)
            ax.legend(fontsize=15, loc="upper right", title=f"{p}, H+c score", title_fontsize=15)
            label(ax, "")
            fig.savefig(f"m4l_shape_{p}_{tag}{'_log' if logy else ''}.png", dpi=110, bbox_inches="tight")
            fig.savefig(f"m4l_shape_{p}_{tag}{'_log' if logy else ''}.pdf", bbox_inches="tight"); plt.close(fig)
        # 2) efficiency of the score cut vs m4l
        fig, ax = plt.subplots(figsize=(10, 8))
        den, _ = np.histogram(m, edges, weights=w)
        for c, col in zip(CUTS[1:], COL[1:]):
            k = s > c
            num, _ = np.histogram(m[k], edges, weights=w[k])
            num2, _ = np.histogram(m[k], edges, weights=w[k] ** 2)
            eff = np.divide(num, den, out=np.zeros_like(num), where=den > 0)
            err = np.divide(np.sqrt(num2), den, out=np.zeros_like(num), where=den > 0)
            hep.histplot(eff, edges, yerr=err, ax=ax, color=col, lw=2, label=f"score > {c:g}")
        ax.axvline(125, color="0.5", ls=":", lw=1)
        ax.set_xlabel(r"$m_{4\ell}$ [GeV]"); ax.set_ylabel("Fraction passing score cut")
        ax.set_xlim(*rng); ax.set_yscale("log"); ax.set_ylim(1e-5, 2)
        ax.legend(fontsize=15, loc="upper right", title=p, title_fontsize=15); label(ax, "")
        fig.savefig(f"eff_vs_m4l_{p}_{tag}.png", dpi=110, bbox_inches="tight")
        fig.savefig(f"eff_vs_m4l_{p}_{tag}.pdf", bbox_inches="tight"); plt.close(fig)
    # 3) mean score vs m4l and 2D
    edges = np.arange(70, 302.5, 2.5)
    sw, _ = np.histogram(m, edges, weights=w * s); ww, _ = np.histogram(m, edges, weights=w)
    fig, ax = plt.subplots(figsize=(10, 8))
    hep.histplot(np.divide(sw, ww, out=np.zeros_like(sw), where=ww > 0), edges, ax=ax, color=COL[-1], lw=2)
    ax.axvline(125, color="0.5", ls=":", lw=1)
    ax.set_xlabel(r"$m_{4\ell}$ [GeV]"); ax.set_ylabel("Mean H+c score"); ax.set_xlim(70, 300)
    label(ax, p); fig.savefig(f"mean_score_vs_m4l_{p}.png", dpi=110, bbox_inches="tight")
    fig.savefig(f"mean_score_vs_m4l_{p}.pdf", bbox_inches="tight"); plt.close(fig)
    fig, ax = plt.subplots(figsize=(11, 8))
    H, xe, ye = np.histogram2d(m, s, [np.arange(70, 302.5, 2.5), np.linspace(0, 1, 41)], weights=w)
    H = H / H.sum(axis=1, keepdims=True).clip(1e-30)   # per m4l column: score distribution
    pc = ax.pcolormesh(xe, ye, np.where(H > 0, H, np.nan).T, cmap="Blues", norm=matplotlib.colors.LogNorm(1e-5, 1))
    fig.colorbar(pc, ax=ax, label="Fraction of events in m4l column")
    ax.set_xlabel(r"$m_{4\ell}$ [GeV]"); ax.set_ylabel("H+c score"); label(ax, p)
    fig.savefig(f"score_vs_m4l_2d_{p}.png", dpi=110, bbox_inches="tight"); plt.close(fig)
    # numbers
    for c in CUTS:
        k = s > c; t = w[k].sum()
        f = lambda lo, hi: w[k & (m >= lo) & (m < hi)].sum() / t
        summary.append((p, c, int(k.sum()), t, f(70, 105), f(105, 115), f(115, 135), f(135, 160), f(160, 1e9)))
with open("summary.md", "w") as fo:
    fo.write("| process | score > | raw | yield | 70-105 | 105-115 | 115-135 | 135-160 | >160 |\n|" + "---|" * 9 + "\n")
    for r in summary:
        fo.write(f"| {r[0]} | {r[1]:g} | {r[2]} | {r[3]:.3f} | " + " | ".join(f"{x:.3f}" for x in r[4:]) + " |\n")
print(open("summary.md").read())
