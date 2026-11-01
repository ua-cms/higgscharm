#!/usr/bin/env python3
"""m4l in 115-135 GeV of qqZZ, ggZZ and qqZZ+ggZZ (2022+2023, builder normalisation, production score
scored_v1) for p_sig > 0, 0.5, 0.7, 0.8, 0.9 (AN thresholds). Unit area, 1 GeV bins; legend: raw N, yield."""
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
CUTS = [0.0, 0.5, 0.7, 0.8, 0.9]
STY = [("black", "-"), ("blue", "-"), ("green", "-"), ("orange", "--"), ("red", "--")]
d = {p: np.load(f"events_{p}.npz") for p in ("qqZZ", "ggZZ")}
sets = {p: [d[p][c] for c in ("zz_mass_inclusive", "mva_score_Signal", "w")] for p in d}
sets["ZZ"] = [np.concatenate([sets[p][i] for p in ("qqZZ", "ggZZ")]) for i in range(3)]
LAB = {"qqZZ": r"q$\bar{q}\to$ZZ", "ggZZ": r"gg$\to$ZZ", "ZZ": r"q$\bar{q}$ZZ + ggZZ"}
edges = np.arange(115, 136, 1.0)
rows = []
for p, (m, s, w) in sets.items():
    win = (m >= 115) & (m < 135)
    fig, ax = plt.subplots(figsize=(10, 9))
    for c, (col, ls) in zip(CUTS, STY):
        k = win & (s > c)
        h, _ = np.histogram(m[k], edges, weights=w[k]); h2, _ = np.histogram(m[k], edges, weights=w[k] ** 2)
        hep.histplot(h / h.sum(), edges, yerr=np.sqrt(h2) / h.sum(), ax=ax, color=col, ls=ls, lw=2.5,
                     label=f"Score > {c:.1f} (N={k.sum()}, {w[k].sum():.3g} ev.)")
        mean = np.average(m[k], weights=w[k]); rms = np.sqrt(np.average((m[k] - mean) ** 2, weights=w[k]))
        rows.append((p, c, int(k.sum()), w[k].sum(), mean, rms))
    ax.axvline(125, color="0.5", ls=":", lw=1)
    ax.set_xlim(115, 135); ax.set_ylim(0, ax.get_ylim()[1] * 1.45)
    ax.set_xlabel(r"$m_{4\ell}$ [GeV]"); ax.set_ylabel("Normalized")
    ax.legend(fontsize=15, loc="upper left", title=f"{LAB[p]}, H+c score", title_fontsize=17)
    hep.cms.label(ax=ax, data=False, label="Preliminary", rlabel=r"61.9 fb$^{-1}$ (13.6 TeV)", fontsize=20)
    fig.savefig(f"m4l_115to135_scorecuts_{p}.png", dpi=110, bbox_inches="tight")
    fig.savefig(f"m4l_115to135_scorecuts_{p}.pdf", bbox_inches="tight"); plt.close(fig)
with open("summary_115to135.md", "w") as fo:
    fo.write("| sample | score > | raw N | yield | mean m4l | RMS |\n|---|---|---|---|---|---|\n")
    for r in rows:
        fo.write(f"| {r[0]} | {r[1]:.1f} | {r[2]} | {r[3]:.3f} | {r[4]:.2f} | {r[5]:.2f} |\n")
print(open("summary_115to135.md").read())
