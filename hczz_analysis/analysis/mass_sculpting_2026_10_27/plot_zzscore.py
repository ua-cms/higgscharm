#!/usr/bin/env python3
"""m4l of qqZZ, ggZZ, qqZZ+ggZZ (2022+2023, builder normalisation, production model scored_v1) for cuts
on the ZZ MVA score p_ZZ = mva_score_qqZZ + mva_score_ggZZ (> 0, 0.5, 0.7, 0.8, 0.9), in 90-160 and 115-135 GeV."""
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
CUTS = [0.0, 0.5, 0.7, 0.8, 0.9]
STY = [("black", "-"), ("blue", "-"), ("green", "-"), ("orange", "--"), ("red", "--")]
d = {p: np.load(f"events_{p}.npz") for p in ("qqZZ", "ggZZ")}
get = lambda p: [d[p]["zz_mass_inclusive"], d[p]["mva_score_qqZZ"] + d[p]["mva_score_ggZZ"], d[p]["w"]]
sets = {p: get(p) for p in d}
sets["ZZ"] = [np.concatenate([sets[p][i] for p in ("qqZZ", "ggZZ")]) for i in range(3)]
LAB = {"qqZZ": r"q$\bar{q}\to$ZZ", "ggZZ": r"gg$\to$ZZ", "ZZ": r"q$\bar{q}$ZZ + ggZZ"}
rows = []
for (lo, hi, step) in ((90, 160, 2.0), (115, 135, 1.0)):
    edges = np.arange(lo, hi + step, step)
    for p, (m, s, w) in sets.items():
        win = (m >= lo) & (m < hi)
        fig, ax = plt.subplots(figsize=(10, 9))
        for c, (col, ls) in zip(CUTS, STY):
            k = win & (s > c)
            h, _ = np.histogram(m[k], edges, weights=w[k]); h2, _ = np.histogram(m[k], edges, weights=w[k] ** 2)
            hep.histplot(h / h.sum(), edges, yerr=np.sqrt(h2) / h.sum(), ax=ax, color=col, ls=ls, lw=2.5,
                         label=f"ZZ score > {c:.1f} (N={k.sum()}, {w[k].sum():.3g} ev.)")
            f = w[k & (m >= 115) & (m < 135)].sum() / w[k].sum()
            rows.append((f"{lo}-{hi}", p, c, int(k.sum()), w[k].sum(), f))
        ax.axvline(125, color="0.5", ls=":", lw=1)
        ax.set_xlim(lo, hi); ax.set_ylim(0, ax.get_ylim()[1] * 1.45)
        ax.set_xlabel(r"$m_{4\ell}$ [GeV]"); ax.set_ylabel("Normalized")
        ax.legend(fontsize=15, loc="upper right" if lo == 90 else "upper left",
                  title=f"{LAB[p]}, ZZ score (qqZZ+ggZZ)", title_fontsize=17)
        hep.cms.label(ax=ax, data=False, label="Preliminary", rlabel=r"61.9 fb$^{-1}$ (13.6 TeV)", fontsize=20)
        n = f"m4l_{lo}to{hi}_zzscorecuts_{p}"
        fig.savefig(n + ".png", dpi=110, bbox_inches="tight"); fig.savefig(n + ".pdf", bbox_inches="tight"); plt.close(fig)
with open("summary_zzscore.md", "w") as fo:
    fo.write("| m4l range | sample | ZZ score > | raw N | yield | frac in 115-135 |\n|---|---|---|---|---|---|\n")
    for r in rows:
        fo.write(f"| {r[0]} | {r[1]} | {r[2]:.1f} | {r[3]} | {r[4]:.3f} | {r[5]:.3f} |\n")
print(open("summary_zzscore.md").read())
