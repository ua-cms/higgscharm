#!/usr/bin/env python3
"""AN-style mass-sculpting plot (as AN/Appendix/Plots/mva_mass_sculpting_zz.png): qqZZ + ggZZ m4l in
90-160 GeV, unit area, for p_sig > 0, 0.5, 0.7, 0.8, 0.9; production score (scored_v1), 2022+2023.
Two versions: weighted (builder normalisation) and unweighted (raw MC events, like the AN legend N)."""
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
CUTS = [0.0, 0.5, 0.7, 0.8, 0.9]
STY = [("black", "-"), ("blue", "-"), ("green", "-"), ("orange", "--"), ("red", "--")]
d = {p: np.load(f"events_{p}.npz") for p in ("qqZZ", "ggZZ")}
m = np.concatenate([d[p]["zz_mass_inclusive"] for p in d])
s = np.concatenate([d[p]["mva_score_Signal"] for p in d])
w = np.concatenate([d[p]["w"] for p in d])
win = (m >= 90) & (m < 160)
edges = np.arange(90, 162, 2.0)
for tag, weighted in (("weighted", True), ("unweighted", False)):
    for logy in (False, True):
        fig, ax = plt.subplots(figsize=(10, 9))
        for c, (col, ls) in zip(CUTS, STY):
            k = win & (s > c)
            ww = w[k] if weighted else np.ones(k.sum())
            h, _ = np.histogram(m[k], edges, weights=ww)
            h2, _ = np.histogram(m[k], edges, weights=ww ** 2)
            hep.histplot(h / h.sum(), edges, yerr=np.sqrt(h2) / h.sum(), ax=ax, color=col, ls=ls, lw=2.5,
                         label=f"Score > {c:.1f} (N={k.sum()})")
        ax.set_xlim(90, 160); ax.set_xlabel(r"$m_{4\ell}$ [GeV]"); ax.set_ylabel("Normalized")
        if logy: ax.set_yscale("log"); ax.set_ylim(1e-4, 3)
        ax.legend(fontsize=16, loc="upper left" if not logy else "upper right",
                  title=f"ZZ Background ({'weighted' if weighted else 'raw MC'})", title_fontsize=18)
        hep.cms.label(ax=ax, data=False, label="Preliminary", rlabel=r"61.9 fb$^{-1}$ (13.6 TeV)" if weighted else "13.6 TeV", fontsize=20)
        name = f"an_style_mass_sculpting_zz_{tag}{'_log' if logy else ''}"
        fig.savefig(name + ".png", dpi=110, bbox_inches="tight"); fig.savefig(name + ".pdf", bbox_inches="tight")
        plt.close(fig)
print("done")
