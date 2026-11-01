#!/usr/bin/env python3
"""m_Z1 of qqZZ + ggZZ (2022+2023, production score scored_v1) for p_sig > 0, 0.5, 0.7, 0.8, 0.9,
for all selected events and for 90 <= m4l < 160 (the AN plot's window). Unit area."""
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
CUTS = [0.0, 0.5, 0.7, 0.8, 0.9]
STY = [("black", "-"), ("blue", "-"), ("green", "-"), ("orange", "--"), ("red", "--")]
d = {p: np.load(f"events_{p}.npz") for p in ("qqZZ", "ggZZ")}
cat = lambda c: np.concatenate([d[p][c] for p in d])
m4l, z1, s, w = cat("zz_mass_inclusive"), cat("z1_mass"), cat("mva_score_Signal"), cat("w")
edges = np.arange(40, 122, 2.0)
for tag, sel, txt in (("all", np.ones_like(m4l, bool), "all m4l"), ("m4l90to160", (m4l >= 90) & (m4l < 160), r"$90<m_{4\ell}<160$ GeV")):
    fig, ax = plt.subplots(figsize=(10, 9))
    for c, (col, ls) in zip(CUTS, STY):
        k = sel & (s > c)
        h, _ = np.histogram(z1[k], edges, weights=w[k]); h2, _ = np.histogram(z1[k], edges, weights=w[k] ** 2)
        hep.histplot(h / h.sum(), edges, yerr=np.sqrt(h2) / h.sum(), ax=ax, color=col, ls=ls, lw=2.5,
                     label=f"Score > {c:.1f} (N={k.sum()})")
    ax.axvline(91.19, color="0.5", ls=":", lw=1)
    ax.set_xlim(40, 120); ax.set_xlabel(r"$m_{Z_1}$ [GeV]"); ax.set_ylabel("Normalized")
    ax.legend(fontsize=16, loc="upper left", title=f"ZZ Background, {txt}", title_fontsize=17)
    hep.cms.label(ax=ax, data=False, label="Preliminary", rlabel=r"61.9 fb$^{-1}$ (13.6 TeV)", fontsize=20)
    fig.savefig(f"z1_mass_sculpting_zz_{tag}.png", dpi=110, bbox_inches="tight"); fig.savefig(f"z1_mass_sculpting_zz_{tag}.pdf", bbox_inches="tight"); plt.close(fig)
print("done")
