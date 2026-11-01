#!/usr/bin/env python3
"""2026-10-28 (user): the three AN mass-sculpting figures (E.1 production, E.2 no-m4l, E.3 mass-scaled leptons) with
the SAME binning (1 GeV) and the SAME axes: qqZZ + ggZZ m4l in 90-160 GeV, weighted to the expected yields, unit area
in 90-160, for p_sig > 0, 0.5, 0.7, 0.8, 0.9 (style of Fig. 65 / plot_an_style.py). Common y range = max over the three
(incl. error bars) x 1.1, from curves with >= 100 MC events; curves with < 20 MC events are not drawn (massinvlep p_sig > 0.9: 1 event). Inputs: Analysis/nom4l_card_2026_10_27/<tag>/sculpting/events_{qqZZ,ggZZ}.npz."""
import numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import mplhep as hep
plt.style.use(hep.style.CMS)
A = "/eos/user/s/snandaku/Analysis/nom4l_card_2026_10_27"
TAGS = {"prod": "prod_jetfix", "nom4l": "nom4l9016_jetfix", "massinvlep": "massinvlep"}
TITLE = {"prod": "Nominal classifier", "nom4l": r"Without $m_{4\ell}$", "massinvlep": "Mass-scaled leptons"}
CUTS = [0.0, 0.5, 0.7, 0.8, 0.9]
STY = [("black", "-"), ("blue", "-"), ("green", "-"), ("orange", "--"), ("red", "--")]
edges = np.arange(90, 161, 1.0)
H = {}
for t, d in TAGS.items():
    ev = {p: np.load(f"{A}/{d}/sculpting/events_{p}.npz") for p in ("qqZZ", "ggZZ")}
    m = np.concatenate([ev[p]["zz_mass_inclusive"] for p in ev])
    s = np.concatenate([ev[p]["mva_score_Signal"] for p in ev])
    w = np.concatenate([ev[p]["w"] for p in ev])
    win = (m >= 90) & (m < 160)
    H[t] = []
    for c in CUTS:
        k = win & (s > c)
        h, _ = np.histogram(m[k], edges, weights=w[k])
        h2, _ = np.histogram(m[k], edges, weights=w[k] ** 2)
        H[t].append((h / h.sum(), np.sqrt(h2) / h.sum(), int(k.sum())))
ymax = 1.1 * max(float(np.max(v + e)) for t in H for v, e, n in H[t] if n >= 100)
for t in TAGS:
    fig, ax = plt.subplots(figsize=(10, 9))
    for (v, e, n), c, (col, ls) in zip(H[t], CUTS, STY):
        if n < 20:
            ax.plot([], [], color=col, ls=ls, lw=2.5, label=f"Score > {c:.1f} (N={n}, not drawn)"); continue
        hep.histplot(v, edges, yerr=e, ax=ax, color=col, ls=ls, lw=2.5, label=f"Score > {c:.1f} (N={n})")
    ax.set_xlim(90, 160); ax.set_ylim(0, ymax)
    ax.set_xlabel(r"$m_{4\ell}$ [GeV]"); ax.set_ylabel("Fraction of events / 1 GeV")
    hl = sorted(zip(*ax.get_legend_handles_labels()), key=lambda x: float(x[1].split("> ")[1].split(" ")[0]))
    ax.legend(*zip(*hl), fontsize=16, loc="upper left", title=f"ZZ background, {TITLE[t]}", title_fontsize=17)
    hep.cms.label(ax=ax, data=False, label="Preliminary", rlabel=r"61.9 fb$^{-1}$ (13.6 TeV)", fontsize=20)
    fig.savefig(f"mass_sculpting_zz_1gev_{t}.png", dpi=110, bbox_inches="tight")
    fig.savefig(f"mass_sculpting_zz_1gev_{t}.pdf", bbox_inches="tight")
    plt.close(fig)
    pk = [float(v[(edges[:-1] >= 115) & (edges[:-1] < 135)].max()) for v, _, _ in H[t]]
    print(t, "N:", [n for _, _, n in H[t]], "peak fraction / 1 GeV in 115-135:", [round(x, 3) for x in pk])
print("common ymax", round(ymax, 3))
