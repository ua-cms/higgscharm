"""Slide figures for the unbinned fit (2026-10-19): pdf vs binned template; limit comparison."""
import json, sys
import numpy as np
from math import comb
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
W = sys.argv[1]
PD = sys.argv[2] if len(sys.argv) > 2 else "out"
TAG = sys.argv[3] if len(sys.argv) > 3 else ""
P = json.load(open(f"{PD}/pdfs.json")); E = np.array(P["u_edges"]); x = np.linspace(0.0005, 0.9995, 1000)
def dens(s):
    n, c = s["order"], np.array(s["coef"]); i = np.arange(n + 1)
    return (np.array([comb(n, k) for k in i])[None, :] * x[:, None] ** i * (1 - x[:, None]) ** (n - i)) @ c
plt.rcParams.update({"font.size": 13})
fig, axs = plt.subplots(2, 2, figsize=(10, 8))
lab = {"Signal": "H+c signal", "ggH": "ggH", "qqZZ": "qq→ZZ", "ZX": "Z+X (data CR)"}
for ax, p in zip(axs.flat, ["Signal", "ggH", "qqZZ", "ZX"]):
    r = P["rates"][p]; d = P["diag"][p]; wb = np.diff(E)
    ax.stairs(r * np.array(d["card_bins"]) / wb, E, color="k", lw=1.6, label="binned (20 bins)")
    ax.plot(x, r * dens(P["pdfs"][p]), color="#c0392b", lw=2.2, label="unbinned pdf")
    ax.set_title(f"{lab[p]}: {r:.3g} events, $n_{{eff}}$ {d['neff']:.0f}", fontsize=13)
    ax.set_xlabel("u = CDF of P(H+c) for S+B mix" if PD == "out_sb" else "u = qqZZ CDF of P(H+c)"); ax.set_xlim(0, 1); ax.set_ylim(bottom=min(0, ax.get_ylim()[0]))
[a.set_ylabel("events / unit u") for a in axs[:, 0]]; axs[0, 0].legend(fontsize=11, loc="upper left")
fig.tight_layout(); fig.savefig(f"{W}/unbinned_pdfs{TAG}.png", dpi=140)

rows = [("Binned, 20 bins (nominal K)", 486.0, 378.5, "#555555"),
        ("MC templates, last bin split finer", None, 358.0, "#888888"),
        ("Unbinned, pdfs in qqZZ-u (first try)", 493.0, 388.0, "#bbbbbb"),
        ("Smooth pdfs in S+B-u, 200 bins", 453.0, 362.5, "#c0392b")]
fig, ax = plt.subplots(figsize=(10, 8))
y = np.arange(len(rows))[::-1]
for yi, (n, full, so, c) in zip(y, rows):
    if full is not None:
        ax.barh(yi + 0.18, full, 0.34, color=c); ax.text(full + 5, yi + 0.18, f"{full:.1f}", va="center", fontsize=13)
    else:
        ax.text(5, yi + 0.18, "(stat only: no systematic templates)", va="center", fontsize=11, color="#444")
    ax.barh(yi - 0.18, so, 0.34, color=c, alpha=0.45)
    ax.text(so + 5, yi - 0.18, f"{so:.1f} (stat only)", va="center", fontsize=12, color="#444")
ax.set_yticks(y); ax.set_yticklabels(["Binned 20 bins\n(nominal K)", "MC templates,\nlast bin split finer", "Unbinned, pdfs in\nqqZZ-u (first try)", "Smooth pdfs in\nS+B-u, 200 bins"])
ax.set_xlim(0, 640); ax.set_xlabel("Expected 95% CL limit on r (median)"); ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout(); fig.savefig(f"{W}/unbinned_limits{TAG}.png", dpi=140)
print("ok")
