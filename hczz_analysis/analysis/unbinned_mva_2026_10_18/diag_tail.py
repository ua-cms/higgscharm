"""Per representation (u = CDF of qqZZ / bkg / sb): expected signal and background above score
thresholds in the original score, from the Bernstein pdfs, vs the MC (and Z+X CR) events."""
import json, sys
import numpy as np
from math import comb
sys.argv = ["x", "events_K_nocapfix.npz", "card_binned_K_nocapfix", "/tmp/x"]
ev = np.load("events_K_nocapfix.npz")
procs = [str(p) for p in ev["procs"]]
def cdf(ref):
    if ref == "qqZZ":
        parts = [(ev["qqZZ|score"], np.abs(ev["qqZZ|weight"]))]
    else:
        bk = [(ev[f"{p}|score"], np.abs(ev[f"{p}|weight"])) for p in procs if p != "Signal"] + [(ev["zx_score"], np.abs(ev["zx_weight"]))]
        sb, wb = np.concatenate([a for a, _ in bk]), np.concatenate([b for _, b in bk]); parts = [(sb, wb / wb.sum())]
        if ref == "sb":
            ws = np.abs(ev["Signal|weight"]); parts.append((ev["Signal|score"], ws / ws.sum()))
    s = np.concatenate([a for a, _ in parts]); w = np.concatenate([b for _, b in parts]); o = np.argsort(s)
    s, c = s[o], np.cumsum(w[o]); c /= c[-1]; s, i = np.unique(s, return_index=True)
    return lambda x: np.clip(np.interp(x, s, c[i]), 0, 1)
x = np.linspace(0, 1, 200001)
def tail(spec, u0):
    n, c = spec["order"], np.array(spec["coef"]); xx = x[x >= u0]; i = np.arange(n + 1)
    d = (np.array([comb(n, k) for k in i])[None, :] * xx[:, None] ** i * (1 - xx[:, None]) ** (n - i)) @ c
    return np.trapz(d, xx)
edges = ev["mva_bins"]
qz = cdf("qqZZ")
# thresholds: score at qqZZ-u = 0.90, 0.95, 0.975, 0.99
thr = np.interp([0.90, 0.95, 0.975, 0.99], qz(np.sort(ev["qqZZ|score"])), np.sort(ev["qqZZ|score"]))
bkgp = [p for p in procs if p != "Signal"] + ["ZX"]
print("score thresholds", np.round(thr, 4))
def mc(p, t):
    s, w = (ev["zx_score"], ev["zx_weight"]) if p == "ZX" else (ev[f"{p}|score"], ev[f"{p}|weight"])
    m = s >= t; return w[m].sum(), np.sqrt((w[m] ** 2).sum())
print(f"{'':10}" + "".join(f"  qqZZ-u>{q:<6}" for q in [0.90, 0.95, 0.975, 0.99]))
for lab in ["MC", "qqZZ", "bkg", "sb"]:
    S, B, ZX, Q = [], [], [], []
    for t in thr:
        if lab == "MC":
            S.append(mc("Signal", t)[0]); B.append(sum(mc(p, t)[0] for p in bkgp)); ZX.append(mc("ZX", t)[0]); Q.append(mc("qqZZ", t)[0])
        else:
            P = json.load(open({"qqZZ": "out", "bkg": "out_bkg", "sb": "out_sb"}[lab] + "/pdfs.json"))
            u0 = cdf(lab)(np.array([t]))[0]; r = P["rates"]
            S.append(r["Signal"] * tail(P["pdfs"]["Signal"], u0)); ZX.append(r["ZX"] * tail(P["pdfs"]["ZX"], u0))
            Q.append(r["qqZZ"] * tail(P["pdfs"]["qqZZ"], u0)); B.append(sum(r[p] * tail(P["pdfs"][p], u0) for p in bkgp))
    print(f"{lab:5} S*1e3 " + " ".join(f"{1e3*v:7.2f}" for v in S) + " | B " + " ".join(f"{v:6.2f}" for v in B) + " | ZX " + " ".join(f"{v:5.2f}" for v in ZX) + " | qqZZ " + " ".join(f"{v:5.2f}" for v in Q))
for t in thr:
    print(f"MC stat at score>{t:.4f}: ZX {mc('ZX', t)[0]:.2f}±{mc('ZX', t)[1]:.2f}  n_ZXevents {(ev['zx_score']>=t).sum()}  Signal {1e3*mc('Signal', t)[0]:.2f}±{1e3*mc('Signal', t)[1]:.2f}")
