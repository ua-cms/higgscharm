#!/usr/bin/env python3
"""2026-10-30: kappa discriminant D = p_S / (p_S + k_qq p_qq + k_gg p_gg + k_OH p_OH), sum k = 1 (kappa*.ipynb), on the
b-hive test set + real Z+X scored with the same model. Metrics: 2D Asimov proxy (sens.py), single-cut S/sqrt(B)
(as the notebook, best cut with signal eff >= 10%), sculpting (ZZ f115-135 at 5% ZZ eff).
usage: kappa_scan.py <pred.npz> <zx dir> [step]"""
import sys, itertools, numpy as np, pandas as pd
sys.path.insert(0, "/eos/user/s/snandaku/Analysis/mva_opt_2026_10_30")
pred, zdir = sys.argv[1], sys.argv[2]; step = float(sys.argv[3]) if len(sys.argv) > 3 else 0.05
sys.argv = [sys.argv[0]]; import sens
e = sens.load(pred); d = np.load(pred, allow_pickle=True); P = d["pred"]; cls = e["cls"]
iS = cls.index("Signal"); bk = ["qqZZ", "ggZZ", "Other_Higgs"]; ib = [cls.index(c) for c in bk]
Zp, Zm, Zw = [], [], []
for era in sens.ERAS:
    for cr in ("3p1f", "2p2f"):
        df = pd.read_parquet(f"{zdir}/{era}/zx_{cr}_{era}_mva.parquet", columns=["m4l", "zx_weight"] + [f"mva_score_{c}" for c in cls])
        Zm.append(np.array([x[0] if hasattr(x, "__len__") and len(x) else (np.nan if hasattr(x, "__len__") else x) for x in df["m4l"].values], float))
        Zp.append(df[[f"mva_score_{c}" for c in cls]].values); Zw.append(df["zx_weight"].values)
Zp, Zm, Zw = np.concatenate(Zp), np.concatenate(Zm), np.concatenate(Zw)
def D(p, k): return p[:, iS] / (p[:, iS] + (p[:, ib] * np.asarray(k)).sum(1) + 1e-12)
def ssb(s, z):
    win = (e["m"] > 105) & (e["m"] < 160); sig = win & (e["t"] == iS); bkg = win & ~sig; zm = (Zm > 105) & (Zm < 160)
    cuts = np.quantile(s[sig], np.linspace(0, 0.9, 91)); best = 0
    for c in cuts:
        S = e["w"][sig & (s >= c)].sum(); B = e["w"][bkg & (s >= c)].sum() + max(Zw[zm & (z >= c)].sum(), 0)
        if B > 0: best = max(best, S / np.sqrt(B))
    return best
rows = []
grid = [k for k in itertools.product(np.arange(0, 1 + 1e-9, step), repeat=2) if k[0] + k[1] <= 1 + 1e-9]
for kq, kg in grid:
    k = (kq, kg, 1 - kq - kg); s = D(P, k); z = D(Zp, k)
    e2 = dict(e); e2["s"] = s; zx = dict(s=z, m=Zm, w=Zw)
    rows.append((k, sens.proxy(e2, zxe=zx)[0], ssb(s, z), sens.sculpt(e2)))
ref = [r for r in rows if np.allclose(r[0], (1/3, 1/3, 1/3), atol=step)]
eq = (1/3, 1/3, 1/3); s = D(P, eq); z = D(Zp, eq); e2 = dict(e); e2["s"] = s
base = (eq, sens.proxy(e2, zxe=dict(s=z, m=Zm, w=Zw))[0], ssb(s, z), sens.sculpt(e2))
print(f"p_S (equal kappa)       proxy {base[1]:6.1f}  S/sqrtB {base[2]:.5f}  sculpt {base[3]:.3f}")
for name, key in (("best 2D proxy", lambda r: r[1]), ("best S/sqrtB", lambda r: -r[2])):
    r = min(rows, key=key); print(f"{name:22s} kappa(qq,gg,OH)={tuple(round(x, 2) for x in r[0])}  proxy {r[1]:6.1f}  S/sqrtB {r[2]:.5f}  sculpt {r[3]:.3f}")
ok = [r for r in rows if r[3] <= base[3] + 0.005]
r = min(ok, key=lambda r: r[1]); print(f"{'best proxy, no extra sculpt':22s} kappa={tuple(round(x, 2) for x in r[0])}  proxy {r[1]:6.1f}  S/sqrtB {r[2]:.5f}  sculpt {r[3]:.3f}")
np.save(pred.replace(".npz", "_kappascan.npy"), np.array([(*r[0], r[1], r[2], r[3]) for r in rows]))
