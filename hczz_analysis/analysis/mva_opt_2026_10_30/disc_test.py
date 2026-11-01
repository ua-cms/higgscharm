import sys, numpy as np
sys.argv = [sys.argv[0]]; sys.path.insert(0, "/eos/user/s/snandaku/Analysis/mva_opt_2026_10_30")
import sens
f = "/eos/user/s/snandaku/Analysis/mva_opt_2026_10_30/pred_fsextv2disco1.npz"
e = sens.load(f); d = np.load(f, allow_pickle=True); P = d["pred"]; cls = e["cls"]
win = (e["m"] > 105) & (e["m"] < 160); Y = {c: e["w"][win & (e["t"] == i)].sum() for i, c in enumerate(cls)}
Y["qqZZ"] += 35.9   # Z+X is ZZ-like in the classifier (fake leptons); counted with qqZZ here
bk = [c for c in cls if c != "Signal"]; y = np.array([Y[c] for c in bk]); y = y / y.sum()
iS = cls.index("Signal")
for name, s in [("p_S", P[:, iS]),
                ("LR physical", P[:, iS] / (P[:, iS] + (P[:, [cls.index(c) for c in bk]] * y).sum(1))),
                ("LR equal", P[:, iS] / (P[:, iS] + P[:, [cls.index(c) for c in bk]].mean(1)))]:
    e2 = dict(e); e2["s"] = s
    print(f"{name:12s} proxy {sens.proxy(e2)[0]:6.1f}  ZX~qqZZ {sens.proxy(e2, zx=35.9)[0]:6.1f}  sculpt5% {sens.sculpt(e2):.3f}")
print("bkg fractions", dict(zip(bk, np.round(y, 3))))
