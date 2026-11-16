"""2026-11-16 (user): P(S) vs D_kappa(0.40,0.15,0.45) on the 5 k-fold 4-class models (r1 + r2), equal-weight held-out AUCs per fold + ZZ m4l sculpting."""
import numpy as np
from sklearn.metrics import roc_auc_score
def sm(x):
    e = np.exp(x - x.max(1, keepdims=True)); return e / e.sum(1, keepdims=True)
K = (0.40, 0.15, 0.45)
F = {"P(S)": lambda p: p[:, 2], "D_kappa": lambda p: p[:, 2] / (p[:, 2] + K[0] * p[:, 0] + K[1] * p[:, 1] + K[2] * p[:, 3])}
B = [("qqZZ", [0]), ("ggZZ", [1]), ("OH", [4, 5, 6, 7, 8, 9]), ("all", [0, 1, 4, 5, 6, 7, 8, 9])]
def ew(s, proc, era, e, bp):
    m = np.isin(proc, [2, 3] + bp) & (era == e); w = np.zeros(len(s)); y = np.isin(proc, [2, 3])
    for c in [2, 3] + bp:
        mm = (proc == c) & (era == e); n = mm.sum()
        if n: w[mm] = 1 / n
    w[m & y] /= w[m & y].sum(); w[m & ~y] /= w[m & ~y].sum()
    return roc_auc_score(y[m], s[m], sample_weight=w[m])
import sys
RUNS = [a.split(":") for a in sys.argv[1:]] or [("train_eval", "first run"), ("train_eval_r2", "retrain")]
for tag, lab in RUNS:
    res = {}; sc = {f: [0, 0, 0, 0] for f in F}; den = [0, 0, 0, 0]
    for k in range(5):
        p = sm(np.load(f"{tag}/kf{k}/prediction.npy").astype(float)); pr = np.load(f"{tag}/kf{k}/process.npy").astype(int)
        m4 = np.load(f"{tag}/kf{k}/m4l.npy"); proc, era = pr % 100, (pr >= 100).astype(int)
        zz = (proc <= 1) & (m4 >= 105) & (m4 < 160); win = (m4 >= 115) & (m4 < 135)
        for f, fn in F.items():
            s = fn(p)
            for e in (0, 1):
                for bn, bp in B: res[(f, e, bn, k)] = ew(s, proc, era, e, bp)
            for i, eff in enumerate((0.5, 0.2, 0.1, 0.05)):
                sel = zz & (s > np.quantile(s[zz], 1 - eff)); sc[f][i] += win[sel].mean()
        for i in range(4): den[i] += win[zz].mean()
    print(f"\n=== 4-class k-fold, {lab}: equal-weight AUC, signal vs ...")
    for e, en in ((0, "2022/23"), (1, "2024")):
        print(f"{en:8s} {'':8s} " + " ".join(f"{'kf'+str(k):>6s}" for k in range(5)) + "   mean  | mean qqZZ  ggZZ    OH")
        for f in F:
            a = [res[(f, e, "all", k)] for k in range(5)]
            mq = [np.mean([res[(f, e, bn, k)] for k in range(5)]) for bn in ("qqZZ", "ggZZ", "OH")]
            print(f"{'':8s} {f:8s} " + " ".join(f"{x:6.3f}" for x in a) + f"  {np.mean(a):.3f}  |     " + " ".join(f"{x:.3f}" for x in mq))
    print("ZZ sculpting f(115-135) ratio at 50/20/10/5% ZZ eff: " + " | ".join(f"{f}: " + " ".join(f"{sc[f][i]/den[i]:.2f}" for i in range(4)) for f in F))
