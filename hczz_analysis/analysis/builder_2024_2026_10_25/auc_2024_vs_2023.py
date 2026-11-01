"""2026-10-25: weighted AUCs of mva_signal_score, signal vs each background, 2024 (_scored_2024_v2)
vs 2023 (_scored_v2), plus argmax-class fractions. Nominal 'base' parquets, all selected events."""
import glob, re, numpy as np, pyarrow.parquet as pq
O = "/eos/user/s/snandaku/higgscharm/outputs"
SETS = {
    "2024": (f"{O}/hplusc_mva_4class_ctag2d_jecshifts_scored_2024_v2/2024", {"sig": r"HPlusCharm_v2_2024"}),
    "2023": (f"{O}/hplusc_mva_4class_ctag2d_jecshifts_scored_v2/2023*", {"sig": r"HPlusCharm_new_2023\w*"}),
}
BKG = {"qqZZ": r"ZZto4L", "ggZZ": r"GluGlu(2Z|ToContin|toContin)\w*", "ggH": r"GluGluHtoZZto4L", "VBF": r"VBFHto2Zto4L"}
def load(base, pat):
    s, w, c = [], [], []
    for d in glob.glob(f"{base}/*"):
        name = re.sub(r"_\d+$", "", d.rstrip("/").split("/")[-1])
        if not re.fullmatch(pat, name):
            continue
        for f in glob.glob(f"{d}/base/*.parquet"):
            t = pq.read_table(f, columns=["mva_signal_score", "weight_nominal", "mva_class_prediction"])
            s.append(t["mva_signal_score"].to_numpy()); w.append(t["weight_nominal"].to_numpy())
            c.append(t["mva_class_prediction"].to_numpy())
    if not s:
        return None
    return np.concatenate(s), np.concatenate(w), np.concatenate(c)
def auc(s1, w1, s0, w0):
    x = np.concatenate([s1, s0]); y = np.r_[np.ones(len(s1)), np.zeros(len(s0))]; w = np.concatenate([w1, w0])
    o = np.argsort(-x, kind="stable"); y, w = y[o], w[o]
    tp = np.cumsum(w * y) / np.sum(w * y); fp = np.cumsum(w * (1 - y)) / np.sum(w * (1 - y))
    return np.trapz(np.r_[0, tp], np.r_[0, fp])
for era, (base, sp) in SETS.items():
    sig = load(base, sp["sig"])
    if sig is None:
        print(era, "no signal found"); continue
    print(f"== {era}: signal {len(sig[0])} ev, mean score {np.average(sig[0], weights=sig[1]):.3f}, "
          f"argmax fractions {np.round(np.bincount(sig[2].astype(int), minlength=4) / len(sig[2]), 3)}")
    for b, p in BKG.items():
        bk = load(base, p)
        if bk is None:
            print(f"   {b}: none"); continue
        print(f"   S vs {b:5s}: AUC {auc(sig[0], sig[1], bk[0], bk[1]):.3f}  ({len(bk[0])} ev, mean {np.average(bk[0], weights=bk[1]):.3f})")
