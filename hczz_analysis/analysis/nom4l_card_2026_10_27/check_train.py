# compare fixed scorer features vs b-hive training npz (nom4l, 10-19), per column
import sys, glob, re, numpy as np, pandas as pd
sys.path.insert(0, "/eos/user/s/snandaku/Higgscharmnew/higgscharm")
from analysis.postprocess.mva_inference import MVAPostProcessor
B = "/eos/user/s/snandaku/b-hive_ttcc"
import os
CFG = os.environ.get("CFG", f"{B}/config/hc_zzto4l_mw_training_4class_nom4l.yml")
MOD = os.environ.get("MOD") or glob.glob(f"{B}/output/TrainingTask/hc_zzto4l_mw_training_4class_nom4l/hcZZ_big4class_nom4l_20261019/train_loose_ymassbal2_nom4l_20261019/*/epochs_40/nominal/best_model.pt")[0]
D = os.environ.get("DSET", f"{B}/output/DatasetConstructorTask/hc_zzto4l_mw_training_4class_nom4l/hcZZ_big4class_nom4l_20261019")
pats = sys.argv[1:]
procs = {}
tot_bad = None; tot_n = 0
for pat in pats:
    f = sorted(glob.glob(f"{D}/{pat}"))[0]
    m = re.search(r"hplusc_mva_4class_(\w+?)_(\w+?)_base_(.+)_\d+_\d+\.npz$", f.split("/")[-1])
    era, ds, rest = m.groups()
    src = glob.glob(f"/eos/user/s/snandaku/higgscharm/outputs/hplusc_mva_4class/{era}/{ds}/base/{rest}.parquet")[0]
    z = np.load(f, allow_pickle=True)
    def flat(name, ncand=None):
        a = z[name]; cols = []
        if ncand is None:
            for k in a.dtype.names: cols.append((k, a[k]))
        else:
            for j in range(ncand):
                for k in a.dtype.names: cols.append((f"{k}[{j}]", a[k][:, j]))
        return cols
    cols = flat("global_features") + flat("cpf_arr", 3) + flat("npf_arr", 4) + flat("vtx_arr", 2) + flat("lt_arr")
    names = [c[0] for c in cols]; T = np.stack([c[1] for c in cols], 1).astype(np.float32)
    # 2026-10-27: aux_m4l datasets store m4l as the last lt field (not an input); mass_scaled_features are
    # divided by it at load time (b-hive LZ4Dataset) -> emulate the training-time input here
    import yaml
    _cfg = yaml.safe_load(open(CFG))
    if _cfg.get("aux_m4l"):
        assert names[-1] == "m4l", names[-3:]
        m4l = T[:, -1].copy(); T = T[:, :-1]; names = names[:-1]
        sc = set(_cfg.get("mass_scaled_features") or [])
        idx = [i for i, nm in enumerate(names) if nm.split("[")[0] in sc]
        T[:, idx] = T[:, idx] / m4l[:, None]
    if era not in procs: procs[era] = MVAPostProcessor(CFG, MOD, era=era)
    df = pd.read_parquet(src)
    X = procs[era].prepare_features(df)
    X = X.numpy() if hasattr(X, "numpy") else np.asarray(X, dtype=np.float32)
    assert X.shape[1] == T.shape[1], (X.shape, T.shape)
    # match on leading lepton pts
    key = lambda A: [tuple(r) for r in np.round(A, 3)]
    li = [names.index(f"lepton_pt[{j}]") for j in range(4)]
    idx = {k: i for i, k in enumerate(key(X[:, li]))}
    rows = [idx.get(k, -1) for k in key(T[:, li])]
    ok = np.array(rows) >= 0
    Xm = X[np.array(rows)[ok]]; Tm = T[ok]
    bad = ~np.isclose(Xm, Tm, rtol=1e-4, atol=1e-4)
    tot_bad = bad.sum(0) if tot_bad is None else tot_bad + bad.sum(0); tot_n += ok.sum()
    print(f"{ds} {era}: npz {len(T)}, matched {ok.sum()}, events with any mismatch {bad.any(1).sum()}")
print(f"TOTAL matched {tot_n}; columns with mismatches:")
for i in np.nonzero(tot_bad)[0]: print(f"  {names[i]}: {tot_bad[i]}")
print(f"ALL {len(names)} COLUMNS MATCH" if tot_bad.sum() == 0 else "MISMATCHES REMAIN")
