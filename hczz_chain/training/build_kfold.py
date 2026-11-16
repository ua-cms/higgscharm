#!/usr/bin/env python3
"""2026-11-13 (user): 5-fold cross-application training of the final MVA recipe (n2_noid_dd_both) on the FINAL production events.

  extract <era> [--limit N]   nominal base parquets of the final production -> rows in the b-hive LZ4 layout of the dd_both
                              config, per (fold, era), into <OUT>/rows/<era>/fold<k>.npz. Window 90 < m4l < 160.
  compose                     for k = 0..4: training set = folds != k (Other Higgs resampled to the user's mix), test set =
                              fold k (all events), written as DatasetConstructorTask/<CFG_KF>/hcZZ_kf<k>[_test]
                              (file_*.lz4, processed_files.txt, histogram.npy, weights.json) + the config copy.

Row (width 70) = the 63 model inputs as analysis/postprocess/mva_inference.py prepare_features() builds them, but WITHOUT the
m4l scaling (b-hive's LZ4Dataset divides mass_scaled_features by the m4l column at load time; check_format.py) | m4l (DisCo mass) | process code | truth 4 |
w = 1. Fold = int(md5(parquet chunk name)) % 5: the chunk name (input file UUID + entry range) is the same in every
variation directory, so an event has one fold everywhere; the scorer uses the same function (mva_registry.chunk_fold).
Samples: 2022/23 signal = card stitching (4FS n_genc10 == 0 + 3FS n_genc10 >= 1, final_fschad), 2024 signal = all
HPlusCharm* (final_bkg); qqZZ = ZZto4L (powheg), ggZZ = gg->ZZ continuum, Other Higgs = ggH, VBF, WH, ZH, ttH, H+b.
Other Higgs mix in training (user 2026-11-14): ALL ggH, WH 25%, H+b 10%, VBF/ZH/ttH relative to WH by sqrt(card yield)
(2022/23 + 2024 card), each capped at what is available; no minimum class size (was: >= 600k, WH filled the rest ~48%). qqZZ, ggZZ and signal: all events. TRAINING-ONLY extra MC (workflow hplusc_mva_4class_ctag2d_final_trainonly:
CJLST's 2023 mcfm701 gg->ZZ 4e/4mu) is added to every training set, never to a test set (it is not scored for a card).
"""
import glob, hashlib, json, os, re, sys
import numpy as np

REPO = "/eos/user/s/snandaku/Higgscharm2026/HcZZ_eossync"
sys.path.insert(0, REPO)
O = "/eos/user/s/snandaku/higgscharm/outputs"
BH = "/eos/user/s/snandaku/b-hive_ttcc"
CFG = "hc_zzto4l_mw_training_4class_fsextv2_comb2224_noid_nomult_dd_both"   # row layout of the extracted rows (63 inputs)
# 2026-11-14 (user: "change the model to dd_cjdphi into the 5 trainings"): training recipe = n2_noid_dd_cjdphi = dd_both without
# kin_dphi_j_4l (global feature 9; configs otherwise identical) -> compose drops that column from the dd_both rows (no re-extraction)
TRAIN_CFG = "hc_zzto4l_mw_training_4class_fsextv2_comb2224_noid_nomult_dd_cjdphi"
DROP_COLS = [9]   # kin_dphi_j_4l in the dd_both row
CFG_KF = TRAIN_CFG + "_kfold"
OUT = "/eos/user/s/snandaku/Analysis/kfold_2026_11_13"
NFOLD, LO, HI, CHUNK = 5, 90.0, 160.0, 100000
ERAS_2223 = ("2022preEE", "2022postEE", "2023preBPix", "2023postBPix")
CLASS = {"qqZZ": 0, "ggZZ": 1, "Signal": 2, "Other_Higgs": 3}
# clean process codes (the b-hive substring matching labelled ggH as ZZto4L); era group 2024 = code + 100
PROC = {"ZZto4L": 0, "ggZZ": 1, "Signal4FS": 2, "Signal3FS": 3, "ggH": 4, "VBF": 5, "WH": 6, "ZH": 7, "ttH": 8, "HB": 9}
SAMPLES = [  # (process, class, prefixes)
    ("ZZto4L", "qqZZ", ("ZZto4L",)),
    ("ggZZ", "ggZZ", ("GluGlu2Zto", "GluGlutoContin", "GluGluToContin")),
    ("ggH", "Other_Higgs", ("GluGluHtoZZto4L",)),
    ("VBF", "Other_Higgs", ("VBFHto",)),
    ("WH", "Other_Higgs", ("WminusH", "WplusH")),
    ("ZH", "Other_Higgs", ("ZHto",)),
    ("ttH", "Other_Higgs", ("TTH",)),
    ("HB", "Other_Higgs", ("HPlusBottom", "2022preEEHB", "2022postEEHB", "2023preBPixHB", "2023postBPixHB")),
]
# card yields of the n2 2022/23 and 2024 binned cat40 cards (Other Higgs processes), for the sqrt mix
CARD = {"VBF": 4.6665 + 7.7321, "WH": 0.6629 + 0.9106 + 1.1017 + 1.5053, "ZH": 1.3972 + 2.3493, "ttH": 0.9960 + 1.5095}  # 2022/23 + 2024
F_WH, F_HB = 0.25, 0.10   # 2026-11-14 (user): WH 25% of Other Higgs, no 600k minimum (was OH_MIN 600000, ggH >= 50%)
TRAINONLY_WF = "hplusc_mva_4class_ctag2d_final_trainonly"


def chunk_fold(name):
    """Fold of a parquet chunk (file name without directory); identical in all variation directories."""
    return int(hashlib.md5(os.path.basename(name).encode()).hexdigest(), 16) % NFOLD


def sample_base(d):
    b = os.path.basename(d)
    return b.rsplit("_", 1)[0] if b.rsplit("_", 1)[-1].isdigit() else b


def inputs(era):
    """[(parquet, process, class, n_genc10 selector or None)] of an era."""
    out = []
    for d in sorted(glob.glob(f"{O}/hplusc_mva_4class_ctag2d_final_bkg/{era}/*")):
        s = sample_base(d)
        for proc, cls, pre in SAMPLES:
            if s.startswith(pre):
                out += [(f, proc, cls, None) for f in sorted(glob.glob(f"{d}/base/*.parquet"))]
                break
        else:
            if era not in ERAS_2223 and s.startswith("HPlusCharm"):   # 2024+: plain 4FS signal, all events
                out += [(f, "Signal4FS", "Signal", None) for f in sorted(glob.glob(f"{d}/base/*.parquet"))]
    for d in sorted(glob.glob(f"{O}/{TRAINONLY_WF}/{era}/*")):   # training-only extra MC (fold = -1)
        s = sample_base(d)
        for proc, cls, pre in SAMPLES:
            if s.startswith(pre):
                out += [(f, proc, cls, "trainonly") for f in sorted(glob.glob(f"{d}/base/*.parquet"))]
                break
    if era in ERAS_2223:  # card stitching from the signal production
        for d in sorted(glob.glob(f"{O}/hplusc_mva_4class_ctag2d_final_fschad/{era}/*")):
            s = sample_base(d)
            if s.startswith("Stitch3FS"):
                out += [(f, "Signal3FS", "Signal", "ge1") for f in sorted(glob.glob(f"{d}/base/*.parquet"))]
            elif s.startswith("HPlusCharm"):
                out += [(f, "Signal4FS", "Signal", "eq0") for f in sorted(glob.glob(f"{d}/base/*.parquet"))]
    return out


def extract(era, limit=None):
    import pandas as pd
    from analysis.postprocess.mva_registry import resolve_model
    from analysis.postprocess.mva_inference import MVAPostProcessor
    reg = resolve_model("n2_noid_dd_both", era=era)
    p = MVAPostProcessor(bhive_config_path=reg["config"], bhive_model_path=reg["model"], apply_mass_window=False, era=era)
    # the LZ4 rows store UNSCALED inputs: b-hive's LZ4Dataset (aux_m4l) divides the mass_scaled_features by the stored m4l
    # column at load time, so the scorer's own m4l scaling is switched off here (it would be applied twice)
    p.config = dict(p.config, mass_scaled_features=[])
    items = inputs(era)
    if limit:
        items = items[::max(1, len(items) // limit)][:limit]
    off = 0 if era in ERAS_2223 else 100
    rows = {k: [] for k in list(range(NFOLD)) + ["trainonly"]}
    nfail = 0
    for i, (f, proc, cls, sel) in enumerate(items):
        try:
            df = pd.read_parquet(f)
        except Exception as e:
            nfail += 1; print("READ FAIL", f, e, flush=True); continue
        m = df["zz_mass_inclusive"].to_numpy(dtype=float)
        keep = (m > LO) & (m < HI)
        if sel == "eq0":
            keep &= df["n_genc10"].to_numpy() == 0
        elif sel == "ge1":
            keep &= df["n_genc10"].to_numpy() >= 1
        if not keep.any():
            continue
        df = df[keep].reset_index(drop=True)
        X = p.prepare_features(df).astype(np.float32)
        assert X.shape[1] == 63, X.shape
        n = len(X)
        truth = np.zeros((n, 4), np.float32); truth[:, CLASS[cls]] = 1
        r = np.concatenate([X, df["zz_mass_inclusive"].to_numpy(np.float32)[:, None],
                            np.full((n, 1), PROC[proc] + off, np.float32), truth, np.ones((n, 1), np.float32)], axis=1)
        rows["trainonly" if sel == "trainonly" else chunk_fold(f)].append(r)
        if i % 500 == 0:
            print(f"{era} {i}/{len(items)} {proc}", flush=True)
    os.makedirs(f"{OUT}/rows/{era}", exist_ok=True)
    for k, rs in rows.items():
        a = np.concatenate(rs) if rs else np.zeros((0, 70), np.float32)
        np.savez(f"{OUT}/rows/{era}/{'fold' if k != 'trainonly' else ''}{k}{'_test' if limit else ''}.npz", rows=a)
        print(f"{era} fold {k}: {len(a)} rows", flush=True)
    print(f"{era}: {len(items)} parquets, {nfail} read failures", flush=True)
    if nfail:
        sys.exit(3)


def load_fold(k, eras):
    return np.concatenate([np.load(f"{OUT}/rows/{e}/fold{k}.npz")["rows"] for e in eras])


def oh_mix(a, rng):
    """Other Higgs of one training set (user 2026-11-14): ALL ggH, WH = F_WH (25%) of the class, H+b = F_HB (10%) of the class,
    VBF/ZH/ttH = WH x sqrt(card yield) / sqrt(WH card yield); a process with too few events keeps all of them. No minimum class
    size: the class total T follows from the fixed parts, T = (ggH + capped) / (1 - sum of the uncapped fractions)."""
    keep = np.ones(len(a), bool); proc = a[:, -6].astype(int) % 100
    idx = {p: np.where(proc == PROC[p])[0] for p in ("ggH", "HB", "VBF", "WH", "ZH", "ttH")}
    frac = {"WH": F_WH, "HB": F_HB, **{p: F_WH * np.sqrt(CARD[p] / CARD["WH"]) for p in ("VBF", "ZH", "ttH")}}
    avail = {p: len(idx[p]) for p in frac}
    lo, hi = float(len(idx["ggH"])), float(len(idx["ggH"]) + sum(avail.values()))
    for _ in range(100):   # T = ggH + sum_p min(frac_p T, avail_p): unique solution in [lo, hi], bisection
        T = 0.5 * (lo + hi)
        lo, hi = (T, hi) if len(idx["ggH"]) + sum(min(frac[p] * T, avail[p]) for p in frac) > T else (lo, T)
    target = {p: min(frac[p] * T, avail[p]) for p in frac}
    report = {"ggH": f"{len(idx['ggH'])}/{len(idx['ggH'])}"}
    for p, t in target.items():
        n = min(len(idx[p]), int(round(t)))
        if n < len(idx[p]):
            keep[rng.choice(idx[p], len(idx[p]) - n, replace=False)] = False
        report[p] = f"{n}/{len(idx[p])}"
    report["total"] = int(sum(int(v.split("/")[0]) for v in report.values()))
    return keep, report


def write_dataset(a, ver, cfg):
    d = f"{BH}/output/DatasetConstructorTask/{CFG_KF}/{ver}"
    os.makedirs(d, exist_ok=True)
    import lz4.frame
    files, sizes = [], []
    for j, i0 in enumerate(range(0, len(a), CHUNK)):
        b = np.ascontiguousarray(a[i0:i0 + CHUNK], dtype=np.float32); fn = f"{d}/file_{j}.lz4"
        with lz4.frame.open(fn, mode="wb") as fp:
            fp.write(np.concatenate((np.array(b.shape, dtype=np.float32), b.ravel())).tobytes())
        files.append(fn); sizes.append(len(b))
    open(f"{d}/processed_files.txt", "w").write("\n".join(files))
    g = cfg["global_features"]
    h = np.zeros((4, len(cfg["bins_pt"]) - 1, len(cfg["bins_eta"]) - 1))
    t = a[:, -5:-1].argmax(1)
    for c in range(4):
        sel = t == c
        h[c] = np.histogram2d(np.clip(a[sel, g.index(cfg["pt_key"])], cfg["bins_pt"][0], cfg["bins_pt"][-1] - 1e-3),
                              np.clip(a[sel, g.index(cfg["eta_key"])], cfg["bins_eta"][0], cfg["bins_eta"][-1] + 1e-3),
                              bins=[cfg["bins_pt"], cfg["bins_eta"]])[0]
    np.save(f"{d}/histogram.npy", h)
    json.dump({"chunk_size": sizes, "sum_weight": [float(s) for s in sizes]}, open(f"{d}/weights.json", "w"), indent=2)
    return np.bincount(t, minlength=4)


def compose(eras):
    import yaml
    cfg = yaml.safe_load(open(f"{BH}/config/{TRAIN_CFG}.yml"))
    rowcfg = yaml.safe_load(open(f"{BH}/config/{CFG}.yml"))
    assert [f for i, f in enumerate(rowcfg["global_features"]) if i not in DROP_COLS] == cfg["global_features"], "row layout vs training config"
    src = open(f"{BH}/config/{TRAIN_CFG}.yml").read()
    open(f"{BH}/config/{CFG_KF}.yml", "w").write(
        "# 2026-11-14: k-fold (5) cross-application datasets hcZZ_kf<k>[_test] of the dd_cjdphi recipe, built from the final\n"
        "# production by Analysis/kfold_2026_11_13/build_kfold.py (dd_both rows minus kin_dphi_j_4l); features identical to " + TRAIN_CFG + "\n" + src)
    drop = lambda a: np.delete(a, DROP_COLS, axis=1)
    folds = {k: drop(load_fold(k, eras)) for k in range(NFOLD)}
    trainonly = drop(np.concatenate([np.load(f) ["rows"] for f in sorted(glob.glob(f"{OUT}/rows/*/trainonly.npz"))] or [np.zeros((0, 70), np.float32)]))
    assert folds[0].shape[1] == 69 and trainonly.shape[1] == 69, (folds[0].shape, trainonly.shape)
    old = np.array([229437, 50625, 394458, 1107928], float)   # dd_both histogram class totals
    mw_old = 4 * old.min() / old.sum()
    summary = {}
    for k in range(NFOLD):
        rng = np.random.default_rng(20261113 + k)
        tr = np.concatenate([folds[j] for j in range(NFOLD) if j != k] + [trainonly])
        keep, rep = oh_mix(tr, rng)
        tr = tr[keep][rng.permutation(keep.sum())]
        n_tr = write_dataset(tr, f"hcZZ_kf{k}", cfg)
        n_te = write_dataset(folds[k], f"hcZZ_kf{k}_test", cfg)
        mw = 4 * n_tr.min() / n_tr.sum()
        summary[k] = {"train_class_counts": n_tr.tolist(), "test_class_counts": n_te.tolist(), "oh_mix": rep,
                      "mean_class_weight": round(float(mw), 4), "disco_lambda": round(1.42 * mw / mw_old, 3)}
        print(k, json.dumps(summary[k]), flush=True)
    json.dump(summary, open(f"{OUT}/compose_summary.json", "w"), indent=1)


if __name__ == "__main__":
    if sys.argv[1] == "extract":
        lim = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else None
        extract(sys.argv[2], lim)
    elif sys.argv[1] == "compose":
        compose(list(ERAS_2223) + ["2024"])
