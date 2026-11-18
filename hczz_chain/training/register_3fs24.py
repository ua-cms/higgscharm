#!/usr/bin/env python3
"""2026-11-16: register the 5 retrained k-fold models (2024 stitched signal) in the branch: models/kf_dd_cjdphi_h128l3_3fs24/ + registry entry (md5)."""
import hashlib, os, shutil, yaml
REPO = "/eos/user/s/snandaku/Higgscharm2026/HcZZ_eossync"; N = "/eos/user/s/snandaku/Analysis/kfold_2026_11_16_3fs24"
T = "/eos/user/s/snandaku/b-hive_ttcc/output/TrainingTask/hc_zzto4l_mw_training_4class_fsextv2_comb2224_noid_nomult_dd_cjdphi_kfold"
C = "hc_zzto4l_mw_training_4class_fsextv2_comb2224_noid_nomult_dd_cjdphi_kfold"; NAME = "kf_dd_cjdphi_h128l3_3fs24"; M = f"{REPO}/models/{NAME}"
md5 = lambda p: hashlib.md5(open(p, "rb").read()).hexdigest()
os.makedirs(M, exist_ok=True); shutil.copy(f"/eos/user/s/snandaku/b-hive_ttcc/config/{C}.yml", M)
for k in range(5):
    os.makedirs(f"{M}/kf{k}", exist_ok=True)
    shutil.copy(f"{T}/hcZZ_kf{k}_3fs24/kf{k}_h128l3_3fs24/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal/best_model.pt", f"{M}/kf{k}/")
    shutil.copy(f"{N}/train_eval_h128l3_3fs24/kf{k}/auc.txt", f"{M}/kf{k}/heldout_auc.txt")
if os.path.exists(f"{N}/ps_vs_dk_3fs24.txt"):
    shutil.copy(f"{N}/ps_vs_dk_3fs24.txt", f"{M}/heldout_equal_weight_auc.txt")
reg = f"{REPO}/models/registry.yaml"; s = open(reg).read()
assert f"  {NAME}:" not in s, "already registered"
rel = [f"models/{NAME}/kf{k}/best_model.pt" for k in range(5)]
e = f"""  {NAME}:
    status: candidate  # user 2026-11-16: retrained with the 2024 stitched signal (4FS 0c + 3FS >=1c, as 2022/23); used for the cards and limits
    description: = kf_dd_cjdphi_h128l3 (62 inputs, MLP 128 x 3, DisCo 1.42, same Other Higgs mix) with the 2024 signal stitched like 2022/23 (Stitch3FS_cH_2024 + _ext1_2024 n_genc10 >= 1, 4FS n_genc10 == 0); 2022/23 rows unchanged
    config: models/{NAME}/{C}.yml
    model: {rel[0]}
    kfold_models:
""" + "".join(f"      - {r}\n" for r in rel) + f"""    md5:
      config: {md5(f"{M}/{C}.yml")}
      model: {md5(f"{REPO}/{rel[0]}")}
      kfold: [{", ".join(md5(f"{REPO}/{r}") for r in rel)}]
    fold: int(md5(parquet chunk file name)) % 5 (analysis/postprocess/kfold.py chunk_fold); Z+X rows by row index % 5
    architecture: MLP_HcZZ_MW_Deep_4class (HCZZ_MLP_HIDDEN=128, HCZZ_MLP_LAYERS=3 in training)
    classes: [qqZZ, ggZZ, Signal, Other_Higgs]
    signal_class: Signal
    n_inputs: 62
    eras: [2022preEE, 2022postEE, 2023preBPix, 2023postBPix, 2024, 2025, 2026]
    tagger: {{PNet: [2022preEE, 2022postEE, 2023preBPix, 2023postBPix], UParTAK4: [2024, 2025, 2026]}}
    training:
      rows: Analysis/kfold_2026_11_16_3fs24 (build_kfold_3fs24.py), datasets hcZZ_kf<k>_3fs24, training_version kf<k>_h128l3_3fs24
      settings: 40 epochs, lr 5e-4, batch 4096, train/val 0.8, class weighting, DisCo lambda 1.42 on qqZZ+ggZZ in 105 < m4l < 160 GeV
      date: 2026-11-16
"""
s = s.replace("  kf_dd_cjdphi_h128l3:\n", e + "  kf_dd_cjdphi_h128l3:\n", 1); open(reg, "w").write(s)
import sys; sys.path.insert(0, REPO)
from analysis.postprocess.mva_registry import resolve_model
print(resolve_model(NAME)["kfold_models"])
