#!/bin/bash
# 2026-11-16 (user: parameters ~10-20% of the training events): same recipe as train_kfold.sh with hidden 128 x 3 residual blocks = 207,620 parameters (was 64 x 2 = 37,956), dropout 0.4 unchanged; version kf<k>_h128l3, eval train_eval_h128l3/
# 2026-11-14 (user): recipe switched to n2_noid_dd_cjdphi (62 inputs, dd_both without kin_dphi_j_4l); bak .bak_20261114_before_cjdphi
# 2026-11-14 (user): k-fold model k of the dd_both recipe = trained on folds != k (hcZZ_kf<k>, Other Higgs WH 25%), common DisCo lambda 1.42 in 105-160
# (= dd_both: HCZZ_DISCO_LAMBDA 1.42, disco, window 105,160), same hyperparameters as condor_hczz_2026_11_06_sculpt/train_var.sh; then inference on
# the held-out fold hcZZ_kf<k>_test + AUCs (Analysis/kfold_2026_11_13/train_eval/kf<k>/auc.txt). usage: train_kfold.sh <k>
K=$1; export HCZZ_MLP_HIDDEN=128 HCZZ_MLP_LAYERS=3 HCZZ_DISCO_LAMBDA=1.42 HCZZ_DECORR_TYPE=disco HCZZ_DECORR_WINDOW=105,160
hostname; date; nvidia-smi -L; env | grep HCZZ_
CFG=hc_zzto4l_mw_training_4class_fsextv2_comb2224_noid_nomult_dd_cjdphi_kfold; DS=hcZZ_kf$K; TR=kf${K}_h128l3
cd /eos/user/s/snandaku/b-hive_ttcc
source /eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh; source /eos/user/s/snandaku/miniforge3/etc/profile.d/mamba.sh
mamba activate b_hive_new; source setup.sh; law index
law run TrainingTask --config $CFG --dataset-version $DS --model-name MLP_HcZZ_MW_Deep_4class --epochs 40 --training-version $TR \
  --learning-rate 0.0005 --mixed-precision True --torch-compile-mode True --train-val-split 0.8 --loss-weighting True --batch-size 4096 \
  || { echo "TRAINING FAILED"; exit 1; }
echo "training done $(date)"
N=output/TrainingTask/$CFG/$DS/$TR/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal; M=$N/best_model.pt; [ -s $M ] || { echo "NO best_model.pt"; exit 2; }
O=/eos/user/s/snandaku/Analysis/kfold_2026_11_13/train_eval_h128l3/kf$K; mkdir -p $O
python3 /eos/user/s/snandaku/Analysis/sculpt_study_2026_11_06/infer.py $CFG $M output/DatasetConstructorTask/$CFG/${DS}_test $O cuda 2>&1 | grep -vE "Inference\.\.\.|^\s*$" | tail -1
python3 - $O <<'PY' | tee $O/auc.txt
import sys, numpy as np
o = sys.argv[1]; p = np.load(f"{o}/prediction.npy"); t = np.load(f"{o}/truth.npy"); t = t.argmax(1) if t.ndim > 1 else t.astype(int)
def auc(s, b):
    x = np.concatenate([s, b]); r = np.empty(len(x)); r[x.argsort(kind="mergesort")] = np.arange(1, len(x) + 1)
    return (r[:len(s)].sum() - len(s) * (len(s) + 1) / 2) / (len(s) * len(b))
ps = p[:, 2]; print("held-out fold", o.rsplit("/", 1)[-1], "events per class", np.bincount(t, minlength=4).tolist())
print("AUC P(S): S/qqZZ %.4f S/ggZZ %.4f S/OH %.4f S/all %.4f" % tuple(auc(ps[t == 2], ps[m]) for m in (t == 0, t == 1, t == 3, t != 2)))
PY
rm -f $N/model_[0-9]*.pt; echo "exit 0 $(date)"
