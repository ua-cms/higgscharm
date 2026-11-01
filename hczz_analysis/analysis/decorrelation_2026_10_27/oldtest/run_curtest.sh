#!/bin/bash
# 2026-10-28 (user): no-m4l / massinvlep AUCs on the CURRENT test split (clean for them), same events for both:
# build once with the production config (same selection as the production test set), convert per config, ROC, then
# auc_mix.py (reweighted to the production test composition 15068/6894/24998/15069).
set -o pipefail
cd /eos/user/s/snandaku/b-hive_ttcc
source /eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh; source /eos/user/s/snandaku/miniforge3/etc/profile.d/mamba.sh
mamba activate b_hive_new; source setup.sh >/dev/null 2>&1
D=/eos/user/s/snandaku/Analysis/decorrelation_2026_10_27/oldtest; OUT=output/DatasetConstructorTask
law run DatasetConstructorTask --config hc_zzto4l_mw_training_4class_nomass --dataset-version curtest_prod_20261028 \
  --filelist filelists/hplusc_mva_4class_test.txt --coffea-worker 8 > $D/curtest_build.log 2>&1 || { echo "BUILD FAILED"; exit 1; }
echo "built $(date)"
for x in "nom4l hcZZ_big4class_nom4l_20261019 train_loose_ymassbal2_nom4l_20261019" "massinvlep hcZZ_big4class_massinvlep_20261027 train_loose_ymassbal2_massinvlep_20261027"; do set -- $x
  C=hc_zzto4l_mw_training_4class_$1
  python3 $D/oldtest_convert.py $C curtest_20261028 --src $PWD/$OUT/hc_zzto4l_mw_training_4class_nomass/curtest_prod_20261028 2>&1 | grep -E "^hc_|Error|error" || { echo "CONVERT FAILED $1"; exit 1; }
  CUDA_VISIBLE_DEVICES="" law run ROCCurveTask --config $C --dataset-version $2 --model-name MLP_HcZZ_MW_Deep_4class --epochs 40 \
    --training-version $3 --learning-rate 0.0005 --mixed-precision True --torch-compile-mode True \
    --test-dataset-version curtest_20261028 > $D/roc_curtest_$1.log 2>&1 || { echo "ROC FAILED $1"; exit 1; }
  I=output/InferenceTask/$C/$2/curtest_20261028/$3/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal/test_attack_nominal
  python3 $D/auc_mix.py $I --n 20 --out $D/auc_curtest_$1.json > /dev/null && echo "$1 done $(date)"
done
