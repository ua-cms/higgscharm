#!/bin/bash
# 2026-10-28 (user): b-hive ROCCurveTask on the OLD production test set (rebuilt per config by
# Analysis/decorrelation_2026_10_27/oldtest/oldtest_convert.py). usage: roc_old.sh <config> <dataset> <training> <test version>
CFG=$1; DT=$2; TT=$3; TV=$4; hostname; date; nvidia-smi -L
cd /eos/user/s/snandaku/b-hive_ttcc
TAG=${DT#hcZZ_big4class_}; TAG=${TAG%_*}   # e.g. massinvlepdisco0p5
TOUT=$(ls -t /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_train_decor/train_job.$TAG.*.out 2>/dev/null | head -1)
if [ -n "$TOUT" ]; then until grep -q "^law exit" $TOUT; do sleep 120; done; grep -q "^law exit 0" $TOUT || { echo "TRAINING FAILED"; exit 1; }; fi
source /eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh; source /eos/user/s/snandaku/miniforge3/etc/profile.d/mamba.sh
mamba activate b_hive_new; source setup.sh; law index
COMMON="--config $CFG --dataset-version $DT --model-name MLP_HcZZ_MW_Deep_4class --epochs 40 --training-version $TT \
  --learning-rate 0.0005 --mixed-precision True --torch-compile-mode True \
  --test-dataset-version $TV"
[ -s output/DatasetConstructorTask/$CFG/$TV/file_0.lz4 ] || { echo "OLD TEST DATASET MISSING"; exit 1; }
law run ROCCurveTask $COMMON
echo "law exit $?"
python3 -c "
import numpy as np,glob
f=glob.glob('output/ROCCurveTask/$CFG/$DT/$TV/$TT/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal/*/AUC_all_processes_all.npy')
print(f); print({k:round(float(v),4) for k,v in np.load(f[0],allow_pickle=True).item().items()})"
date
