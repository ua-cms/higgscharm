#!/bin/bash
# 2026-10-28 (user): HcZZ classifier with new inputs (decay angles, 4l/jet kinematics, pruned by importance) on the
# FS-stitched balanced training set (filelists/hcZZ_fs_{train,test}_20261028.txt). usage: train_fs.sh <config> <tag>
CFG=$1; TAG=$2; hostname; date; nvidia-smi -L
cd /eos/user/s/snandaku/b-hive_ttcc
source /eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh; source /eos/user/s/snandaku/miniforge3/etc/profile.d/mamba.sh
mamba activate b_hive_new; source setup.sh; law index
DS=hcZZ_fs_${TAG}_20261028; TR=train_fs_${TAG}_20261028
law run DatasetConstructorTask --config $CFG --dataset-version $DS --filelist filelists/hcZZ_fs_train_20261028.txt --coffea-worker 8 || { echo "DATASET FAILED"; exit 1; }
echo "dataset done $(date)"
law run TrainingTask --config $CFG --dataset-version $DS --model-name MLP_HcZZ_MW_Deep_4class --epochs 40 --training-version $TR \
  --train-val-split 0.8 --loss-weighting True --learning-rate 0.0005 --mixed-precision True --torch-compile-mode True
echo "law exit $?"; date
law run DatasetConstructorTask --config $CFG --dataset-version ${DS}_test --filelist filelists/hcZZ_fs_test_20261028.txt --coffea-worker 8 || { echo "TEST DATASET FAILED"; exit 1; }
law run ROCCurveTask --config $CFG --dataset-version $DS --model-name MLP_HcZZ_MW_Deep_4class --epochs 40 --training-version $TR \
  --learning-rate 0.0005 --mixed-precision True --torch-compile-mode True --test-dataset-version ${DS}_test
echo "roc exit $?"
python3 -c "
import numpy as np,glob
f=glob.glob('output/ROCCurveTask/$CFG/$DS/${DS}_test/$TR/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal/*/AUC_all_processes_all.npy')
print({k:round(float(v),4) for k,v in np.load(f[0],allow_pickle=True).item().items()})"
date
