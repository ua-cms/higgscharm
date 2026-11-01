#!/bin/bash
# 2026-10-27: mass-decorrelated SM 4-class H+c MVAs (user: mass-invariant inputs + DisCo). = the 10-19 nom4l recipe
# (90 <= m4l < 160, no m4l input, same flags) with config $1 (aux_m4l + mass_scaled_features or disco, see
# b-hive utils/torch/mass_decorrelation.py). usage: train_job.sh <config> <tag>
CFG=$1; TAG=$2; hostname; date; nvidia-smi -L
cd /eos/user/s/snandaku/b-hive_ttcc
source /eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh; source /eos/user/s/snandaku/miniforge3/etc/profile.d/mamba.sh
mamba activate b_hive_new; source setup.sh; law index
law run DatasetConstructorTask --config $CFG --dataset-version hcZZ_big4class_${TAG}_20261027 \
  --filelist filelists/hplusc_mva_4class_train.txt --coffea-worker 8 || { echo "DATASET FAILED"; exit 1; }
echo "dataset done $(date)"
law run TrainingTask --config $CFG --dataset-version hcZZ_big4class_${TAG}_20261027 \
  --model-name MLP_HcZZ_MW_Deep_4class --epochs 40 --training-version train_loose_ymassbal2_${TAG}_20261027 \
  --train-val-split 0.8 --loss-weighting True --learning-rate 0.0005 --mixed-precision True --torch-compile-mode True
echo "law exit $?"; date
