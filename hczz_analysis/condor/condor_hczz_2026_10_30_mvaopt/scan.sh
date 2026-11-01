#!/bin/bash
# 2026-10-30 MVA optimisation scan: train one setting on the fsextv2 lambda=1 dataset (same inputs / DisCo), then
# test-set inference + sensitivity proxy. usage: scan.sh <tag> <hidden> <layers> <dropout> <lr> <epochs> [batch]
TAG=$1; export HCZZ_MLP_HIDDEN=$2 HCZZ_MLP_LAYERS=$3 HCZZ_MLP_DROPOUT=$4; LR=$5; EP=$6; BS=${7:-1024}
hostname; date; nvidia-smi -L
B=/eos/user/s/snandaku/b-hive_ttcc; W=/eos/user/s/snandaku/Analysis/mva_opt_2026_10_30
CFG=hc_zzto4l_mw_training_4class_fsextv2_disco1; DS=hcZZ_fs_fsextv2disco1_20261028; TR=scan_${TAG}
cd $B; source /eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh; source /eos/user/s/snandaku/miniforge3/etc/profile.d/mamba.sh
mamba activate b_hive_new; source setup.sh; law index >/dev/null
law run TrainingTask --config $CFG --dataset-version $DS --filelist filelists/hcZZ_fs_train_20261028.txt \
  --model-name MLP_HcZZ_MW_Deep_4class --epochs $EP --training-version $TR --batch-size $BS \
  --train-val-split 0.8 --loss-weighting True --learning-rate $LR --mixed-precision True --torch-compile-mode True
echo "law exit $?"; date
T=$B/output/TrainingTask/$CFG/$DS/$TR/MLP_HcZZ_MW_Deep_4class/epochs_$EP/nominal
[ -s $T/best_model.pt ] || { echo "NO MODEL"; exit 1; }
python3 $W/predict_test.py $CFG MLP_HcZZ_MW_Deep_4class $T $B/output/DatasetConstructorTask/$CFG/${DS}_test $W/pred_scan_$TAG.npz || exit 2
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh >/dev/null 2>&1
R=$(python3 $W/sens.py $W/pred_scan_$TAG.npz | tail -1)
echo "$TAG h=$2 l=$3 d=$4 lr=$LR ep=$EP bs=$BS | $R" | tee -a $W/scan_results.txt
date
