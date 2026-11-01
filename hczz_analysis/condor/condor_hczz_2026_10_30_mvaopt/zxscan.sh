#!/bin/bash
# 2026-10-30: for every scan tag, wait for its model + test predictions, score the Z+X estimate with it and append the
# Z+X-aware proxy to scan_results_zx.txt. usage: zxscan.sh <tag> [<tag> ...]
B=/eos/user/s/snandaku/b-hive_ttcc; W=/eos/user/s/snandaku/Analysis/mva_opt_2026_10_30
CFG=hc_zzto4l_mw_training_4class_fsextv2_disco1; DS=hcZZ_fs_fsextv2disco1_20261028
hostname; date
for TAG in "$@"; do
  for i in $(seq 1 720); do [ -s $W/pred_scan_$TAG.npz ] && break; sleep 120; done
  [ -s $W/pred_scan_$TAG.npz ] || { echo "$TAG: no predictions (timeout)" >> $W/scan_results_zx.txt; continue; }
  T=$(ls -d $B/output/TrainingTask/$CFG/$DS/scan_$TAG/MLP_HcZZ_MW_Deep_4class/epochs_*/nominal | head -1)
  bash $W/zx_score.sh $CFG $T/best_model.pt $W/zx_scan_$TAG
  R=$(bash -c "source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh >/dev/null 2>&1; cd $W; python3 sens.py pred_scan_$TAG.npz:zx_scan_$TAG | tail -1")
  echo "$TAG | $(grep "^$TAG " $W/scan_results.txt | cut -d'|' -f1) | $R" | tee -a $W/scan_results_zx.txt
done; date
