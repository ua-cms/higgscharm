#!/bin/bash
# score the current Z+X estimate (per-jet, jet fix, 10-29) with one model. usage: zx_score.sh <b-hive config name> <best_model.pt> <out dir>
CFG=$1; M=$2; OUT=$3; mkdir -p $OUT
EST=/eos/user/s/snandaku/Analysis/zx_background_estimate_CRmerged_jecv4_105160_perjet_jetfix_2026_10_29
cd /eos/user/s/snandaku/Higgscharmnew/higgscharm
source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc13-opt/setup.sh >/dev/null 2>&1
python3 -u scripts/run_zx_inference.py --input $EST --output $OUT --config /eos/user/s/snandaku/b-hive_ttcc/config/$CFG.yml --model $M > $OUT/inference.log 2>&1
echo "zx exit $? $OUT"
