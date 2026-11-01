#!/bin/bash
# 2026-10-28: Z+X rebuild after the CR-merge per-jet fix (merge_cr_parquets.py kept only the leading jet).
# Same chain/options as Analysis/jecv4_bkg_2026_10_22/zx_pipeline.sh; new dirs, the 10-22 ones untouched.
set -o pipefail
WF=hplusc_mva_4class_CR_merged_jecv4; E=/eos/user/s/snandaku/Higgscharmnew/higgscharm; A=/eos/user/s/snandaku/Analysis
MO=/eos/user/s/snandaku/higgscharm/outputs/${WF}_merged_perjet
EST=$A/zx_background_estimate_CRmerged_jecv4_105160_perjet_2026_10_28
cd $E; echo "start $(date) $(hostname)"
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
python3 -u scripts/merge_cr_parquets.py --workflow $WF --year all --merge-out $MO --force || { echo MERGE FAILED; exit 1; }
mkdir -p $EST
python3 -u scripts/estimate_zx_background.py --workflow $WF --merged-base $MO \
   --fake-rates /eos/user/p/pkatris/higgscharm/outputs/fake_rates/OS/2022preEE/fake_rates.pkl \
   --year all --mass-window 105 160 --output $EST 2>&1 | tee $EST/estimate.log || { echo ESTIMATE FAILED; exit 1; }
echo "DONE $(date)"
