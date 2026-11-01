#!/bin/bash
# 2026-10-30: Z+X on the lepss CR production; same chain/options as condor_hczz_2026_10_28_zxperjet + the 10-29 jet fix.
set -o pipefail
WF=hplusc_mva_4class_CR_merged_lepss; E=/eos/user/s/snandaku/Higgscharmnew/higgscharm
L=/eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30; MO=/eos/user/s/snandaku/higgscharm/outputs/${WF}_merged_perjet
EST=$L/zx_estimate_105160
cd $E; { echo "start $(date) $(hostname)"
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
python3 -u scripts/merge_cr_parquets.py --workflow $WF --year all --merge-out $MO --force || { echo MERGE FAILED; exit 1; }
mkdir -p $EST
python3 -u scripts/estimate_zx_background.py --workflow $WF --merged-base $MO \
   --fake-rates /eos/user/p/pkatris/higgscharm/outputs/fake_rates/OS/2022preEE/fake_rates.pkl \
   --year all --mass-window 105 160 --output $EST 2>&1 | tee $EST/estimate.log || { echo ESTIMATE FAILED; exit 1; }
python3 - <<PY || { echo JET CHECK FAILED; exit 1; }
import glob, pandas as pd
bad = 0
for f in sorted(glob.glob("$EST/*/zx_*.parquet")):
    t = pd.read_parquet(f, columns=["jet_multiplicity", "jet_pt_0", "jet_eta_0", "jet_phi_0"]); h = t.jet_multiplicity > 0
    fr = float(t.loc[h, ["jet_pt_0", "jet_eta_0", "jet_phi_0"]].isna().mean().max()); print(f, "events", len(t), "NaN jet kin among jet events", fr)
    bad += fr > 0
import os, sys; sys.stdout.flush(); os._exit(1 if bad else 0)   # 10-30: plain exit aborted in pyarrow thread teardown (core dump) after a PASSING check
PY
echo "DONE $(date)"; } 2>&1 | tee $L/zxbuild.log
