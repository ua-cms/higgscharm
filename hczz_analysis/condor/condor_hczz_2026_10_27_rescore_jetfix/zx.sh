#!/bin/bash
M=$1; hostname; date
source /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_rescore_jetfix/models.sh $M; mkdir -p $ZX
cd /eos/user/s/snandaku/Higgscharmnew/higgscharm
grep -q "'jet_pt', 'cjets_pt'" analysis/postprocess/mva_inference.py || { echo "SCORER NOT FIXED"; exit 3; }
source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc13-opt/setup.sh
# 2026-10-28: per-jet Z+X estimate (CR merge kept only the leading jet before) + full lepton mapping; wait for the rebuild
EST=/eos/user/s/snandaku/Analysis/zx_background_estimate_CRmerged_jecv4_105160_perjet_jetfix_2026_10_29   # 10-29: jet-kinematics fix
until grep -q '^DONE' /eos/user/s/snandaku/Analysis/mva_newinputs_2026_10_28/zx_jetfix_rebuild.log 2>/dev/null; do sleep 120; done
python3 -u scripts/run_zx_inference.py --input $EST --output $ZX \
  --config $CFGM --model $TDIR/best_model.pt > $ZX/inference.log 2>&1
rc=$?; echo "exit $rc $(date)"; exit $rc
