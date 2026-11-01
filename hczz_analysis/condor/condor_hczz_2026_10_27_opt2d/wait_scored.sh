#!/bin/bash
# 2026-10-28: wait until the 8 scoring jobs + Z+X of model $1 finished OK (chain in condor_hczz_2026_10_27_rescore_jetfix)
M=$1; C=/afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_rescore_jetfix; hostname; date
while true; do
  nok=$(ls $C/score.$M.*.out 2>/dev/null | xargs -r grep -l "^exit 0" | wc -l)
  nbad=$(ls $C/score.$M.*.out $C/zx.$M.*.out 2>/dev/null | xargs -r grep -lE "^exit [1-9]" | wc -l)
  zok=$(ls $C/zx.$M.*.out 2>/dev/null | xargs -r grep -l "^exit 0" | wc -l)
  [ $nbad -gt 0 ] && ! [ $nok -ge 8 ] && { echo "scoring failure seen ($nbad), still waiting for retries $(date)"; }
  [ $nok -ge 8 ] && [ $zok -ge 1 ] && { echo "scored + zx ready $(date)"; exit 0; }
  sleep 120
done
