#!/bin/bash
# 2026-10-04 driver: sumw scan (checkpointed, up to 5 passes until 0 open errors), then pipeline.sh
# (cards + limits + impacts). Resumable: rerun this script after any interruption.
# Launch: setsid nohup bash .../run_all.sh > .../run_all.log 2>&1 < /dev/null &
P=/eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02
export X509_USER_PROXY=$HOME/private/x509up_u$(id -u)
echo "start $(date) $(hostname)"
for pass in 1 2 3 4 5; do
  grep -q " 0 open errors; 0 chunk reads without sumw" $P/make_sumw_table.log 2>/dev/null && break
  ( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; cd $P
    python3 -u make_sumw_table.py > $P/make_sumw_table.log 2>&1 )
  echo "scan pass $pass: $(grep -E '^matched' $P/make_sumw_table.log) $(date)"
done
bash $P/pipeline.sh
