#!/bin/bash
# 2026-10-28: can the classifier inputs reconstruct m4l? GBR regression of m4l on qqZZ 105-160 for several input sets
hostname; date; source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc13-opt/setup.sh
cd /eos/user/s/snandaku/Analysis/decorrelation_2026_10_27/ht_leak; python3 -u regress.py > regress.txt 2>&1; echo "exit $? $(date)"
