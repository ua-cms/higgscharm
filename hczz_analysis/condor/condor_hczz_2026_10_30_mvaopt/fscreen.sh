#!/bin/bash
hostname; date; source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh >/dev/null 2>&1
python3 -u /eos/user/s/snandaku/Analysis/mva_opt_2026_10_30/feature_screen.py 2>&1 | tee /eos/user/s/snandaku/Analysis/mva_opt_2026_10_30/feature_screen.log; date
