#!/bin/bash
set -e
export X509_USER_PROXY=$HOME/private/x509up_u178308
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
cd /eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07
python3 -u scan_4fs_preveto_genc.py --eras 2022preEE 2022postEE 2023preBPix 2023postBPix
echo "SCAN DONE $(date)"
