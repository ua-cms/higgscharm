#!/bin/bash
export X509_USER_PROXY=$HOME/private/x509up_u178308
source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc11-opt/setup.sh >/dev/null 2>&1
python3 /eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/probe.py /eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/rerun_files.txt /eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/probe_result.json > /eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/probe.log 2>&1; tail -1 /eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/probe.log
