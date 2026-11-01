#!/bin/bash
# 2026-10-30: pre-jet-veto sumw table for the lepss production (prod scoring = same chunks for every model)
hostname; date; source /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_30_lepss/models.sh prod
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
export X509_USER_PROXY=/afs/cern.ch/user/s/snandaku/private/x509up_u178308
python3 -u /eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/sumw/make_sumw_table.py --scored-dir $BKG --jecshifts-dir $BKG --fs-scored-dir $SIG --out /eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/sumw > /eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/sumw/sumw.log 2>&1
rc=$?; echo "exit $rc $(date)"; exit $rc
