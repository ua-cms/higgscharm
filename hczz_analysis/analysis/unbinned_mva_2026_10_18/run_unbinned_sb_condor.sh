#!/bin/bash
# 2026-10-19: full unbinned limit with pdfs in u = CDF of the (signal + background) mix (out_sb)
source /cvmfs/cms.cern.ch/cmsset_default.sh >/dev/null 2>&1; cd /eos/user/s/snandaku/Analysis/combine/CMSSW_14_1_0_pre4/src; eval $(scramv1 runtime -sh 2>/dev/null)
WD=/tmp/snandaku/unbinned_2026_10_18/full_sb_condor; mkdir -p $WD; cp /eos/user/s/snandaku/Analysis/unbinned_mva_2026_10_18/card_unbinned_sb/* $WD; cd $WD
text2workspace.py datacard_unbinned.txt -o workspace.root > t2w.log 2>&1
combine -M AsymptoticLimits -m 120 --run blind --rAbsAcc 0.00001 --rRelAcc 0.00001 workspace.root -n unb_full_sb > limit.log 2>&1
cp limit.log /eos/user/s/snandaku/Analysis/unbinned_mva_2026_10_18/limit_unbinned_sb_full_condor.log; echo "done $(date)"
