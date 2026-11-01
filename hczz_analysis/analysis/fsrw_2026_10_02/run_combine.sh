#!/bin/bash
# usage: run_combine.sh <card dir on EOS> <datacard name> <tag> [impacts|impacts0]
# impacts: Asimov at r = expected median limit; impacts0: background-only Asimov (r = 0)
set -e
SRC=$1; CARD=$2; TAG=$3; IMP=$4
cd /eos/user/s/snandaku/Analysis/combine/CMSSW_14_1_0_pre4/src
source /cvmfs/cms.cern.ch/cmsset_default.sh >/dev/null 2>&1
eval $(scramv1 runtime -sh 2>/dev/null)
WD=/tmp/snandaku/fsrw_2026_10_02/combine_$TAG
mkdir -p $WD; cp $SRC/$CARD $SRC/histograms_*.root $WD/; cd $WD
text2workspace.py $CARD -o workspace.root > t2w.log 2>&1
combine -M AsymptoticLimits -m 120 --run blind --rAbsAcc 0.00001 --rRelAcc 0.00001 workspace.root -n $TAG > limit.log 2>&1
grep -E "Expected" limit.log
if [ "$IMP" = "impacts" ]; then
  MED=$(grep "Expected 50.0%" limit.log | awk '{print $NF}')
  COMMON="-M Impacts -d workspace.root -m 120 -t -1 --expectSignal $MED --setParameterRanges r=0,1500 --robustFit 1"
elif [ "$IMP" = "impacts0" ]; then
  COMMON="-M Impacts -d workspace.root -m 120 -t -1 --expectSignal 0 --setParameterRanges r=-1500,1500 --robustFit 1"
fi
if [ -n "$IMP" ]; then
  combineTool.py $COMMON --doInitialFit > imp_init.log 2>&1
  combineTool.py $COMMON --doFits --parallel 12 > imp_fits.log 2>&1
  combineTool.py $COMMON -o impacts_$TAG.json > imp_collect.log 2>&1
  plotImpacts.py -i impacts_$TAG.json -o impacts_$TAG > imp_plot.log 2>&1 || true
  echo "impacts done: $WD/impacts_$TAG.json"
fi
cp -p limit.log $SRC/limit_$TAG.log
[ -f impacts_$TAG.json ] && cp -p impacts_$TAG.json impacts_$TAG.pdf $SRC/ 2>/dev/null || true
