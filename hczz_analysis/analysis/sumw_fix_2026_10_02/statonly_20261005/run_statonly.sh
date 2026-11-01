#!/bin/bash
# usage: run.sh <card dir> <card> <tag>
SRC=$1; CARD=$2; TAG=$3
cd /eos/user/s/snandaku/Analysis/combine/CMSSW_14_1_0_pre4/src
source /cvmfs/cms.cern.ch/cmsset_default.sh >/dev/null 2>&1
eval $(scramv1 runtime -sh 2>/dev/null)
WD=/tmp/snandaku/claude-178308/-eos-home-s-snandaku-second-brain/d5301257-bd32-4370-81d1-b66438379be4/scratchpad/statonly/$TAG
mkdir -p $WD; cp $SRC/$CARD $SRC/histograms_*.root $WD/; cd $WD
text2workspace.py $CARD -o ws.root > t2w.log 2>&1 || { echo "t2w failed"; tail t2w.log; exit 1; }
A="-M AsymptoticLimits -m 120 --run blind --rAbsAcc 0.00001 --rRelAcc 0.00001 ws.root"
combine $A -n full > full.log 2>&1
combine $A -n nosyst --freezeParameters allConstrainedNuisances > nosyst.log 2>&1
combine $A -n statonly --freezeParameters allConstrainedNuisances --freezeNuisanceGroups autoMCStats > statonly.log 2>&1
for v in full nosyst statonly; do echo "$TAG $v: $(grep -E 'Expected (16|50|84)' $v.log | awk '{printf "%s ", $NF}')"; done
