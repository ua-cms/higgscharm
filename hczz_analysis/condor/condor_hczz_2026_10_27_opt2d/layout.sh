#!/bin/bash
# 2026-10-27: build + expected limit for one 2D layout. usage: layout.sh <model> <name> rect <m4l edges> <score edges>
#                                                                  layout.sh <model> <name> cat <layout.json>
M=$1; NAME=$2; MODE=$3; hostname; date
source /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_rescore_jetfix/models.sh $M
D=/eos/user/s/snandaku/Analysis/opt2d_2026_10_27; W=$D/$M; DF=$W/card_$NAME; mkdir -p $DF
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P4=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
OUT=/eos/user/s/snandaku/higgscharm/outputs; S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
A="--scored-dir $BKG --jecshifts-dir $BKG --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir $ZX --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/v4v3_card_2026_10_23/sumw_table.json --fs-scored-dir $SIG --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json --pdf-ch 1.065 --gg-scale-json-dir $P4/out"
K="--ctag2d-capfix none --fs-prescribed full --fs-scale-mode separate --split-other-higgs --per-process-bkg-scale --higgs-scale-norm"
B=$D/create_datacards_unroll_2d.py; source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
if [ "$MODE" = rect ]; then
  ME=$4; SE=$5; U=$(python3 $B --print-edges $ME $SE); UNR="--unroll-m4l-edges $ME"
else
  L=$(python3 $B --print-cat-edges $4); SC=$(echo "$L" | sed -n 1p); U=$(echo "$L" | sed -n 2p); UNR="--unroll-score-cats $SC"
fi
echo "$NAME $MODE unroll [$UNR] bin-edges $U" | tee $DF/layout.txt
( cd $S; python3 -u $B $UNR $A $ST $K --bin-edges $U --output $DF > $DF/build.log 2>&1 )
grep -q "\[Done\]" $DF/build.log || { echo "BUILD FAILED $DF"; exit 1; }
env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh $DF datacard_mva_with_zx.txt ${M}_$NAME > $W/limit_$NAME.out 2>&1
echo "$NAME: $(grep 'Expected 50' $W/limit_$NAME.out) $(date)" | tee -a $W/limits.txt
