#!/bin/bash
# 2026-10-21: nominal K + --higgs-scale-norm (r 456.0 card) rebuilt with the optimised p_sig binning.
# usage: pipeline_optbin_condor.sh <tag e.g. optbin10> <comma-separated edges>
TAG=$1; EDGES=$2
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P4=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
P=/eos/user/s/snandaku/Analysis/optbin_2026_10_21
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_scored_v8 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_scored_v2 --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_105160_2026_10_07 --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02/sumw_table.json --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json --pdf-ch 1.065 --gg-scale-json-dir $P4/out"
K="--ctag2d-capfix none --fs-prescribed full --fs-scale-mode separate --split-other-higgs --per-process-bkg-scale --higgs-scale-norm"
PRE=combine_run3_105160_ctag2d_stitch3fs_fsprescribed_pdfcH_withzxCRmerged_4era_jecshifts_automcstats_sumwfix_new4fsscale
T=decorr_nocapfix_sepscale_full_splithiggs_ppscale_higgsnorm_tthxs_${TAG}
X=create_datacards_ctag2d_100150_stitched_decorr.py
DF=$B/${PRE}_${T}_2026_10_21; mkdir -p $DF
echo "start $(date) $(hostname) tag $TAG edges $EDGES"
C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh "$@"; }
( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; cd $S
  python3 -u $X $A $ST $K --bin-edges $EDGES --output $DF > $DF/build.log 2>&1; echo "build exited $? $(date)" )
grep -q "\[Done\]" $DF/build.log || { echo "BUILD FAILED"; exit 1; }
C $DF datacard_mva_with_zx.txt ${TAG}_105160 > $P/limit_${TAG}.out 2>&1
echo "limit: $(grep 'Expected 50' $P/limit_${TAG}.out) $(date)"
C $DF datacard_mva_with_zx.txt ${TAG}_105160_imp impacts > $P/impacts_${TAG}.out 2>&1 && echo "impacts done $(date)"
C $DF datacard_mva_with_zx.txt ${TAG}_105160_imp0 impacts0 > $P/impacts_${TAG}_r0.out 2>&1 && echo "impacts0 done $(date)"
echo "ALL DONE $(date)"
