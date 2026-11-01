#!/bin/bash
# 2026-10-25 REGRESSION copy of v4v3_card_2026_10_23/pipeline_v4v3full.sh (builder modules patched for 2024; must reproduce the 10-23 card bit-for-bit). Original: optbin10 card fully on JEC V4/V3: background nominal + shifts from the 10-22 reprocessing (hplusc_mva_4class_ctag2d_jecshifts_bkg_scored_v1, per-event muon ID SF from the processor, so no --recompute-muon-id-sf), Z+X from the V4/V3 CR production (35.9), sumw table rebuilt for that production (this dir). Compare with 427.0 (V2/V1) / 427.5 (V4/V3 + recomputed muon SF).
# usage: pipeline_optbin_condor.sh <tag e.g. optbin10> <comma-separated edges>
TAG=$1; EDGES=$2
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P4=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
P=/eos/user/s/snandaku/Analysis/builder_2024_2026_10_25
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_bkg_scored_v1 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_bkg_scored_v1 --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_jecv4_105160_2026_10_22 --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/v4v3_card_2026_10_23/sumw_table.json --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json --pdf-ch 1.065 --gg-scale-json-dir $P4/out"
K="--ctag2d-capfix none --fs-prescribed full --fs-scale-mode separate --split-other-higgs --per-process-bkg-scale --higgs-scale-norm"
PRE=combine_run3_105160_ctag2d_stitch3fs_fsprescribed_pdfcH_withzxCRmerged_4era_jecshifts_automcstats_sumwfix_new4fsscale
T=decorr_nocapfix_sepscale_full_splithiggs_ppscale_higgsnorm_tthxs_${TAG}
X=create_datacards_ctag2d_100150_stitched_decorr.py
DF=$P/regress_nominal_card; mkdir -p $DF
echo "start $(date) $(hostname) tag $TAG edges $EDGES"
C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh "$@"; }
( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; cd $S
  python3 -u $X $A $ST $K --bin-edges $EDGES --output $DF > $DF/build.log 2>&1; echo "build exited $? $(date)" )
grep -q "\[Done\]" $DF/build.log || { echo "BUILD FAILED"; exit 1; }
C $DF datacard_mva_with_zx.txt ${TAG}_105160 > $P/limit_${TAG}_v4v3full.out 2>&1
echo "limit: $(grep 'Expected 50' $P/limit_${TAG}_v4v3full.out) $(date)"
echo "ALL DONE $(date)"
REF=$B/${PRE}_${T}_v4v3full_2026_10_23
echo "== datacard diff vs $REF"; diff <(grep -v "^#" $REF/datacard_mva_with_zx.txt) <(grep -v "^#" $DF/datacard_mva_with_zx.txt) && echo "DATACARD IDENTICAL (ignoring comment lines)"
( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; python3 $P/compare_root.py $REF $DF )
