#!/bin/bash
# 2026-10-13: nominal 4-era direct stitch in [105,160) (--fs-prescribed full + pdf_cH 1.065), with the 0c 4FS scale
# factors from our REBUILT 4FS H->ZZ gridpack (out/gg_<era>.json, mu_R-floor events fixed) instead of the official H->gg 4FS.
# Reference: 10-12 nominal r = 489.5. Then impacts.
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
T=4era_jecshifts_automcstats_sumwfix_new4fsscale_2026_10_13
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_scored_v8 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_scored_v2 --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_105160_2026_10_07 --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02/sumw_table.json --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json"
X=create_datacards_ctag2d_100150_stitched.py
DF=$B/combine_run3_105160_ctag2d_stitch3fs_fsprescribed_full_pdfcH_withzxCRmerged_$T
echo "start $(date) $(hostname)"
klist -s || { echo "NO VALID KERBEROS TICKET: run kinit first"; exit 1; }
C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh "$@"; }
mkdir -p $DF
( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; cd $S
  python3 -u $X $A $ST --fs-prescribed full --pdf-ch 1.065 --gg-scale-json-dir $P/out --output $DF > $DF/build.log 2>&1 )
echo "build exited $? $(date)"; grep -q "\[Done\]" $DF/build.log || { echo "BUILD FAILED"; exit 1; }
C $DF datacard_mva_with_zx.txt stitch3fs_fsfull_pdfcH_new4fs_105160_4era_withzx > $P/combine_limit.out 2>&1
echo "limit: $(grep 'Expected 50' $P/combine_limit.out) $(date)"
C $DF datacard_mva_with_zx.txt stitch3fs_fsfull_pdfcH_new4fs_105160_imp impacts > $P/combine_impacts.out 2>&1 && echo "impacts done $(date)"
echo "ALL DONE $(date)"
