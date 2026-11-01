#!/bin/bash
# 2026-10-12: the 4-era direct stitch in the NEW m4l window [105,160) (user decision 10-12), with the
# 10-07 [105,160] Z+X (zx_background_mva_CRmerged_105160_2026_10_07). Otherwise pipeline_fsprescribed.sh.
# Four builds in parallel:
#   NONE  stitch, no FS nuisances          (ref.; 10-10 value in [100,150) 466.5)
#   FULL  stitch, --fs-prescribed full      (LHCHWG-2026-007 as written: unrenormalised NLO envelope, no extra NNLO lnN)
#   SHAPE stitch, --fs-prescribed shape     (CMS-style split, for comparison)
#   RW    4fs_rw, no FS nuisances           (ref.; 10-10 value in [100,150) 478.0)
# pdf_cH is added afterwards by editing the card (pure text row), once its value is measured.
P=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
T=4era_jecshifts_automcstats_sumwfix_2026_10_12
DN=$B/combine_run3_105160_ctag2d_stitch3fs_withzxCRmerged_$T
DF=$B/combine_run3_105160_ctag2d_stitch3fs_fsprescribed_full_withzxCRmerged_$T
DS=$B/combine_run3_105160_ctag2d_stitch3fs_fsprescribed_shape_withzxCRmerged_$T
DR=$B/combine_run3_105160_ctag2d_fsrw_nofssyst_withzxCRmerged_$T
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_scored_v8 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_scored_v2 --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_105160_2026_10_07 --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02/sumw_table.json --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P/preveto_genc_4fs.json"
RW="--signal-mode 4fs_rw --no-fs-syst --stitch-weights /eos/user/s/snandaku/Analysis/fs_stitch_gg_2026_10_02/stitch_weights.json"
X=create_datacards_ctag2d_100150_stitched.py
echo "start $(date) $(hostname)"
klist -s || { echo "NO VALID KERBEROS TICKET: run kinit first"; exit 1; }

C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh "$@"; }
lim() {  # dir card tag
  [ -f $1/$2 ] || return
  [ -f $1/limit_$3.log ] && { echo "keep limit $3"; return; }
  C $1 $2 $3 > $P/combine_$3.out 2>&1
  echo "$3: $(grep 'Expected 50' $P/combine_$3.out) $(date)"
}
build() {  # dir, extra args
  local d=$1; shift
  grep -q "\[Done\]" $d/build.log 2>/dev/null && { echo "keep $(basename $d)"; return; }
  mkdir -p $d
  ( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; cd $S; python3 -u $X $A "$@" --output $d > $d/build.log 2>&1 )
  echo "$(basename $d) exited $? $(date)"
}
( build $DN $ST;                     lim $DN datacard_mva_with_zx.txt stitch3fs_105160_4era_withzx ) > $P/pipeline_105160_none.log 2>&1 &
( build $DF $ST --fs-prescribed full;  lim $DF datacard_mva_with_zx.txt stitch3fs_fsfull_105160_4era_withzx ) > $P/pipeline_105160_full.log 2>&1 &
( build $DS $ST --fs-prescribed shape; lim $DS datacard_mva_with_zx.txt stitch3fs_fsshape_105160_4era_withzx ) > $P/pipeline_105160_shape.log 2>&1 &
( build $DR $RW;                     lim $DR datacard_mva_with_zx.txt fsrw_105160_4era_withzx ) > $P/pipeline_105160_rw.log 2>&1 &
wait
cat $P/pipeline_105160_{none,full,shape,rw}.log
for d in $DN $DF $DS $DR; do grep -q "\[Done\]" $d/build.log || echo "BUILD FAILED: $d"; done
echo "ALL DONE $(date)"
