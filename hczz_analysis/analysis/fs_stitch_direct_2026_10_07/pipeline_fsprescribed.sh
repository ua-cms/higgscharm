#!/bin/bash
# 2026-10-11: 4-era direct stitch WITH the prescribed FS uncertainties (--fs-prescribed shape and full,
# built in parallel); otherwise identical to pipeline_4era.sh (2026-10-10). Original header:
# 4-era (2026-10-10; was 2022-only dry run 10-07) of the DIRECT 3FS+4FS stitch (LHCHWG-2026-007 sec. 4.4) vs the 4FS reweighted to the
# stitch (4fs_rw, the 10-05 nominal), both normalised to sigma_NNLO = 73.30 fb, [100,150), with Z+X,
# no CMS_FS_cH_* nuisances (the stitched sample's scale envelope is not built yet), so that the
# two cards differ only in how the signal is made (2026-10-07).
#   stitched: n_genc10 = 0 from 4FS (sigma 97.62 fb, K4 0.76), >= 1 from the new 3FS (55.0 fb, K3 1.31),
#             region fractions from pre-veto sumw (4FS split from scan_4fs_preveto_genc.py)
#   4fs_rw:   4FS x per-event weights from the official H->gg samples, sumw table
# Launch: setsid nohup bash .../fs_stitch_direct_2026_10_07/pipeline.sh > .../pipeline.log 2>&1 < /dev/null &
P=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
T=4era_jecshifts_automcstats_sumwfix_2026_10_11
FSS=$B/combine_run3_100150_ctag2d_stitch3fs_fsprescribed_shape_withzxCRmerged_$T
FSF=$B/combine_run3_100150_ctag2d_stitch3fs_fsprescribed_full_withzxCRmerged_$T
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_scored_v8 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_scored_v2 --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_2026_09_23 --sumw-table /eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02/sumw_table.json --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P/preveto_genc_4fs.json"
X=create_datacards_ctag2d_100150_stitched.py
echo "start $(date) $(hostname)"
klist -s || { echo "NO VALID KERBEROS TICKET: run kinit first"; exit 1; }
grep -q "SCAN DONE" $P/scan.log && grep -q "^2023postBPix: pre-veto" $P/scan.log || { echo "4FS pre-veto scan not done"; exit 1; }

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
( build $FSS $ST --fs-prescribed shape; lim $FSS datacard_mva_with_zx.txt stitch3fs_fsshape_4era_withzx ) > $P/pipeline_fsprescribed_shape.log 2>&1 &
( build $FSF $ST --fs-prescribed full;  lim $FSF datacard_mva_with_zx.txt stitch3fs_fsfull_4era_withzx ) > $P/pipeline_fsprescribed_full.log 2>&1 &
wait
cat $P/pipeline_fsprescribed_shape.log $P/pipeline_fsprescribed_full.log
for d in $FSS $FSF; do grep -q "\[Done\]" $d/build.log || echo "BUILD FAILED: $d"; done
echo "ALL DONE $(date)"
