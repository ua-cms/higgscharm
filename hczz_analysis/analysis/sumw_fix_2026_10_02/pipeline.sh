#!/bin/bash
# fsrw cards rebuilt with the pre-veto, per-chunk sumw table (2026-10-02).
# Same inputs/options as Analysis/fsrw_2026_10_02 plus --sumw-table. Resumable: a card with
# "[Done]" in its build.log and a limit with limit_<tag>.log are kept.
# Launch: setsid nohup bash .../sumw_fix_2026_10_02/pipeline.sh > .../sumw_fix_2026_10_02/pipeline.log 2>&1 < /dev/null &
P=/eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02
G=/eos/user/s/snandaku/Analysis/fs_stitch_gg_2026_10_02
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
T=jecshifts_automcstats_sumwfix_2026_10_02
FW=$B/combine_run3_100150_ctag2d_fsrw_withzxCRmerged_$T
FN=$B/combine_run3_100150_ctag2d_fsrw_nozx_$T
FX=$B/combine_run3_100150_ctag2d_fsrw_nofssyst_withzxCRmerged_$T   # 2026-10-04: FS-nuisance effect on the limit
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_scored_v8 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_scored_v2 --automcstats-threshold 10"
RW="--signal-mode 4fs_rw --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --stitch-weights $G/stitch_weights.json --sumw-table $P/sumw_table.json"
ZX="--zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_2026_09_23"
X=create_datacards_ctag2d_100150_stitched.py
echo "start $(date) $(hostname)"
klist -s || { echo "NO VALID KERBEROS TICKET: run kinit first"; exit 1; }

# 1. wait for the sumw table; refuse to build on open errors or missing chunks
until grep -q "^DONE" $P/make_sumw_table.log 2>/dev/null; do
  pgrep -u $USER -f "python3 -u make_sumw" >/dev/null || { grep -q "^DONE" $P/make_sumw_table.log || { echo "SUMW SCAN NOT RUNNING AND NOT DONE"; exit 1; }; }
  sleep 120
done
grep -q "^DONE" $P/make_sumw_table.log || { echo "SUMW SCAN FAILED"; tail -20 $P/make_sumw_table.log; exit 1; }
tail -3 $P/make_sumw_table.log
# 2026-10-05: gate on chunk coverage only. Open errors on dead replica URLs (cceos.ihep.ac.cn, se01.indiacms) are
# harmless when every chunk read has its sumw; a needed file that failed would show up as a chunk without sumw.
grep -q " 0 chunk reads without sumw; 0 files where" $P/make_sumw_table.log || { echo "SUMW TABLE INCOMPLETE: see problems.json"; exit 1; }

# 2. cards one at a time, each limit right after its card (2026-10-04)
C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh "$@"; }
lim() {  # dir card tag
  [ -f $1/$2 ] || return
  [ -f $1/limit_$3.log ] && { echo "keep limit $3"; return; }
  C $1 $2 $3 > $P/combine_$3.out 2>&1
}

build() {  # dir, extra args
  local d=$1; shift
  grep -q "\[Done\]" $d/build.log 2>/dev/null && { echo "keep $(basename $d)"; return; }
  mkdir -p $d
  ( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; cd $S; python3 -u $X $A "$@" --output $d > $d/build.log 2>&1 )
  echo "$(basename $d) exited $? $(date)"
}
build $FW $ZX $RW; lim $FW datacard_mva_with_zx.txt sumwfix_withzx
build $FN --skip-zx $RW; lim $FN datacard_no_zx.txt sumwfix_nozx
build $FX $ZX $RW --no-fs-syst; lim $FX datacard_mva_with_zx.txt sumwfix_nofssyst_withzx
for d in $FW $FN $FX; do grep -q "\[Done\]" $d/build.log || echo "BUILD FAILED: $d"; done

for t in sumwfix_withzx sumwfix_nozx sumwfix_nofssyst_withzx; do echo "$t: $(grep 'Expected 50' $P/combine_$t.out 2>/dev/null)"; done
if [ -f $FW/datacard_mva_with_zx.txt ] && [ ! -f $FW/impacts_sumwfix_withzx.json ]; then
  C $FW datacard_mva_with_zx.txt sumwfix_withzx impacts >> $P/combine_sumwfix_withzx.out 2>&1 && echo "impacts done $(date)"
fi
echo "ALL DONE $(date)"
