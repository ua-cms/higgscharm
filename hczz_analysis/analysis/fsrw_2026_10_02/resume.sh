#!/bin/bash
# Resume the fsrw pipeline (same steps as pipeline.sh) after an interruption: every step that is
# already complete is skipped. Safe to run repeatedly. Launch detached, e.g. on any lxplus9 node:
#   setsid nohup bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/resume.sh > /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/resume.log 2>&1 < /dev/null &
P=/eos/user/s/snandaku/Analysis/fsrw_2026_10_02
G=/eos/user/s/snandaku/Analysis/fs_stitch_gg_2026_10_02
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
R=/eos/user/s/snandaku/Higgscharmnew/higgscharm
S=$R/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
T=jecshifts_automcstats_2026_10_02
FW=$B/combine_run3_100150_ctag2d_fsrw_withzxCRmerged_$T
FN=$B/combine_run3_100150_ctag2d_fsrw_nozx_$T
F0=$B/combine_run3_100150_ctag2d_fsrw_nofssyst_withzxCRmerged_$T
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_scored_v8 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_scored_v2 --automcstats-threshold 10"
RW="--signal-mode 4fs_rw --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --stitch-weights $G/stitch_weights.json"
ZX="--zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_2026_09_23"
X=create_datacards_ctag2d_100150_stitched.py
SL=$OUT/logs_fschad_scored_v1/4fs.log
echo "resume start $(date) $(hostname)"
klist -s || { echo "NO VALID KERBEROS TICKET: run kinit first"; exit 1; }

# 1. MVA scoring of the fschad 4FS production (rerun from scratch if it never finished cleanly)
if ! grep -q "^EXIT=0" $SL 2>/dev/null; then
  echo "scoring not complete -> rerunning $(date)"
  ( cd $R && source /eos/home-s/snandaku/miniforge3/etc/profile.d/conda.sh && mamba activate b_hive_new && \
    python3 analysis/postprocess/run_mva_postprocess.py --input-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad \
      --output-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --config models/hc_zzto4l_mw_training_4class_nomass.yml \
      --model models/best_model.pt --all-eras --no-mass-window --no-histograms --datasets HPlusCharm_ --workers 8 > $SL 2>&1
    echo "EXIT=$?" >> $SL )
  grep -q "^EXIT=0" $SL || { echo "SCORING FAILED, see $SL"; exit 1; }
fi
echo "scoring OK"

# 2. final stitching weights from all 4 eras
(
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
python3 -c "import json,sys; d=json.load(open('$G/stitch_weights.json')); sys.exit(0 if len(d['eras_used'])==4 else 1)" 2>/dev/null \
  || python3 $G/stitch_weights.py > $G/logs/stitch_weights_final.log 2>&1
python3 -c "import json; d=json.load(open('$G/stitch_weights.json')); assert len(d['eras_used'])==4, d['eras_used']; print('weights from', d['eras_used'])"
) || { echo "STITCH WEIGHTS FAILED"; exit 1; }

# 3. cards, one at a time; a card with "[Done]" in its build.log is kept
build() {  # dir, extra args
  local d=$1; shift
  grep -q "\[Done\]" $d/build.log 2>/dev/null && { echo "keep $(basename $d)"; return; }
  mkdir -p $d
  ( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; cd $S; python3 -u $X $A "$@" --output $d > $d/build.log 2>&1 )
  echo "$(basename $d) exited $? $(date)"
}
build $FW $ZX $RW
build $FN --skip-zx $RW
build $F0 $ZX $RW --no-fs-syst
for d in $FW $FN $F0; do grep -q "\[Done\]" $d/build.log || echo "BUILD FAILED: $d"; done

# 4. limits (skipped when limit_<tag>.log already exists in the card dir), then impacts on with-ZX
C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash $P/run_combine.sh "$@"; }
lim() {  # dir card tag
  [ -f $1/$2 ] || return
  [ -f $1/limit_$3.log ] && { echo "keep limit $3"; return; }
  C $1 $2 $3 > $P/combine_$3.out 2>&1
}
lim $FW datacard_mva_with_zx.txt fsrw_withzx
lim $FN datacard_no_zx.txt fsrw_nozx
lim $F0 datacard_mva_with_zx.txt fsrw_nofssyst_withzx
for t in fsrw_withzx fsrw_nozx fsrw_nofssyst_withzx; do echo "$t: $(grep 'Expected 50' $P/combine_$t.out 2>/dev/null)"; done
if [ -f $FW/datacard_mva_with_zx.txt ] && [ ! -f $FW/impacts_fsrw_withzx.json ]; then
  C $FW datacard_mva_with_zx.txt fsrw_withzx impacts >> $P/combine_fsrw_withzx.out 2>&1 && echo "impacts done $(date)"
fi
echo "ALL DONE $(date)"
