#!/bin/bash
# 2026-10-02: H+c signal = 4FS FxFx H->ZZ reweighted to the FS.pdf 3FS+4FS stitch with weights derived on the
# official Run 3 H->gg samples (our rederivation, Analysis/fs_stitch_gg_2026_10_02), normalised to NNLO 68.2 fb.
# Builder: create_datacards_ctag2d_100150_stitched.py --signal-mode 4fs_rw (nominal = massive 1c, T = 5 GeV;
# CMS_FS_cH_massless1c and CMS_FS_cH_T10 as shape nuisances). Signal from the fschad production
# (gen_chad1_pt / n_chad5 / n_chad10, muon ID SF in the weights), scored into fschad_scored_v1.
# Backgrounds, Z+X, binning, other systematics: identical to the 09-25 perdsnorm reference (335.0 / 328.0).
# Builds run one at a time (the 09-29 parallel builds were killed); combine in a clean env.
P=/eos/user/s/snandaku/Analysis/fsrw_2026_10_02
G=/eos/user/s/snandaku/Analysis/fs_stitch_gg_2026_10_02
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
T=jecshifts_automcstats_2026_10_02
FW=$B/combine_run3_100150_ctag2d_fsrw_withzxCRmerged_$T
FN=$B/combine_run3_100150_ctag2d_fsrw_nozx_$T
F0=$B/combine_run3_100150_ctag2d_fsrw_nofssyst_withzxCRmerged_$T
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_scored_v8 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_scored_v2 --automcstats-threshold 10"
RW="--signal-mode 4fs_rw --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --stitch-weights $G/stitch_weights.json"
ZX="--zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_2026_09_23"
X=create_datacards_ctag2d_100150_stitched.py
echo "start $(date) $(hostname)"
# 1. wait for the inputs
until grep -q "^EXIT=" $OUT/logs_fschad_scored_v1/4fs.log 2>/dev/null; do sleep 120; done
echo "scoring: $(tail -1 $OUT/logs_fschad_scored_v1/4fs.log) $(date)"
grep -q "^EXIT=0" $OUT/logs_fschad_scored_v1/4fs.log || { echo "SCORING FAILED"; exit 1; }
until grep -q "ALL DONE" $G/logs/rerun_skipped.log 2>/dev/null; do sleep 120; done
echo "gg reruns done $(date)"
(
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
python3 $G/stitch_weights.py > $G/logs/stitch_weights_final.log 2>&1
python3 -c "import json; d=json.load(open('$G/stitch_weights.json')); assert len(d['eras_used'])==4, d['eras_used']; print('weights from', d['eras_used'])"
) || { echo "STITCH WEIGHTS FAILED"; exit 1; }
# 2. cards, one at a time
(
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
cd $S
mkdir -p $FW $FN $F0
python3 -u $X $A $ZX       $RW --output $FW > $FW/build.log 2>&1; echo "FW exited $? $(date)"
python3 -u $X $A --skip-zx $RW --output $FN > $FN/build.log 2>&1; echo "FN exited $? $(date)"
python3 -u $X $A $ZX       $RW --no-fs-syst --output $F0 > $F0/build.log 2>&1; echo "F0 exited $? $(date)"
)
ok=1
for d in $FW $FN $F0; do grep -q "\[Done\]" $d/build.log || { echo "BUILD FAILED: $d"; tail -8 $d/build.log; ok=0; }; done
for d in $FW $FN $F0; do echo "== $(basename $d)"; grep -E "^rate|CMS_FS_cH" $d/datacard_*.txt; done
# 3. limits, then impacts on the with-ZX nominal
C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash $P/run_combine.sh "$@"; }
rm -rf /tmp/snandaku/fsrw_2026_10_02
[ -f $FW/datacard_mva_with_zx.txt ] && C $FW datacard_mva_with_zx.txt fsrw_withzx > $P/combine_fsrw_withzx.out 2>&1
[ -f $FN/datacard_no_zx.txt ]      && C $FN datacard_no_zx.txt      fsrw_nozx   > $P/combine_fsrw_nozx.out 2>&1
[ -f $F0/datacard_mva_with_zx.txt ] && C $F0 datacard_mva_with_zx.txt fsrw_nofssyst_withzx > $P/combine_fsrw_nofssyst_withzx.out 2>&1
echo "limits done $(date)"
for t in fsrw_withzx fsrw_nozx fsrw_nofssyst_withzx; do echo "$t: $(grep 'Expected 50' $P/combine_$t.out)"; done
[ $ok = 1 ] && C $FW datacard_mva_with_zx.txt fsrw_withzx impacts >> $P/combine_fsrw_withzx.out 2>&1 && echo "impacts done $(date)"
echo "ALL DONE $(date)"
