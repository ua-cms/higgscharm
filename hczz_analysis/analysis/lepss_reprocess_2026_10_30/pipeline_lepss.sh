#!/bin/bash
# 2026-10-27: HcZZ 2022/23 cards with a no-m4l SM MVA. Same builder chain/options as the 429 card
# (Analysis/v4v3_card_2026_10_23/pipeline_v4v3full.sh); only the scored inputs and the binning change.
# usage: pipeline.sh <tag> <bkg scored dir> <sig scored dir> <zx mva dir>
#   1. dump build (optbin10 edges) -> events_<tag>.npz (+ card with the old optbin10 edges, limit)
#   2. optimise_binning.py --smooth-zx (as optbin10) -> S10 edges for this score; 1D card + limit
#   3. 2D m4l x score (create_datacards_unroll_m4l.py), m4l edges M4L, score edges = signal quantiles; card + limit
set -o pipefail
TAG=$1; BKG=$2; SIG=$3; ZX=$4
P=/eos/user/s/snandaku/Analysis/nom4l_card_2026_10_27; W=/eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/cards/$TAG; mkdir -p $W   # 2026-10-30 lepss: scripts from nom4l_card, outputs here
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P4=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
OPT=/eos/user/s/snandaku/Analysis/optbin_2026_10_21
A="--scored-dir $BKG --jecshifts-dir $BKG --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir $ZX --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30/sumw/sumw_table.json --fs-scored-dir $SIG --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_lepss_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_lepss_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json --pdf-ch 1.065 --gg-scale-json-dir $P4/out"
K="--ctag2d-capfix none --fs-prescribed full --fs-scale-mode separate --split-other-higgs --per-process-bkg-scale --higgs-scale-norm"
OPTBIN10=0.000000,0.077426,0.198279,0.381715,0.518877,0.634575,0.714366,0.808976,0.833804,0.874049,1.000000
M4L=${M4L:-105,115,119,122,124,126,128,131,140,160}
NSQ=${NSQ:-4}
PY=/eos/home-s/snandaku/miniforge3/bin/python3
LCG="source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh"
C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh "$@"; }
build() {  # <name> <script + args...>
  local DF=$W/card_$1; shift; mkdir -p $DF
  ( eval $LCG; cd $S; python3 -u "$@" --output $DF > $DF/build.log 2>&1 )
  grep -q "\[Done\]" $DF/build.log || { echo "BUILD FAILED $DF"; return 1; }
}
limit() {  # <name>
  C $W/card_$1 datacard_mva_with_zx.txt ${TAG}_$1 > $W/limit_$1.out 2>&1
  echo "$1: $(grep 'Expected 50' $W/limit_$1.out) $(date)" | tee -a $W/limits.txt
}
echo "start $(date) $(hostname) tag $TAG"; echo "bkg $BKG"; echo "sig $SIG"; echo "zx $ZX"

# -1. scoring validation (validate_scoring.py: completeness vs scored_v1_jetfix (10-27: was scored_v1, deleted), identical non-score columns, re-scoring,
#     inputs = production minus m4l; blocking) and performance vs training (validate_performance.py; report only:
#     known issue 10-27, the scorer reads cjets_* before jet_* while b-hive trained on jet_*, ~9% of events)
CFGM=${CFGM:-/eos/user/s/snandaku/b-hive_ttcc/config/hc_zzto4l_mw_training_4class_nom4l.yml}
TDIR=${TDIR:-/eos/user/s/snandaku/b-hive_ttcc/output/TrainingTask/hc_zzto4l_mw_training_4class_nom4l/hcZZ_big4class_nom4l_20261019/train_loose_ymassbal2_nom4l_20261019/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal}
DSET=${DSET:-/eos/user/s/snandaku/b-hive_ttcc/output/DatasetConstructorTask/hc_zzto4l_mw_training_4class_nom4l/hcZZ_big4class_nom4l_20261019}
( source /eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh; conda activate b_hive_new
  python3 -u $P/validate_scoring.py $W/validate_scoring.json --config $CFGM --model $TDIR/best_model.pt \
    --pair $OUT/hplusc_mva_4class_ctag2d_lepss_bkg $BKG $OUT/hplusc_mva_4class_ctag2d_jecshifts_bkg_scored_v1_jetfix \
    --pair $OUT/hplusc_mva_4class_ctag2d_lepss_fschad $SIG $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1_jetfix ) > $W/validate_scoring.log 2>&1
rc=$?; echo "validate_scoring exit $rc $(date)" | tee -a $W/limits.txt
# 2026-10-30 lepss: report only (the reference scored_v1_jetfix has the OLD lepton corrections, so non-score columns differ by design)
[ $rc -eq 0 ] || echo "scoring validation differences vs the old production (expected), see $W/validate_scoring.log"
( source /eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh; conda activate b_hive_new
  python3 -u $P/validate_performance.py $W/validate_performance.json --config $CFGM --model $TDIR/best_model.pt \
    --dataset $DSET --metrics $TDIR/validation_metrics.npz --filelist /eos/user/s/snandaku/b-hive_ttcc/filelists/hplusc_mva_4class_train.txt \
    --scored $BKG $SIG --nfiles 300 ) > $W/validate_performance.log 2>&1
echo "validate_performance exit $? (report only) $(date)" | tee -a $W/limits.txt

# 0. m4l sculpting check with this score (scripts of ../mass_sculpting_2026_10_27)
MS=/eos/user/s/snandaku/Analysis/mass_sculpting_2026_10_27; mkdir -p $W/sculpting
cp $MS/collect.py $MS/plot.py $MS/plot_an_style.py $MS/plot_window115135.py $MS/plot_zzscore.py $W/sculpting/
( cd $W/sculpting && SCORED=$BKG $PY collect.py && for x in plot plot_an_style plot_window115135 plot_zzscore; do $PY $x.py; done ) > $W/sculpting/run.log 2>&1
echo "sculpting done (exit $?) $(date)"
# 2026-10-28 eve (user): decorrelated / no-m4l / fsext models are judged on the 2D (m4l, score) fit only -> no 1D card
[ -n "$NO_1D" ] && { echo "NO_1D: stop after validation + sculpting $(date)"; exit 0; }

# 1. dump + card with the production optbin10 edges
build optbin10old /eos/user/s/snandaku/Analysis/unbinned_mva_2026_10_18/dump_events.py --dump $W/events.npz $A $ST $K --bin-edges $OPTBIN10 || exit 1
limit optbin10old
[ -n "$ONLY_OPTBIN10" ] && { echo "ONLY_OPTBIN10: stop after optbin10 (10-28 user) $(date)"; exit 0; }

# 2. re-optimised 10 bins (same optimiser/options as edges_smzx.txt S10)
( eval $LCG; python3 -u $OPT/optimise_binning.py $W/events.npz $W/opt_smzx.json --smooth-zx > $W/opt_smzx.log 2>&1 )
E10=$($PY -c "import json; d=json.load(open('$W/opt_smzx.json')); print(','.join(f'{x:.6f}' for x in d['K']['10']['edges']))")
echo "S10 edges: $E10" | tee -a $W/limits.txt
build S10 create_datacards_ctag2d_100150_stitched_decorr.py $A $ST $K --bin-edges $E10 || exit 1
limit S10

# 3. 2D: m4l edges x NSQ equal-signal score bins (signal = in-window nominal stitched signal of step 1)
SE=$($PY -c "
import numpy as np; ev=np.load('$W/events.npz'); s=ev['Signal|score']; w=np.abs(ev['Signal|weight']); o=np.argsort(s); c=np.cumsum(w[o]); c/=c[-1]
q=[float(np.interp(f,c,s[o])) for f in np.arange(1,$NSQ)/$NSQ]; print(','.join(f'{x:.6f}' for x in [0.0]+q+[1.0]))")
U=$($PY $P/create_datacards_unroll_m4l.py --print-edges $M4L $SE)
echo "2D: m4l $M4L x score $SE" | tee -a $W/limits.txt
build 2D $P/create_datacards_unroll_m4l.py --unroll-m4l-edges $M4L $A $ST $K --bin-edges $U || exit 1
limit 2D
echo "ALL DONE $(date)"
