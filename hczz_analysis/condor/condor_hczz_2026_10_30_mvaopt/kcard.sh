#!/bin/bash
# 2026-10-30 MVA optimisation: 2D cat40 card (opt2d recipe) for model $1 with the kappa discriminant (HCZZ_KAPPA=$3) or p_S ($2 = pS)
#  (1) opt2d: (m4l, score) dump -> optimise_2d (b>=0.1, MC stat<=50%) -> cat40 card + limit (as condor_hczz_2026_10_27_opt2d)
#  (2) unbinned: KDE pdfs -> DSCB(m4l|u) -> pdfs_dscb; inclusive DSCB cards (cat40, 55x40);
#      final-state split: dump_ch -> dscb_ch (our e/mu scale/res from the shifted samples) -> make_card_ch (cat40, 55x40)
#  -> limits.txt
M=$1; KTAG=$2; [ "$KTAG" != pS ] && export HCZZ_KAPPA=$3; hostname; date; echo "kappa ${HCZZ_KAPPA:-none}"
source /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_rescore_jetfix/models.sh $M   # 10-29 production (fixed Z+X), as the 456.0 card
L=/eos/user/s/snandaku/Analysis/mva_opt_2026_10_30; W=$L/kcard/${M}_$KTAG; mkdir -p $W
D=/eos/user/s/snandaku/Analysis/opt2d_2026_10_27; N=/eos/user/s/snandaku/Analysis/unbinned2d_2026_10_30
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P4=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
OUT=/eos/user/s/snandaku/higgscharm/outputs; S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
A="--scored-dir $BKG --jecshifts-dir $BKG --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir $ZX --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/v4v3_card_2026_10_23/sumw_table.json --fs-scored-dir $SIG --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json --pdf-ch 1.065 --gg-scale-json-dir $P4/out"
K="--ctag2d-capfix none --fs-prescribed full --fs-scale-mode separate --split-other-higgs --per-process-bkg-scale --higgs-scale-norm"
LCG="source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh"
combine1() {  # <card dir> <datacard> <name>
  env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash -c "
  source /cvmfs/cms.cern.ch/cmsset_default.sh >/dev/null 2>&1; cd /eos/user/s/snandaku/Analysis/combine/CMSSW_14_1_0_pre4/src; eval \$(scramv1 runtime -sh 2>/dev/null)
  T=\$(mktemp -d /tmp/lepss_${M}_XXXX); cp $1/* \$T/; cd \$T
  text2workspace.py $2 -o ws.root > t2w.log 2>&1 && combine -M AsymptoticLimits -m 120 --run blind --rAbsAcc 0.00001 --rRelAcc 0.00001 ws.root -n $3 > limit.log 2>&1
  cp t2w.log limit.log $1/; echo \"$3: \$(grep 'Expected 50.0%' limit.log) \$(date)\" >> $W/limits.txt; rm -rf \$T"
}
# (1) opt2d
( eval $LCG; cd $S; python3 -u $D/dump_events_2d.py --dump $W/events_2d.npz $A $ST $K --output $W/card_dumpbuild > $W/dump.log 2>&1 ) || { echo "DUMP FAILED"; exit 1; }
rm -rf $W/card_dumpbuild
( eval $LCG; cd $W; python3 -u $D/optimise_2d.py events_2d.npz opt_b0.1_r0.5.json --km 16 --ks 8 --budgets 20,30,40,50 --bmin 0.1 --relmax 0.5 > opt_b0.1_r0.5.log 2>&1
  python3 -c "import json; d=json.load(open('opt_b0.1_r0.5.json')); json.dump({'cats': d['cat']['40']['cats']}, open('layout_b0p1r0p5cat40.json','w'), indent=1)" ) || { echo "OPT FAILED"; exit 1; }
KC=$W/card_b0p1r0p5cat40; mkdir -p $KC
( eval $LCG; B=$D/create_datacards_unroll_2d.py; Lc=$(python3 $B --print-cat-edges $W/layout_b0p1r0p5cat40.json); SC=$(echo "$Lc" | sed -n 1p); U=$(echo "$Lc" | sed -n 2p)
  echo "cat40 unroll-score-cats $SC bin-edges $U" > $KC/layout.txt; cd $S; python3 -u $B --unroll-score-cats $SC $A $ST $K --bin-edges $U --output $KC > $KC/build.log 2>&1 )
grep -q "\[Done\]" $KC/build.log || { echo "CAT40 BUILD FAILED"; exit 1; }
env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh $KC datacard_mva_with_zx.txt ${M}_lepss_cat40 > $W/limit_cat40.out 2>&1
echo "binned cat40: $(grep 'Expected 50' $W/limit_cat40.out) $(date)" | tee -a $W/limits.txt
rm -f $W/events_2d.npz; cat $W/limits.txt; echo "done $(date)"
