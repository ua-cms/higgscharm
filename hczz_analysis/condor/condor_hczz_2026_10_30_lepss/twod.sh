#!/bin/bash
# 2026-10-30 lepss: 2D chain for a decorrelated model on the lepss production:
#  (1) opt2d: (m4l, score) dump -> optimise_2d (b>=0.1, MC stat<=50%) -> cat40 card + limit (as condor_hczz_2026_10_27_opt2d)
#  (2) unbinned: KDE pdfs -> DSCB(m4l|u) -> pdfs_dscb; inclusive DSCB cards (cat40, 55x40);
#      final-state split: dump_ch -> dscb_ch (our e/mu scale/res from the shifted samples) -> make_card_ch (cat40, 55x40)
#  -> limits.txt
M=$1; hostname; date
source /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_30_lepss/models.sh $M
L=/eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30; W=$L/twod/$M; mkdir -p $W
D=/eos/user/s/snandaku/Analysis/opt2d_2026_10_27; N=/eos/user/s/snandaku/Analysis/unbinned2d_2026_10_30
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P4=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
OUT=/eos/user/s/snandaku/higgscharm/outputs; S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
A="--scored-dir $BKG --jecshifts-dir $BKG --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir $ZX --mass-window 105 160 --sumw-table $L/sumw/sumw_table.json --fs-scored-dir $SIG --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_lepss_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_lepss_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json --pdf-ch 1.065 --gg-scale-json-dir $P4/out"
K="--ctag2d-capfix none --fs-prescribed full --fs-scale-mode separate --split-other-higgs --per-process-bkg-scale --higgs-scale-norm"
LCG="source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh"
combine1() {  # <card dir> <datacard> <name>
  env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash -c "
  source /cvmfs/cms.cern.ch/cmsset_default.sh >/dev/null 2>&1; cd /eos/user/s/snandaku/Analysis/combine/CMSSW_14_1_0_pre4/src; eval \$(scramv1 runtime -sh 2>/dev/null)
  T=\$(mktemp -d /tmp/lepss_${M}_XXXX); cp $1/* \$T/; cd \$T
  text2workspace.py $2 -o ws.root > t2w.log 2>&1 && combine -M AsymptoticLimits -m 120 --run blind --rAbsAcc 0.00001 --rRelAcc 0.00001 ws.root -n $3 > limit.log 2>&1
  cp t2w.log limit.log $1/; echo \"$3: \$(grep 'Expected 50.0%' limit.log) \$(date)\" >> $W/limits.txt; rm -rf \$T"
}
# (1) opt2d  (10-31: skipped on a rescue rerun when the cat40 card + limit already exist)
KC=$W/card_b0p1r0p5cat40
if [ -f $W/events_2d.npz ] && grep -q "\[Done\]" $KC/build.log 2>/dev/null && grep -q "binned cat40: Expected" $W/limits.txt 2>/dev/null; then
  echo "opt2d outputs present, skipping step 1"; sed -i 's/^cat40 unroll-score-cats/cat40 --unroll-score-cats/' $KC/layout.txt
else
( eval $LCG; cd $S; python3 -u $D/dump_events_2d.py --dump $W/events_2d.npz $A $ST $K --output $W/card_dumpbuild > $W/dump.log 2>&1 ) || { echo "DUMP FAILED"; exit 1; }
rm -rf $W/card_dumpbuild
( eval $LCG; cd $W; python3 -u $D/optimise_2d.py events_2d.npz opt_b0.1_r0.5.json --km 16 --ks 8 --budgets 20,30,40,50 --bmin 0.1 --relmax 0.5 > opt_b0.1_r0.5.log 2>&1
  python3 -c "import json; d=json.load(open('opt_b0.1_r0.5.json')); json.dump({'cats': d['cat']['40']['cats']}, open('layout_b0p1r0p5cat40.json','w'), indent=1)" ) || { echo "OPT FAILED"; exit 1; }
KC=$W/card_b0p1r0p5cat40; mkdir -p $KC
( eval $LCG; B=$D/create_datacards_unroll_2d.py; Lc=$(python3 $B --print-cat-edges $W/layout_b0p1r0p5cat40.json); SC=$(echo "$Lc" | sed -n 1p); U=$(echo "$Lc" | sed -n 2p)
  echo "cat40 --unroll-score-cats $SC bin-edges $U" > $KC/layout.txt; cd $S; python3 -u $B --unroll-score-cats $SC $A $ST $K --bin-edges $U --output $KC > $KC/build.log 2>&1 )
grep -q "\[Done\]" $KC/build.log || { echo "CAT40 BUILD FAILED"; exit 1; }
env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh $KC datacard_mva_with_zx.txt ${M}_lepss_cat40 > $W/limit_cat40.out 2>&1
echo "binned cat40: $(grep 'Expected 50' $W/limit_cat40.out) $(date)" | tee -a $W/limits.txt
fi
# (2) unbinned
( eval $LCG
  python3 -u $N/make_pdfs_2d.py $W/events_2d.npz $KC $W/pdfs > $W/pdfs.log 2>&1 || exit 1
  python3 -u $N/dscb_cond.py $W/events_2d.npz $W/pdfs/pdfs_2d.npz $W/dscb > $W/dscb_cond.log 2>&1 || exit 2
  python3 -u $N/make_pdfs_2d.py $W/events_2d.npz $KC $W/pdfs_dscb --dscb $W/dscb/dscb_cond.json > $W/pdfs_dscb.log 2>&1 || exit 3
  cd $S; python3 -u $N/dump_events_2d_ch.py --dump $W/events_2d_ch.npz $A $ST $K --output $W/card_dumpbuild_ch > $W/dump_ch.log 2>&1 || exit 4
  rm -rf $W/card_dumpbuild_ch
  python3 -u $N/dscb_ch.py $W/events_2d_ch.npz $W/pdfs_dscb/pdfs_2d.npz $W/dscb_ch > $W/dscb_ch.log 2>&1 || exit 5
  for b in cat40 fine:55x40; do
    python3 -u $N/make_card_2d.py $W/pdfs_dscb/pdfs_2d.npz $KC $W/card_dscb_${b/fine:/} --binning $b >> $W/cards_dscb.log 2>&1 || exit 6
    python3 -u $N/make_card_ch.py $W/events_2d_ch.npz $W/pdfs_dscb/pdfs_2d.npz $W/dscb_ch/dscb_ch.json $KC $W/card_ch_${b/fine:/} --binning $b >> $W/cards_ch.log 2>&1 || exit 7
  done ) || { echo "UNBINNED STEP FAILED ($?)"; exit 1; }
combine1 $W/card_dscb_cat40 datacard_2d.txt dscb_cat40 & combine1 $W/card_ch_cat40 datacard_ch.txt ch_cat40 &
combine1 $W/card_dscb_55x40 datacard_2d.txt dscb_55x40 & combine1 $W/card_ch_55x40 datacard_ch.txt ch_55x40 & wait
grep -h "CMS_s\|CMS_r" $W/dscb_ch.log | grep "_e\|_m" > $W/lepton_terms.txt
rm -f $W/events_2d.npz   # EOS space (2.5 GB); events_2d_ch.npz kept for the channel-split studies
cat $W/limits.txt; echo "done $(date)"
