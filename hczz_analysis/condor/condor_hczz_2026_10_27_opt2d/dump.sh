#!/bin/bash
# 2026-10-27: fine-m4l event dump (dump_events_2d.py) for the joint 2D bin optimisation; same builder args as pipeline.sh
M=${1:-nom4l9016}; hostname; date
source /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_rescore_jetfix/models.sh $M
D=/eos/user/s/snandaku/Analysis/opt2d_2026_10_27; W=$D/$M; mkdir -p $W/card_dumpbuild
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P4=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
OUT=/eos/user/s/snandaku/higgscharm/outputs; S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
A="--scored-dir $BKG --jecshifts-dir $BKG --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir $ZX --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/v4v3_card_2026_10_23/sumw_table.json --fs-scored-dir $SIG --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json --pdf-ch 1.065 --gg-scale-json-dir $P4/out"
K="--ctag2d-capfix none --fs-prescribed full --fs-scale-mode separate --split-other-higgs --per-process-bkg-scale --higgs-scale-norm"
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; cd $S
python3 -u $D/dump_events_2d.py --dump $W/events_2d.npz $A $ST $K --output $W/card_dumpbuild > $W/dump.log 2>&1
rc=$?; echo "exit $rc $(date)"; exit $rc
