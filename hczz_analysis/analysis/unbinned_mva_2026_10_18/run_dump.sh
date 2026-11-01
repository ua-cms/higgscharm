#!/bin/bash
# 2026-10-18: card-K inputs (no capfix, 33 sources) dumped per event for the unbinned MVA fit
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P4=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
OUT=/eos/user/s/snandaku/higgscharm/outputs
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_scored_v8 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_scored_v2 --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_105160_2026_10_07 --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02/sumw_table.json --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
ST="--signal-mode stitched --sigma-4fs-fb 97.62 --sigma-3fs-fb 55.0 --sigma-total-fb 73.30 --sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json --pdf-ch 1.065 --gg-scale-json-dir $P4/out"
F="--ctag2d-capfix none --fs-prescribed full --fs-scale-mode separate --split-other-higgs --per-process-bkg-scale"
D=/eos/user/s/snandaku/Analysis/unbinned_mva_2026_10_18
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
cd /eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
python3 -u $D/dump_events.py --dump $D/events_K_nocapfix.npz $A $ST $F --output $D/card_binned_K_nocapfix > $D/dump.log 2>&1
echo "dump exited $? $(date)"
