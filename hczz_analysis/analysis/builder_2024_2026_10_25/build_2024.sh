#!/bin/bash
# 2026-10-25: first real 2024 card (UParT-fixed production + _scored_2024_v2), optbin10 edges,
# plain 4FS signal, no Z+X (user 10-23), lumi_Run3 still the 2022/23 value (2024 value pending).
set -e
P=/eos/user/s/snandaku/Analysis/builder_2024_2026_10_25
O=/eos/user/s/snandaku/higgscharm/outputs
S=$O/hplusc_mva_4class_ctag2d_jecshifts_scored_2024_v2
R=$O/hplusc_mva_4class_ctag2d_jecshifts
EDGES=0.000000,0.077426,0.198279,0.381715,0.518877,0.634575,0.714366,0.808976,0.833804,0.874049,1.000000
OUT=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs/combine_2024_105160_ctag2d_4fs_pdfcH_nozx_upart_jecshifts_automcstats_decorr_nocapfix_sepscale_splithiggs_ppscale_higgsnorm_optbin10_2026_10_25
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
cd /eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
python3 -u create_datacards_2024.py --scored-dir $S --jecshifts-dir $S --sumw-dir $R --skip-zx \
  --automcstats-threshold 10 --eras 2024 --mass-window 105 160 --fs-scored-dir $S \
  --sumw-4fs-dir $R --sumw-3fs-dir $R --signal-mode 4fs --sigma-4fs-fb 97.62 --sigma-total-fb 73.30 \
  --sig4fs-scale-json /eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13/new4fs_pooled.json \
  --pdf-ch 1.065 --ctag2d-capfix none --split-other-higgs --per-process-bkg-scale --higgs-scale-norm \
  --bin-edges $EDGES --output $OUT
echo "Build exited $? $(date)"
bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh $OUT datacard_no_zx.txt 2024_optbin10_4fs_nozx
echo "ALL DONE $(date)"
