#!/bin/bash
# 2026-10-12: after pipeline_105160.sh: pdf_cH copies of the full/shape cards, their limits, impacts of full+pdf_cH.
P=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
T=4era_jecshifts_automcstats_sumwfix_2026_10_12
DF=$B/combine_run3_105160_ctag2d_stitch3fs_fsprescribed_full_withzxCRmerged_$T
DS=$B/combine_run3_105160_ctag2d_stitch3fs_fsprescribed_shape_withzxCRmerged_$T
C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh "$@"; }
for d in $DF $DS; do python3 $P/add_pdfch.py $d/datacard_mva_with_zx.txt; done
C $DF datacard_mva_with_zx_pdfcH.txt stitch3fs_fsfull_pdfcH_105160_4era_withzx > $P/combine_stitch3fs_fsfull_pdfcH_105160_4era_withzx.out 2>&1 &
C $DS datacard_mva_with_zx_pdfcH.txt stitch3fs_fsshape_pdfcH_105160_4era_withzx > $P/combine_stitch3fs_fsshape_pdfcH_105160_4era_withzx.out 2>&1 &
wait
grep -H "Expected 50" $P/combine_stitch3fs_fs*_pdfcH_105160_4era_withzx.out
C $DF datacard_mva_with_zx_pdfcH.txt stitch3fs_fsfull_pdfcH_105160_imp impacts > $P/combine_stitch3fs_fsfull_pdfcH_105160_imp.out 2>&1 && echo "impacts done $(date)"
echo "PDFCH DONE $(date)"
