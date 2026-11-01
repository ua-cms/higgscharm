#!/bin/bash
# 2026-10-22: 4FS-alone signal on the optimised 10-bin p_sig binning, current nominal config (K + --higgs-scale-norm, no capfix),
# compared with the stitched nominal optbin10 (r 427.0).
#   M0  = 4FS alone (73.30 fb), separate scalevar_muR_cH/muF_cH from the rebuilt-4FS class ratios, pdf_cH, NO FS nuisance  (10-16 L0)
#   T3  = 3FS alone at 55.0 fb, template source only
#   M3b = M0 + CMS_FS_cH_3FS4FS, Up = 3FS x 73.30 x R / 55.0, R = 55.0/83.38 = 0.660 (old plain-4FS cross-section-ratio envelope; 10-16 L3b)
#   M4  = M0 + CMS_FS_cH_3FS4FS with the emulated plain-4FS template (class x lead c-hadron pT; 10-16 L4lead)
EDGES=0.000000,0.077426,0.198279,0.381715,0.518877,0.634575,0.714366,0.808976,0.833804,0.874049,1.000000
P0=/eos/user/s/snandaku/Analysis/fs_stitch_direct_2026_10_07; P4=/eos/user/s/snandaku/Analysis/fs_stitch_new4fs_2026_10_13
P=/eos/user/s/snandaku/Analysis/optbin_2026_10_21/fs4fs
PF=/eos/user/s/snandaku/Analysis/ctag2d_capfix_decorr_2026_10_15; PP=/eos/user/s/snandaku/Analysis/fs_plain4fs_rw_2026_10_16
B=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/outputs
S=/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts
OUT=/eos/user/s/snandaku/higgscharm/outputs
A="--scored-dir $OUT/hplusc_mva_4class_ctag2d_scored_v8 --jecshifts-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_scored_v2 --automcstats-threshold 10 --eras 2022preEE 2022postEE 2023preBPix 2023postBPix --zx-dir /eos/user/s/snandaku/Analysis/zx_background_mva_CRmerged_105160_2026_10_07 --mass-window 105 160 --sumw-table /eos/user/s/snandaku/Analysis/sumw_fix_2026_10_02/sumw_table.json --fs-scored-dir $OUT/hplusc_mva_4class_ctag2d_fschad_scored_v1 --sumw-4fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad"
COM="--sumw-3fs-dir $OUT/hplusc_mva_4class_ctag2d_jecshifts_fschad --preveto-genc-4fs $P0/preveto_genc_4fs.json --ctag2d-capfix none --split-other-higgs --per-process-bkg-scale --higgs-scale-norm --bin-edges $EDGES"
declare -A F=( [M0]="--signal-mode 4fs --sigma-4fs-fb 97.62 --sigma-total-fb 73.30 --sig4fs-scale-json $P4/new4fs_pooled.json --pdf-ch 1.065 $COM"
               [T3]="--signal-mode 3fs --sigma-3fs-fb 55.0 --sigma-total-fb 0 $COM" )
PRE=combine_run3_105160_ctag2d_withzxCRmerged_4era_jecshifts_automcstats_sumwfix
declare -A T=( [M0]=sig4fsalone_sepscale_pdfcH_nocapfix_splithiggs_ppscale_higgsnorm_tthxs_optbin10 [T3]=sig3fsalone55_nocapfix_splithiggs_ppscale_higgsnorm_tthxs_optbin10 )
X=create_datacards_ctag2d_100150_stitched_decorr.py
C() { env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash /eos/user/s/snandaku/Analysis/fsrw_2026_10_02/run_combine.sh "$@"; }
echo "start $(date) $(hostname)"
for k in M0 T3; do
  DF=$B/${PRE}_${T[$k]}_2026_10_22; mkdir -p $DF
  ( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh; cd $S
    python3 -u $X $A ${F[$k]} --output $DF > $DF/build.log 2>&1; echo "build $k exited $? $(date)" ) &
done
wait
for k in M0 T3; do grep -q "\[Done\]" $B/${PRE}_${T[$k]}_2026_10_22/build.log || { echo "BUILD $k FAILED"; tail -20 $B/${PRE}_${T[$k]}_2026_10_22/build.log; exit 1; }; done
D0=$B/${PRE}_${T[M0]}_2026_10_22; D3=$B/${PRE}_${T[T3]}_2026_10_22
D3b=${D0/sig4fsalone/sig4fsalone_fsheyenR0660}; D4=${D0/sig4fsalone/sig4fsalone_fsplainlead}
( source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
  python3 $PF/add_fs_3fs4fs.py $D0 $D3 $D3b yield 48.351 55.0
  python3 $PP/add_fs_plain.py lead $D0 $D3 $D4 ) > $P/add_fs.log 2>&1 || { echo "ADD_FS FAILED"; cat $P/add_fs.log; exit 1; }
cat $P/add_fs.log
declare -A DIR=( [M0]=$D0 [M3b]=$D3b [M4]=$D4 )
for k in M0 M3b M4; do
  C ${DIR[$k]} datacard_mva_with_zx.txt ${k}_optbin10 > $P/limit_${k}.out 2>&1
  echo "limit $k: $(grep 'Expected' $P/limit_${k}.out | tr '\n' ' ') $(date)"
done
for k in M0 M3b M4; do
  C ${DIR[$k]} datacard_mva_with_zx.txt ${k}_optbin10_imp impacts > $P/impacts_${k}.out 2>&1 && echo "impacts $k done $(date)"
done
echo "ALL DONE $(date)"
