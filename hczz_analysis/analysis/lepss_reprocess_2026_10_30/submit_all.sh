#!/bin/bash
# 2026-10-30: full reprocessing with the corrected lepton scale/smearing (see Notes 2026-10-30).
# Same datasets as the 10-22 V4/V3 campaign / the fschad signal production, new workflow names (own output dirs).
export X509_USER_PROXY=$HOME/private/x509up_u178308
source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc11-opt/setup.sh >/dev/null 2>&1
H=/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm; cd $H
A=/eos/user/s/snandaku/Analysis/lepss_reprocess_2026_10_30
for p in "hplusc_mva_4class_ctag2d_jecshifts_bkg:hplusc_mva_4class_ctag2d_lepss_bkg" \
         "hplusc_mva_4class_ctag2d_jecshifts_fschad:hplusc_mva_4class_ctag2d_lepss_fschad" \
         "hplusc_mva_4class_CR_merged_jecv4:hplusc_mva_4class_CR_merged_lepss"; do
  old=${p%%:*}; new=${p##*:}
  for e in 2022preEE 2022postEE 2023preBPix 2023postBPix; do
    for d in $(ls condor/$old/$e); do
      grep -q "^$new $e $d " $A/submitted.txt && continue
      out=$(python3 submit_condor.py -w $new -y $e -d $d --nfiles 15 --output_format parquet --eos --jobflavor workday --submit 2>&1)
      cl=$(echo "$out" | grep -oE "submitted to cluster [0-9]+" | grep -oE "[0-9]+$")
      echo "$new $e $d ${cl:-FAILED}" | tee -a $A/submitted.txt
      [ -z "$cl" ] && echo "$out" | tail -5 >> $A/submit_errors.txt
    done
  done
done
