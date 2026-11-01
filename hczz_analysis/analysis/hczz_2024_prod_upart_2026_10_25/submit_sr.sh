#!/bin/bash
# 2026-10-25: 2024 SR production rerun with the UParTAK4 jet-score columns (user: "yes submit SR").
# Same plan as hczz_2024_prod_2026_10_23 (SR lines only), but every input file pinned to a readable site up front.
source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc12-opt/setup.sh
export X509_USER_PROXY=$HOME/private/x509up_u178308
cd /afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm
P=/eos/home-s/snandaku/Analysis/hczz_2024_prod_upart_2026_10_25
: > $P/submitted.txt
while read wf ds nfiles njobs; do
  python3 submit_condor.py -w $wf -y 2024 -d $ds --output_format parquet --eos --jobflavor workday > $P/gen_$ds.log 2>&1
  J=condor/$wf/2024/$ds
  python3 $P/pin.py $J/partitions.json >> $P/pin.log
  n=$(wc -l < $J/jobnum.txt)
  cl=$(condor_submit $J/${wf}_${ds}.sub 2>&1 | grep -o "submitted to cluster [0-9]*" | awk '{print $4}')
  echo "$wf $ds $njobs $n ${cl:-FAILED}" | tee -a $P/submitted.txt
done < $P/plan_sr.txt
