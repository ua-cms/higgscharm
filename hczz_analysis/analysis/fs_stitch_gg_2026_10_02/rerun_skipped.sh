#!/bin/bash
# 2026-10-02: rerun the eras that lost files to transient xrootd errors in pass 1, with retries.
W=/eos/user/s/snandaku/Analysis/fs_stitch_gg_2026_10_02
export X509_USER_PROXY=/tmp/x509up_u178308
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
echo "start $(date) $(hostname)"
one() {  # era prefix
  [ -f $W/out/gg_$1.json ] && cp $W/out/gg_$1.json $W/out/gg_$1_pass1.json
  python3 -u $W/derive_chad_n.py $W/out/gg_$1 $W/filelists/${2}_GG_3FS.txt $W/filelists/${2}_GG_4FSFXFX.txt 0.09762 > $W/logs/$1_pass2.log 2>&1
  echo "EXIT=$?" >> $W/logs/$1_pass2.log
}
one 2022preEE R322 &
# wait for pass 1 of 2023preBPix to end before rerunning it
until grep -q "^EXIT=" $W/logs/2023preBPix.log 2>/dev/null; do sleep 60; done
one 2023preBPix R323 &
wait
echo "ALL DONE $(date)"
