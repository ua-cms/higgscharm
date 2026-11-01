#!/bin/bash
# 2026-10-02: official Run 3 H->gg H+c 3FS / 4FS FxFx GEN c-hadron spectra split by multiplicity,
# for the FS.pdf stitching applied to our 4FS H->ZZ by reweighting. XSDB 4FS FxFx = 0.09762 pb.
W=/eos/user/s/snandaku/Analysis/fs_stitch_gg_2026_10_02
export X509_USER_PROXY=/tmp/x509up_u178308
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
echo "start $(date) $(hostname)"
for s in 2022preEE:R322 2022postEE:R3EE 2023preBPix:R323 2023postBPix:R323BPix; do
  IFS=: read era p <<< "$s"
  ( python3 -u $W/derive_chad_n.py $W/out/gg_$era $W/filelists/${p}_GG_3FS.txt $W/filelists/${p}_GG_4FSFXFX.txt 0.09762 > $W/logs/$era.log 2>&1; echo EXIT=$? >> $W/logs/$era.log ) &
done
wait
echo "ALL DONE $(date)"
