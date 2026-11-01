#!/bin/bash
# 2026-10-28: optimise the 2D (m4l, score) binning for model $1 (b >= 0.1, MC stat <= 50%, as the nom4l best 444.5),
# then build + combine cat40, cat30 and the best rectangular layout with <= 40 bins
M=$1; hostname; date
D=/eos/user/s/snandaku/Analysis/opt2d_2026_10_27; W=$D/$M; cd $W
source /cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh
python3 -u $D/optimise_2d.py events_2d.npz opt_b0.1_r0.5.json --km 16 --ks 8 --budgets 20,30,40,50 --bmin 0.1 --relmax 0.5 > opt_b0.1_r0.5.log 2>&1 || { echo "OPT FAILED"; exit 1; }
case $M in massinvlep|massinvlepdisco*|fsext*|prod) ONLY40=1 ;; esac   # 10-28 user: only the 2D optimised cat40 (prod too from 10-28 eve)
if [ -z "$ONLY40" ]; then python3 -u $D/optimise_2d.py events_2d.npz opt_b0.1_r0.5_half.json --half --km 16 --ks 8 --budgets 20,30,40,50 --bmin 0.1 --relmax 0.5 > opt_b0.1_r0.5_half.log 2>&1 || echo "half check failed (continuing)"; fi
python3 - <<'PY'
import json
d=json.load(open('opt_b0.1_r0.5.json'))
for b in ('40','30'):
    json.dump({'cats':d['cat'][b]['cats']},open(f'layout_b0p1r0p5cat{b}.json','w'),indent=1)
r=[(v['sumZ2_eval'],k,v) for k,v in d['rect'].items() if v['nbins']<=40]
z,k,v=max(r)
open('rect_best.txt','w').write(f"b0p1r0p5rect{k} {','.join(str(int(x)) for x in v['m4l_edges'])} {','.join('%.6f'%x for x in v['score_edges'])}\n")
print('rect best',k,z)
PY
read RN RM RS < rect_best.txt
L=/afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_opt2d/layout.sh
bash $L $M b0p1r0p5cat40 cat $W/layout_b0p1r0p5cat40.json
[ -n "$ONLY40" ] && { rm -f $W/events_2d.npz; echo "ONLY40 done $(date)"; cat $W/limits.txt; exit 0; }
bash $L $M b0p1r0p5cat30 cat $W/layout_b0p1r0p5cat30.json
bash $L $M $RN rect $RM $RS
echo "done $(date)"; cat $W/limits.txt
