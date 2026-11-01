#!/bin/bash
# wait for the decorrelated training of model $1 (condor 16525602, out train_job.<tag>.16525602.out)
M=$1; source /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_rescore_jetfix/models.sh $M
OUTF=$(ls -t /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_train_decor/train_job.$M.*.out 2>/dev/null | head -1)
hostname; date
while true; do
  [ -z "$OUTF" ] && OUTF=$(ls -t /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_train_decor/train_job.$M.*.out 2>/dev/null | head -1)
  grep -q "DATASET FAILED" $OUTF 2>/dev/null && { echo "DATASET FAILED"; exit 1; }
  if grep -q "^law exit" $OUTF 2>/dev/null; then
    grep -q "^law exit 0" $OUTF && [ -s $TDIR/best_model.pt ] && { echo "model ready $(date)"; ls -la $TDIR/best_model.pt; exit 0; }
    echo "TRAINING FAILED"; exit 1
  fi
  sleep 120
done
