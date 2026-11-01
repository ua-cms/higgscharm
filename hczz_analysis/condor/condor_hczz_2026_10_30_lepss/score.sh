#!/bin/bash
TMPDIR=${_CONDOR_SCRATCH_DIR:-${TMPDIR:-/tmp}}
# 2026-10-30: score the lepss reprocessing (corrected lepton scale/smearing). usage: score.sh <model> <era> <bkg|sig>
M=$1; ERA=$2; KIND=$3; hostname; date
source /eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh; conda activate b_hive_new
H=/eos/user/s/snandaku/Higgscharmnew/higgscharm; cd $H
source /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_30_lepss/models.sh $M
O=/eos/user/s/snandaku/higgscharm/outputs
if [ "$KIND" = bkg ]; then IN=$O/hplusc_mva_4class_ctag2d_lepss_bkg; OUT=$BKG; else IN=$O/hplusc_mva_4class_ctag2d_lepss_fschad; OUT=$SIG; fi
grep -q "'jet_pt', 'cjets_pt'" analysis/postprocess/mva_inference.py || { echo "SCORER NOT FIXED"; echo "exit 3"; exit 3; }
python3 -u analysis/postprocess/run_mva_postprocess.py --input-dir $IN --output-dir $OUT --config $CFGM --model $TDIR/best_model.pt \
  --era $ERA --no-mass-window --no-histograms --workers 8 2> >(tee $TMPDIR/score_err.log >&2)
rc=$?
# 10-28 eve: run_mva_postprocess exits 0 even when files fail to write (EOS quota: thousands of 'Errno 122' per job) -> fail here
sleep 2; NERR=$(grep -c 'Error processing' $TMPDIR/score_err.log 2>/dev/null || true)
[ "${NERR:-0}" -gt 0 ] && { echo "SCORING ERRORS: $NERR files failed"; rc=4; }
echo "exit $rc $(date)"; exit $rc
