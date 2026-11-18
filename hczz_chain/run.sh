#!/bin/bash
# HcZZ analysis chain, one stage per call (hczz_chain/README.md). Each stage reproduces the command that made the 2026-11 results
# (condor_hczz_2026_11_09_final/*_final.sh, condor_hczz_2026_11_16_kf128), with the tools and inputs of this repository.
#   run.sh produce <era> [sr|sig|cr|all] [--submit]   submit the parquet production (submit_condor.py; dry run without --submit)
#   run.sh prodcheck <2223|2024|era...> [--wait]       every job of the production wrote its .coffea (completeness)
#   run.sh sumw                                         pre-veto generated Σw table of the 2022/23 MC (card normalisation)
#   run.sh score <era> <bkg|sig>                        MVA scores ($HCZZ_MODEL, out-of-fold for k-fold models) -> *_scored_<model>
#   run.sh zx <2223|2024>                               Z+X: merge CR parquets, estimate with the pkatris fake rates, MVA-score it
#   run.sh score <era> hb                               H+b samples with n_genb10 (2022/23, HCZZ_HB_MODE=stitched)
#   run.sh card2223                                     2022/23 2D (D, m4l) card: event dump -> bin optimiser -> 40-bin card -> limit
#   run.sh card2024 <kappa|plain> [stitched|4fs]        2024 2D card (D_kappa or P(S)), FS-stitched signal (default) or 4FS alone -> limit
#   run.sh combo <kappa|plain>                          2022/23 + 2024 combination (2024 card of that kind, 2024 nuisances decorrelated)
set -o pipefail
HERE=${HCZZ_REPO:+$HCZZ_REPO/hczz_chain}; HERE=${HERE:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}; source $HERE/config.sh   # condor: HCZZ_REPO from the submit file
TOOLS=$HERE/tools; INPUTS=$HERE/inputs; SCRIPTS=$HCZZ_REPO/combine/scripts; cd $HCZZ_REPO
RAWB=$HCZZ_OUTPUTS/$HCZZ_WF_SR; RAWS=$HCZZ_OUTPUTS/$HCZZ_WF_SIG
BKG=${RAWB}_scored_$HCZZ_MODEL; SIG=${RAWS}_scored_$HCZZ_MODEL                    # scored copies (same names as the 2026-11 chain)
RAWH=$HCZZ_OUTPUTS/$HCZZ_WF_HB; HBS=${RAWH}_scored_$HCZZ_MODEL; HBSFX=; [ "$HCZZ_HB_MODE" = stitched ] && HBSFX=_hbst   # stitched H+b (HCZZ_HB_MODE)
MO=$HCZZ_OUTPUTS/${HCZZ_WF_CR}_merged_perjet; ZXEST=$HCZZ_RUN/zx_estimate; ZX=$HCZZ_RUN/zx_mva_$HCZZ_MODEL
SUMW=$HCZZ_RUN/sumw; KT=k$(echo $HCZZ_KAPPA | tr -d . | tr , _); CARDS=$HCZZ_RUN/cards/${HCZZ_MODEL}_$KT
lcg() { source $HCZZ_LCG_POST >/dev/null 2>&1; }
log() { echo "[$(date '+%F %T')] $*"; }

limit() {  # <card dir> <datacard> <name> <limits.txt>: blind expected limit, AsymptoticLimits as in all 2026 results
  env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash -c "
  source /cvmfs/cms.cern.ch/cmsset_default.sh >/dev/null 2>&1; cd $HCZZ_COMBINE_SRC; eval \$(scramv1 runtime -sh 2>/dev/null)
  T=\$(mktemp -d /tmp/hczz_$3_XXXX); cp $1/* \$T/; cd \$T
  text2workspace.py $2 -o ws.root > t2w.log 2>&1 && combine -M AsymptoticLimits -m 120 --run blind --rAbsAcc 0.00001 --rRelAcc 0.00001 ws.root -n $3 > limit.log 2>&1
  cp t2w.log limit.log $1/; echo \"$3: \$(grep 'Expected 50.0%' limit.log) \$(date)\" >> $4; rm -rf \$T"
  grep -q "^$3: Expected" $4 || { log "LIMIT FAILED: $1/limit.log"; return 1; }
  grep "^$3:" $4 | tail -1
}

data_keys() {  # <era>: data datasets of the era fileset (the production runs on all of them)
  python3 -c "
import yaml, glob; f = glob.glob('analysis/filesets/$1_nanov*.yaml')[0]; d = yaml.safe_load(open(f))
print(' '.join(k for k, v in d.items() if isinstance(v, dict) and v.get('era') not in ('mc', 'signal', None)))"
}
mc_keys() {  # <sr|sig|cr> <era>: MC datasets of the final production (datasets.yaml)
  python3 -c "import yaml; print(' '.join(yaml.safe_load(open('$HERE/datasets.yaml'))['mc'].get('$1', {}).get('$2', [])))"
}

stage=$1; shift
case $stage in
produce)
  ERA=$1; WHAT=${2:-all}; SUBMIT=; [ "${3:-$2}" = --submit ] && SUBMIT=--submit; [ "$WHAT" = --submit ] && WHAT=all
  source $HCZZ_REPO/setup.sh >/dev/null; mkdir -p $HCZZ_RUN; touch $HCZZ_RUN/submitted.txt
  for k in sr sig cr hb; do
    [ "$WHAT" = all ] || [ "$WHAT" = $k ] || continue
    case $k in sr) W=$HCZZ_WF_SR; DS="$(mc_keys sr $ERA) $(data_keys $ERA)";; sig) W=$HCZZ_WF_SIG; DS=$(mc_keys sig $ERA);; hb) W=$HCZZ_WF_HB; DS=$(mc_keys hb $ERA);;
               cr) W=$HCZZ_WF_CR; DS=$(mc_keys cr $ERA); [ -n "$DS" ] && DS="$DS $(data_keys $ERA)";; esac
    for d in $DS; do
      grep -q "^$W $ERA $d " $HCZZ_RUN/submitted.txt && continue
      out=$(python3 submit_condor.py -w $W -y $ERA -d $d --nfiles $HCZZ_NFILES --output_format parquet --jobflavor $HCZZ_JOBFLAVOR $SUBMIT 2>&1)
      cl=$(echo "$out" | grep -oE "submitted to cluster [0-9]+" | grep -oE "[0-9]+$")
      if [ -n "$SUBMIT" ]; then echo "$W $ERA $d ${cl:-FAILED}" | tee -a $HCZZ_RUN/submitted.txt; else echo "dry run: $W $ERA $d"; fi
    done
  done ;;
prodcheck)
  WAIT=; ARGS=(); for a in "$@"; do [ "$a" = --wait ] && WAIT=--wait || ARGS+=("$a"); done
  python3 $TOOLS/prodcheck.py --workflows $HCZZ_WF_SR $HCZZ_WF_SIG $HCZZ_WF_CR --eras "${ARGS[@]}" $WAIT || exit 1
  H=$(echo " ${ARGS[*]} " | grep -oE " (2223|2022preEE|2022postEE|2023preBPix|2023postBPix)" | tr '\n' ' ')   # the H+b production is 2022/23 only
  [ "$HCZZ_HB_MODE" = stitched ] && [ -n "$H" ] && { python3 $TOOLS/prodcheck.py --workflows $HCZZ_WF_HB --eras $H $WAIT || exit 1; }; exit 0 ;;
sumw)
  lcg; mkdir -p $SUMW
  python3 -u $TOOLS/make_sumw_table.py --scored-dir $BKG --jecshifts-dir $BKG --fs-scored-dir $SIG --out $SUMW > $SUMW/sumw.log 2>&1 || { log "SUMW FAILED ($SUMW/sumw.log)"; exit 1; }
  log "Σw table $SUMW/sumw_table.json" ;;
score)
  ERA=$1; KIND=$2; case $KIND in sig) IN=$RAWS; OUT=$SIG;; hb) IN=$RAWH; OUT=$HBS;; *) IN=$RAWB; OUT=$BKG;; esac
  source $HCZZ_CONDA; conda activate $HCZZ_CONDA_ENV; E=$(mktemp)
  python3 -u analysis/postprocess/run_mva_postprocess.py --input-dir $IN --output-dir $OUT --model-name $HCZZ_MODEL --era $ERA \
    --no-mass-window --no-histograms --workers 8 ${HCZZ_SCORE_DATASETS:+--datasets $HCZZ_SCORE_DATASETS} 2> >(tee $E >&2); rc=$?; sleep 2   # HCZZ_SCORE_DATASETS: only these dataset prefixes
  n=$(grep -c 'Error processing' $E || true); rm -f $E
  [ "${n:-0}" -gt 0 ] && { log "SCORING ERRORS: $n files"; rc=4; }
  # 2026-11-16: every raw parquet must have a complete scored copy (same rows, score column): EOS quota/write losses fail the node
  [ $rc = 0 ] && { python3 -u $TOOLS/check_scored.py --raw-dir $IN --scored-dir $OUT --era $ERA ${HCZZ_SCORE_DATASETS:+--datasets $HCZZ_SCORE_DATASETS} || { log "SCORED OUTPUT INCOMPLETE"; rc=5; }; }
  exit $rc ;;
zx)
  P=$1; lcg; mkdir -p $ZXEST $ZX; export HCZZ_ZX_SUMW_BASE=$RAWB   # generated-Σw sidecars of the SR production (ZZ-in-3P1F correction)
  if [ "$P" = 2223 ]; then Y=all; FRF=$HCZZ_FR_DIR/2022preEE/fake_rates.pkl; ERAS="$HCZZ_ERAS_2223"; O2=$ZXEST
  else Y=2024; FRF=$HCZZ_FR_DIR/2024/fake_rates.pkl; ERAS=2024; O2=$ZXEST/2024; fi
  [ -f $FRF ] || { log "fake rates not readable: $FRF"; exit 1; }
  python3 -u scripts/merge_cr_parquets.py --workflow $HCZZ_WF_CR --year $Y --merge-out $MO --force > $ZXEST/merge_$P.log 2>&1 || { log "MERGE FAILED"; exit 1; }
  python3 -u scripts/estimate_zx_background.py --workflow $HCZZ_WF_CR --merged-base $MO --fake-rates $FRF --year $Y --mass-window $HCZZ_ZX_MASS \
    --output $O2 > $ZXEST/estimate_$P.log 2>&1 || { log "ESTIMATE FAILED"; exit 2; }
  python3 $TOOLS/check_zx_jets.py $ZXEST $ERAS || { log "Z+X JET CHECK FAILED"; exit 3; }
  python3 -u scripts/run_zx_inference.py --model-name $HCZZ_MODEL --input $ZXEST --output $ZX --eras $ERAS > $ZX/inference_$P.log 2>&1
  rc=$?; log "Z+X inference rc $rc, errors: $(grep -ci error $ZX/inference_$P.log)"; exit $rc ;;
card2223)
  W=$CARDS/2223$HBSFX; KC=$W/card_b0p1r0p5cat40; mkdir -p $W $KC; lcg; export HCZZ_KAPPA
  A="--scored-dir $BKG --jecshifts-dir $BKG --automcstats-threshold 10 --eras $HCZZ_ERAS_2223 --zx-dir $ZX --mass-window 105 160 --sumw-table $SUMW/sumw_table.json --fs-scored-dir $SIG --sumw-4fs-dir $RAWS"
  ST="--signal-mode stitched --sigma-4fs-fb $HCZZ_SIGMA_4FS_FB --sigma-3fs-fb $HCZZ_SIGMA_3FS_FB --sigma-total-fb $HCZZ_SIGMA_TOTAL_FB --sumw-3fs-dir $RAWS --preveto-genc-4fs $INPUTS/preveto_genc_4fs.json --pdf-ch $HCZZ_PDF_CH --gg-scale-json-dir $INPUTS/gg_scale_new4fs"
  K="--ctag2d-capfix none --fs-prescribed full --fs-scale-mode separate --split-other-higgs --per-process-bkg-scale --higgs-scale-norm"
  [ -n "$HBSFX" ] && K="$K --hb-mode stitched --hb-scored-dir $HBS --hb-sumw-dir $RAWH"
  ( cd $SCRIPTS; python3 -u $TOOLS/dump_events_2d.py --dump $W/events_2d.npz $A $ST $K --output $W/card_dumpbuild > $W/dump.log 2>&1 ) || { log "DUMP FAILED"; exit 1; }
  rm -rf $W/card_dumpbuild
  ( cd $W; python3 -u $TOOLS/optimise_2d.py events_2d.npz opt_b0.1_r0.5.json $HCZZ_OPT > opt_b0.1_r0.5.log 2>&1 && \
    python3 -c "import json; d = json.load(open('opt_b0.1_r0.5.json')); json.dump({'cats': d['cat']['40']['cats']}, open('layout_b0p1r0p5cat40.json', 'w'), indent=1)" ) || { log "OPTIMISER FAILED"; exit 2; }
  Lc=$(python3 $TOOLS/create_datacards_unroll_2d.py --print-cat-edges $W/layout_b0p1r0p5cat40.json); SC=$(echo "$Lc" | sed -n 1p); U=$(echo "$Lc" | sed -n 2p)
  echo "cat40 --unroll-score-cats $SC bin-edges $U" > $KC/layout.txt
  ( cd $SCRIPTS; python3 -u $TOOLS/create_datacards_unroll_2d.py --unroll-score-cats $SC $A $ST $K --bin-edges $U --output $KC > $KC/build.log 2>&1 )
  grep -q "\[Done\]" $KC/build.log || { log "CARD BUILD FAILED ($KC/build.log)"; exit 3; }
  rm -f $W/events_2d.npz   # 2-4 GB, the card dir has everything the limit needs
  limit $KC datacard_mva_with_zx.txt binned_cat40 $W/limits.txt ;;
card2024)
  T=${1:-kappa}; SM=${2:-$HCZZ_SIGNAL_2024}; if [ $T = plain ]; then unset HCZZ_KAPPA; else export HCZZ_KAPPA; fi
  W=$CARDS/2024_$T$([ $SM = 4fs ] && echo _4fs)$HBSFX; KC=$W/card_b0p1r0p5cat40; mkdir -p $W $KC; lcg; export HCZZ_ALLOW_MISSING_SYST=$HCZZ_ALLOW_MISSING_SYST_2024
  if [ $SM = stitched ]; then   # 4FS n_genc10 == 0 + 3FS (Stitch3FS_cH_*_2024) n_genc10 >= 1, as 2022/23 (2026-11-16)
    SIGA="--signal-mode stitched --sigma-4fs-fb $HCZZ_SIGMA_4FS_FB --sigma-3fs-fb $HCZZ_SIGMA_3FS_FB --sigma-total-fb $HCZZ_SIGMA_TOTAL_FB --preveto-genc-4fs $INPUTS/preveto_genc_4fs.json --gg-scale-json-dir $INPUTS/gg_scale_new4fs --fs-prescribed full --fs-scale-mode separate"
  else
    SIGA="--signal-mode 4fs --sigma-4fs-fb $HCZZ_SIGMA_4FS_FB --sigma-total-fb $HCZZ_SIGMA_TOTAL_FB --sig4fs-scale-json $INPUTS/new4fs_pooled.json"
  fi
  A="--scored-dir $BKG --jecshifts-dir $BKG --sumw-dir $RAWB --zx-dir $ZX --automcstats-threshold 10 --eras 2024 --mass-window 105 160 --fs-scored-dir $BKG \
   --sumw-4fs-dir $RAWB --sumw-3fs-dir $RAWB $SIGA --pdf-ch $HCZZ_PDF_CH --ctag2d-capfix none --split-other-higgs --per-process-bkg-scale --higgs-scale-norm"
  [ -n "$HBSFX" ] && A="$A --hb-mode stitched --hb-scored-dir $BKG --hb-sumw-dir $RAWB"   # no 2024 4FS H+b: the SR 5FS sample over its whole phase space at K_5FS
  ( cd $SCRIPTS; python3 -u $TOOLS/dump_events_2d_2024.py --dump $W/events_2d.npz $A --output $W/card_dumpbuild > $W/dump.log 2>&1 ) || { log "DUMP FAILED"; exit 1; }
  rm -rf $W/card_dumpbuild
  ( cd $W; python3 -u $TOOLS/optimise_2d_2024.py events_2d.npz opt_b0.1_r0.5.json $HCZZ_OPT > opt_b0.1_r0.5.log 2>&1 && \
    python3 -c "import json; d = json.load(open('opt_b0.1_r0.5.json')); json.dump({'cats': d['cat']['40']['cats']}, open('layout_b0p1r0p5cat40.json', 'w'), indent=1)" ) || { log "OPTIMISER FAILED"; exit 2; }
  Lc=$(python3 $TOOLS/create_datacards_unroll_2d_2024.py --print-cat-edges $W/layout_b0p1r0p5cat40.json); SC=$(echo "$Lc" | sed -n 1p); U=$(echo "$Lc" | sed -n 2p)
  echo "cat40 --unroll-score-cats $SC bin-edges $U" > $KC/layout.txt
  ( cd $SCRIPTS; python3 -u $TOOLS/create_datacards_unroll_2d_2024.py --unroll-score-cats $SC $A --bin-edges $U --output $KC > $KC/build.log 2>&1 )
  grep -q "\[Done\]" $KC/build.log || { log "CARD BUILD FAILED ($KC/build.log)"; exit 3; }
  rm -f $W/events_2d.npz
  limit $KC datacard_mva_with_zx.txt binned_cat40 $W/limits.txt ;;
combo)
  T=${1:-kappa}; C23=$CARDS/2223$HBSFX/card_b0p1r0p5cat40; C24=$CARDS/2024_$T$HBSFX/card_b0p1r0p5cat40; W=$HCZZ_RUN/combo/${HCZZ_MODEL}_${KT}_2024$T$HBSFX; mkdir -p $W
  env -i HOME=$HOME USER=$USER PATH=/usr/bin:/bin bash -c "
  source /cvmfs/cms.cern.ch/cmsset_default.sh >/dev/null 2>&1; cd $HCZZ_COMBINE_SRC; eval \$(scramv1 runtime -sh 2>/dev/null)
  T=\$(mktemp -d /tmp/hczz_comb_XXXX); cd \$T; mkdir y2223 y2024; cp $C23/* y2223/
  python3 $TOOLS/decorrelate_2024.py $C24 datacard_mva_with_zx.txt y2024 > $W/decorrelate.log 2>&1 || exit 1
  combineCards.py y2223=y2223/datacard_mva_with_zx.txt y2024=y2024/datacard_mva_with_zx.txt > comb.txt; cp comb.txt $W/
  for c in comb y2024; do [ \$c = comb ] && card=comb.txt || card=y2024/datacard_mva_with_zx.txt
    text2workspace.py \$card -o ws_\$c.root > t2w_\$c.log 2>&1 && combine -M AsymptoticLimits -m 120 --run blind --rAbsAcc 0.00001 --rRelAcc 0.00001 ws_\$c.root -n \$c > limit_\$c.log 2>&1
    cp t2w_\$c.log limit_\$c.log $W/; echo \"\$c: \$(grep 'Expected 50.0%' limit_\$c.log) \$(date)\" >> $W/limits.txt; done; rm -rf \$T" || { log "COMBINATION FAILED ($W)"; exit 1; }
  cat $W/limits.txt ;;
*) sed -n 3,11p $0; exit 1 ;;
esac
