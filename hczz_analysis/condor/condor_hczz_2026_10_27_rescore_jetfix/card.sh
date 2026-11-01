#!/bin/bash
# card chain (sculpting, optbin10old = 429 edges, S10 re-optimised, 2D) on the jetfix scoring
M=$1; hostname; date
source /afs/cern.ch/user/s/snandaku/condor_hczz_2026_10_27_rescore_jetfix/models.sh $M
case $M in prod|nom4l9016|nom4l105160) TAG=${M}_jetfix ;; *) TAG=$M ;; esac
case $M in massinvlep|massinvlepdisco*|fsext*|prod) export ONLY_OPTBIN10=1 ;; esac   # 10-28 user: only 1D optbin10 (prod too from the 10-28 eve new-Z+X rerun)
case $M in prod) ;; *) export NO_1D=1 ;; esac   # 10-29 user: massinvlep λ0.5 2D only too (1D exemption removed)   # 10-28 eve user: prod = 1D only (m4l is an MVA input); all others = 2D only (card.sh = validation + sculpting)
bash /eos/user/s/snandaku/Analysis/nom4l_card_2026_10_27/pipeline.sh $TAG $BKG $SIG $ZX
rc=$?
# 10-27 (EOS space): the decorrelated models' event dump (2.5 GB, only used inside the pipeline) is removed at the end
case $M in massinv*|disco*|fsext*) rm -f /eos/user/s/snandaku/Analysis/nom4l_card_2026_10_27/$TAG/events.npz ;; esac
exit $rc
