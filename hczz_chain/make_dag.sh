#!/bin/bash
# Write the HTCondor DAG of the chain after the production (run.sh produce ... --submit submits the parquet jobs; this DAG starts
# with the completeness gates).  make_dag.sh [2223|2024|all] -> $HCZZ_RUN/dag/chain.dag ; submit with condor_submit_dag from there.
#   2223: gate -> scoring (bkg + sig per era) + Z+X -> Σw -> card2223 (D_kappa)
#   2024: gate -> scoring + Z+X -> card2024 kappa + plain
#   all : both, then the 2022/23 + 2024 combinations (kappa, plain)
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd); source $HERE/config.sh; P=${1:-all}
D=$HCZZ_RUN/dag; mkdir -p $D; cd $D
node() {  # <name> <flavour> <cpus> <mem MB> <run.sh args...>
  local n=$1 f=$2 c=$3 m=$4; shift 4
  cat > $n.sub <<SUB
executable = $HERE/run.sh
arguments = $*
output = $n.\$(ClusterId).out
error = $n.\$(ClusterId).err
log = $n.\$(ClusterId).log
getenv = HCZZ_*
environment = "HCZZ_REPO=$HCZZ_REPO"
transfer_executable = false
MY.SendCredential = True
requirements = (TARGET.OpSysAndVer =?= "AlmaLinux9")
request_CPUs = $c
request_memory = $m
+JobFlavour = "$f"
queue
SUB
  echo "JOB $n $n.sub" >> chain.dag; echo "RETRY $n 1" >> chain.dag
}
: > chain.dag; echo "# HcZZ chain $(date '+%F %T'), model $HCZZ_MODEL, kappa $HCZZ_KAPPA, run dir $HCZZ_RUN" >> chain.dag
S23=; S24=
if [ $P = 2223 ] || [ $P = all ]; then
  node G23 nextweek 1 2000 prodcheck 2223 --wait
  for e in $HCZZ_ERAS_2223; do for k in bkg sig; do node S_${e}_$k workday 8 16000 score $e $k; S23="$S23 S_${e}_$k"; done; done
  node ZX23 nextweek 4 16000 zx 2223; node SUMW workday 4 8000 sumw; node C23 tomorrow 12 24000 card2223
  echo "PARENT G23 CHILD $S23 ZX23" >> chain.dag; echo "PARENT $S23 CHILD SUMW" >> chain.dag; echo "PARENT SUMW ZX23 CHILD C23" >> chain.dag
fi
if [ $P = 2024 ] || [ $P = all ]; then
  node G24 nextweek 1 2000 prodcheck 2024 --wait
  node S_2024_bkg workday 8 16000 score 2024 bkg; node ZX24 nextweek 4 16000 zx 2024
  node C24K tomorrow 12 24000 card2024 kappa; node C24P tomorrow 12 24000 card2024 plain
  echo "PARENT G24 CHILD S_2024_bkg ZX24" >> chain.dag; echo "PARENT S_2024_bkg ZX24 CHILD C24K C24P" >> chain.dag
fi
if [ $P = all ]; then
  node CBK workday 4 8000 combo kappa; node CBP workday 4 8000 combo plain
  echo "PARENT C23 C24K CHILD CBK" >> chain.dag; echo "PARENT C23 C24P CHILD CBP" >> chain.dag
fi
echo "$D/chain.dag: $(grep -c ^JOB chain.dag) nodes; submit: cd $D && condor_submit_dag chain.dag"
