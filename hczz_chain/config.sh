# HcZZ chain configuration: every external location and analysis choice of the chain in one place.
# Sourced by hczz_chain/run.sh; any variable can be overridden from the environment (export HCZZ_...=... before running).

HCZZ_REPO=${HCZZ_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}

# --- production (parquets) ---
export HCZZ_OUTPUTS=${HCZZ_OUTPUTS:-/eos/user/s/snandaku/higgscharm/outputs}             # submit_condor.py --eos writes <outputs>/<workflow>/<era>/<dataset>/
HCZZ_WF_SR=${HCZZ_WF_SR:-hplusc_mva_4class_ctag2d_final_bkg}                            # SR: backgrounds + data (+ signal for 2024+)
HCZZ_WF_SIG=${HCZZ_WF_SIG:-hplusc_mva_4class_ctag2d_final_fschad}                      # 2022/23 signal (4FS n_genc10 == 0 + 3FS n_genc10 >= 1)
HCZZ_WF_CR=${HCZZ_WF_CR:-hplusc_mva_4class_CR_merged_final}                            # Z+X control regions (2P2F, 3P1F, SS)
HCZZ_WF_HB=${HCZZ_WF_HB:-hplusc_mva_4class_ctag2d_final_hb}                            # 2022/23 H+b 5FS FxFx + 4FS with n_genb10 (2026-11-16, for HCZZ_HB_MODE=stitched)
HCZZ_NFILES=${HCZZ_NFILES:-15}; HCZZ_JOBFLAVOR=${HCZZ_JOBFLAVOR:-workday}
export HCZZ_CONDOR_DIRS=${HCZZ_CONDOR_DIRS:-$HCZZ_REPO/condor:/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/condor}  # partitions.json of the jobs (Σw table, completeness check)
export X509_USER_PROXY=${X509_USER_PROXY:-$HOME/private/x509up_u178308}

# --- run directory (Σw table, Z+X estimate, scored Z+X, cards, limits) ---
HCZZ_RUN=${HCZZ_RUN:-/eos/user/s/snandaku/Analysis/hczz_chain_run}

# --- MVA (models/registry.yaml) and discriminant ---
HCZZ_MODEL=${HCZZ_MODEL:-kf_dd_cjdphi_h128l3}                                           # registry name; k-fold entries score every event out-of-fold
HCZZ_KAPPA=${HCZZ_KAPPA:-0.40,0.15,0.45}                                               # D = P(S) / (P(S) + k_qq P(qqZZ) + k_gg P(ggZZ) + k_OH P(OH))

# --- Z+X: fake rates of pkatris (opposite-sign method, 2 |eta| bins: e 1.479 / mu 1.2), read in place ---
HCZZ_FR_DIR=${HCZZ_FR_DIR:-/eos/user/p/pkatris/higgscharm/outputs/fake_rates_nocjet/OS}  # <dir>/<era>/fake_rates.pkl; 2022/23 per era (--year all reads all four), 2024 own
HCZZ_ZX_MASS=${HCZZ_ZX_MASS:-"105 160"}

# --- cards ---
HCZZ_ERAS_2223=${HCZZ_ERAS_2223:-"2022preEE 2022postEE 2023preBPix 2023postBPix"}
HCZZ_SIGMA_4FS_FB=97.62; HCZZ_SIGMA_3FS_FB=55.0; HCZZ_SIGMA_TOTAL_FB=73.30; HCZZ_PDF_CH=1.065   # FS-stitched H+c -> ZZ* signal (Heyen FS envelope), pdf_cH lnN
HCZZ_OPT=${HCZZ_OPT:-"--km 16 --ks 8 --budgets 20,30,40,50 --bmin 0.1 --relmax 0.5"}       # 2D (D, m4l) bin optimiser; the 40-bin layout is the nominal card
HCZZ_SIGNAL_2024=${HCZZ_SIGNAL_2024:-stitched}   # 2024 signal: stitched (4FS 0c + 3FS >=1c, 3FS samples since 2026-11-16) or 4fs
HCZZ_HB_MODE=${HCZZ_HB_MODE:-pooled}   # H+b background: pooled (old 5FS samples of the SR production at the fileset xsec) or stitched (5FS n_genb10 == 0 + 4FS >= 1, K = NNLO/NLO, sum = bbH NNLO 574.8 fb; combine/scripts/hb_stitch.py; 2024 5FS only); stitched cards/combos get the suffix _hbst
HCZZ_ALLOW_MISSING_SYST_2024=${HCZZ_ALLOW_MISSING_SYST_2024:-scalevar_muR_bbH,scalevar_muF_bbH}

# --- environments ---
HCZZ_LCG_PROD=${HCZZ_LCG_PROD:-/cvmfs/sft.cern.ch/lcg/views/LCG_110/x86_64-el9-gcc13-opt/setup.sh}   # coffea-2026 production (setup.sh)
HCZZ_LCG_POST=${HCZZ_LCG_POST:-/cvmfs/sft.cern.ch/lcg/views/LCG_107/x86_64-el9-gcc13-opt/setup.sh}   # Z+X, Σw, card building
HCZZ_CONDA=${HCZZ_CONDA:-/eos/user/s/snandaku/miniforge3/etc/profile.d/conda.sh}; HCZZ_CONDA_ENV=${HCZZ_CONDA_ENV:-b_hive_new}  # MVA scoring (torch)
HCZZ_COMBINE_SRC=${HCZZ_COMBINE_SRC:-/eos/user/s/snandaku/Analysis/combine/CMSSW_14_1_0_pre4/src}   # CMSSW area with HiggsAnalysis/CombinedLimit (scram b python done)
