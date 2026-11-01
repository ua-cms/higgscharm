#!/bin/bash
hostname; date; source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc13-opt/setup.sh >/dev/null 2>&1
cd /eos/user/s/snandaku/Higgscharmnew/higgscharm
python3 -u /eos/user/s/snandaku/Analysis/mva_opt_2026_10_30/perm_importance.py /eos/user/s/snandaku/b-hive_ttcc/config/hc_zzto4l_mw_training_4class_fsextv2_disco1.yml /eos/user/s/snandaku/b-hive_ttcc/output/TrainingTask/hc_zzto4l_mw_training_4class_fsextv2_disco1/hcZZ_fs_fsextv2disco1_20261028/train_fs_fsextv2disco1_20261028/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal/best_model.pt /eos/user/s/snandaku/Analysis/mva_opt_2026_10_30/importance_fsextv2disco1.json --nrep 3 2>&1 | tee /eos/user/s/snandaku/Analysis/mva_opt_2026_10_30/importance_fsextv2disco1.log; date
