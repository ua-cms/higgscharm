# model definitions for the jetfix rescoring; sets CFGM TDIR DSET BKG SIG ZX
B=/eos/user/s/snandaku/b-hive_ttcc; O=/eos/user/s/snandaku/higgscharm/outputs; A=/eos/user/s/snandaku/Analysis
case $1 in
 prod) CFGM=/eos/user/s/snandaku/Higgscharmnew/higgscharm/models/hc_zzto4l_mw_training_4class_nomass.yml
       TDIR=$B/output/TrainingTask/hc_zzto4l_mw_training_4class_nomass/hcZZ_big4classjetyesmasslooseoldcbal/train_loose_oldcwithmassbal/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal
       DSET=$B/output/DatasetConstructorTask/hc_zzto4l_mw_training_4class_nomass/hcZZ_big4classjetyesmasslooseoldcbal
       S=v1_jetfix ;;
 nom4l9016) CFGM=$B/config/hc_zzto4l_mw_training_4class_nom4l.yml
       TDIR=$B/output/TrainingTask/hc_zzto4l_mw_training_4class_nom4l/hcZZ_big4class_nom4l_20261019/train_loose_ymassbal2_nom4l_20261019/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal
       DSET=$B/output/DatasetConstructorTask/hc_zzto4l_mw_training_4class_nom4l/hcZZ_big4class_nom4l_20261019
       S=nom4l_jetfix ;;
 nom4l105160) CFGM=$B/config/hc_zzto4l_mw_training_4class_nom4l_105160.yml
       TDIR=$B/output/TrainingTask/hc_zzto4l_mw_training_4class_nom4l_105160/hcZZ_big4class_nom4l_105160_20261027/train_loose_ymassbal2_nom4l_105160_20261027/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal
       DSET=$B/output/DatasetConstructorTask/hc_zzto4l_mw_training_4class_nom4l_105160/hcZZ_big4class_nom4l_105160_20261027
       S=nom4l105160_jetfix ;;
 massinv|disco2|disco10|disco50|massinvdisco2|massinvlep|massinvlepdisco0p1|massinvlepdisco0p5|massinvlepdisco2|massinvlepdisco10)   # 2026-10-27 mass-decorrelated trainings (condor 16525602); 10-28 massinvdisco2 = massinv inputs + DisCo 2; 10-28 pm massinvlepdisco* = massinvlep + DisCo lambda 0.1/0.5/2/10
       if [ $1 = massinv ]; then C=hc_zzto4l_mw_training_4class_massinv; elif [ $1 = massinvdisco2 ]; then C=hc_zzto4l_mw_training_4class_massinv_disco2; elif [ $1 = massinvlep ]; then C=hc_zzto4l_mw_training_4class_massinvlep; elif [[ $1 = massinvlepdisco* ]]; then C=hc_zzto4l_mw_training_4class_massinvlep_disco${1#massinvlepdisco}; else C=hc_zzto4l_mw_training_4class_nom4l_$1; fi
       CFGM=$B/config/$C.yml
       TDIR=$B/output/TrainingTask/$C/hcZZ_big4class_$1_20261027/train_loose_ymassbal2_$1_20261027/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal
       DSET=$B/output/DatasetConstructorTask/$C/hcZZ_big4class_$1_20261027
       S=$1 ;;
 fsext|fsextdisco0p1|fsextdisco0p5|fsextv2disco*|fsextmdisco*)   # 2026-10-28 eve: new inputs (77) + FS-stitched signal (4FS 0c + 3FS >=1c); fsextdisco0p5 = chosen (DisCo 0.5), trained on lxplus-gpu
       # 10-29: fsextv2disco0p5 (73, jet-4l pT features dropped) / fsextmdisco0p5 (77, mass-normalised ptbalm/ptfracm): m4l leak fixed, lxplus-gpu
       C=hc_zzto4l_mw_training_4class_${1/disco/_disco}
       CFGM=$B/config/$C.yml
       TDIR=$B/output/TrainingTask/$C/hcZZ_fs_$1_20261028/train_fs_$1_20261028/MLP_HcZZ_MW_Deep_4class/epochs_40/nominal
       DSET=$B/output/DatasetConstructorTask/$C/hcZZ_fs_$1_20261028
       S=$1 ;;
 *) echo "unknown model $1"; exit 2 ;;
esac
BKG=$O/hplusc_mva_4class_ctag2d_lepss_bkg_scored_$S; SIG=$O/hplusc_mva_4class_ctag2d_lepss_fschad_scored_$S   # 2026-10-30 lepss reprocessing
ZX=$A/zx_background_mva_CRmerged_lepss_105160_${S}_2026_10_30
export CFGM TDIR DSET; export HCZZ_ELE_SHIFTS=1   # builder: CMS_scale_e/CMS_res_e dirs exist in the lepss production
