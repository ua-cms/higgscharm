# n2_noid_dd_both: final HcZZ MVA (candidate, frozen after the 2022/23 limit)

Four-class MLP (`MLP_HcZZ_MW_Deep_4class`, classes qqZZ, ggZZ, Signal = H+c, Other_Higgs) trained with b-hive on 2022, 2023 and 2024 together; one model for 2022-2026 (2025/2026 use the 2024 settings).

| | |
|---|---|
| Files | `best_model.pt` (md5 `8c6cd12a…`), `hc_zzto4l_mw_training_4class_fsextv2_comb2224_noid_nomult_dd_both.yml` (md5 `ae9fc9fd…`); checked by `analysis/postprocess/mva_registry.py` |
| Inputs (63) | event: jet H_T, eta_4l, dR(Z1,Z2), dphi(c-jet, H), dphi(jet 1, 4l), deta(c-jet, 4l), decay angles cos theta*, cos theta1, cos theta2, phi, phi1, is_2024; per jet (3): pT and the 2D c-tag category one-hot (L0, C0-C4, B0-B4; PNet 2022/23, UParTAK4 2024+); Z pT (2); per lepton (4): pT, relIso, SIP3D; pT_4l, m4l (lepton and Z pT scaled by m4l) |
| Not used | lepton ID flags, jet multiplicity, c-jet H_T, deta(jet 1, 4l) (dropped 2026-11-07 to 11-13: no gain or m4l sculpting) |
| Training | dataset hcZZ_comb2224 (2022/23 + 2024, is_2024 flag), 40 epochs, lr 5e-4, batch 4096, class weighting, DisCo lambda 1.42 between the score and m4l for qqZZ+ggZZ in 105 < m4l < 160 GeV |
| Test AUC S vs qqZZ / ggZZ / Other_Higgs | 2022/23 0.7523 / 0.7918 / 0.8675; 2024 0.7327 / 0.7645 / 0.8449 (`eval.txt`) |
| m4l sculpting | ZZ f(115-135) after the 20% ZZ-efficiency score cut / inclusive: 1.11 (2022/23), 1.03 (2024); no narrow 125 GeV peak |
| Parquet input | workflow `hplusc_hczz_final` stores every column the scorer reads (36: 30 kinematic + B/CvB/CvL of the era's tagger) |

Scoring: `python3 analysis/postprocess/run_mva_postprocess.py -i <outputs/hplusc_hczz_final> -o <scored dir> --model-name n2_noid_dd_both --era <era> --no-mass-window --no-histograms` (b-hive environment not needed for this flat MLP).
