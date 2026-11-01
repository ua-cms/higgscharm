# 2026-10-27: m4l sculpting of the 2022/23 SM H+c MVA (qqZZ, ggZZ)

Question: does the production H+c MVA (`train_loose_oldcwithmassbal`, models/best_model.pt md5 fd19031f,
`mva_score_Signal`) sculpt a 125 GeV peak in qqZZ/ggZZ, as seen for CuH22 in ../../SMEFT/cuh22_smmva_20261019?

Inputs: `outputs/hplusc_mva_4class_ctag2d_jecshifts_bkg_scored_v1`, 4 eras 2022/23 (61.9/fb), `base` only,
normalised as the datacard builder (bookkeeping.py, sumw table v4v3_card_2026_10_23). All selected events
are scored (any m4l). Check: totals = bookkeeping final_2022_2023/yields_all.csv (qqZZ 570.094 / window 38.381,
ggZZ 163.897 / 5.797), identical.

- `collect.py` -> events_{qqZZ,ggZZ}.npz; `plot.py` -> plots + summary.md
- m4l_shape_*: unit-normalised m4l for score > 0, 0.1, 0.3, 0.5, 0.7 (wide 70-300, zoom 100-170, lin/log)
- eff_vs_m4l_*: fraction passing each score cut per m4l bin; mean_score_vs_m4l_*; score_vs_m4l_2d_*

Result: YES, strong sculpting. The score cut turns the flat ZZ continuum into a peak at ~125 GeV
(width ~ a few GeV, edge at ~131, low tail to ~110). Mean qqZZ score: 0.59 at 125 GeV, < 0.01 above 140 GeV.
Fraction of the events passing the cut that lie in 115-135 GeV: qqZZ 0.81 / 0.94 / 0.99 / 0.997 for score > 0.1/0.3/0.5/0.7
(ggZZ 0.84 / 0.95 / 0.98 / 0.994), vs 0.025 (qqZZ) / 0.011 (ggZZ) without a cut.
Cause: m4l is an MVA input (global_features and lt_candidates in hc_zzto4l_mw_training_4class_nomass.yml;
"nomass" does not mean no m4l). `in_mass_window` is [90,160) = the training range, constant 1 there.
Training range is 90-160: scores outside it are extrapolation (not used in the [105,160) card).
| process | score > | raw | yield | 70-105 | 105-115 | 115-135 | 135-160 | >160 |
|---|---|---|---|---|---|---|---|---|
| qqZZ | 0 | 806857 | 570.094 | 0.123 | 0.009 | 0.025 | 0.034 | 0.810 |
| qqZZ | 0.1 | 17892 | 12.431 | 0.012 | 0.181 | 0.805 | 0.001 | 0.000 |
| qqZZ | 0.3 | 10169 | 7.006 | 0.000 | 0.058 | 0.942 | 0.000 | 0.000 |
| qqZZ | 0.5 | 5900 | 4.041 | 0.000 | 0.010 | 0.990 | 0.000 | 0.000 |
| qqZZ | 0.7 | 2333 | 1.623 | 0.000 | 0.003 | 0.997 | 0.000 | 0.000 |
| ggZZ | 0 | 880493 | 163.897 | 0.010 | 0.004 | 0.011 | 0.021 | 0.955 |
| ggZZ | 0.1 | 7539 | 1.447 | 0.013 | 0.147 | 0.839 | 0.001 | 0.000 |
| ggZZ | 0.3 | 4240 | 0.847 | 0.000 | 0.050 | 0.950 | 0.000 | 0.000 |
| ggZZ | 0.5 | 2319 | 0.488 | 0.000 | 0.018 | 0.982 | 0.000 | 0.000 |
| ggZZ | 0.7 | 769 | 0.171 | 0.000 | 0.006 | 0.994 | 0.000 | 0.000 |

## 115-135 GeV window (plot_window115135.py, for discussion with Gerrit)
m4l_115to135_scorecuts_{qqZZ,ggZZ,ZZ}.png/.pdf: m4l in [115,135), 1 GeV bins, unit area, p_sig > 0/0.5/0.7/0.8/0.9.
Flat without a cut; peaked at ~124-125 with a cut, RMS 5.7 -> 3.0 (0.5) -> 2.2 (0.8) -> 1.2 GeV (0.9).
Numbers: summary_115to135.md. Also an_style_mass_sculpting_zz_* (AN format, 90-160) and z1_mass_sculpting_zz_*.

## ZZ MVA score (plot_zzscore.py)
m4l_{90to160,115to135}_zzscorecuts_{qqZZ,ggZZ,ZZ}: cuts on p_ZZ = mva_score_qqZZ + mva_score_ggZZ.
Mirror image of the H+c score: a hole at 122-128 GeV (empty for p_ZZ > 0.5), Z->4l peak at 91 enhanced.
Fraction in 115-135 (90-160 range, ZZ): 0.18 -> 0.09 (0.5) -> 0.035 (0.9). Table: summary_zzscore.md.

## qqZZ MVA score alone (plot_qqzzscore.py)
m4l_{90to160,115to135}_qqzzscorecuts_qqZZ[_log]: qqZZ only, cuts on mva_score_qqZZ > 0/0.5/0.7/0.8/0.9.
Score > 0.5 empties ~118-130 GeV (hole around 125) and boosts the Z->4l peak at 91. Fraction of the 90-160
yield in 115-135: 0.171 -> 0.024 (0.5) -> 0.003 (0.7) -> 0.000 (0.9). Table: summary_qqzzscore.md.
