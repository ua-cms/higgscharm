# 2026-10-27: mass-decorrelated H+c MVAs (user, after Gerrit's comment: correlated vs decorrelated, with systematics)

Motivation: the no-m4l MVA still sculpts (m4l is recoverable from the lepton pt/eta/phi inputs to 0.1 GeV for 94% of
ZZ events), see ../nom4l_card_2026_10_27/README.md. Reference limits (fixed scorer, full systematics):
prod (m4l input) 1D 426 / 2D 440; no-m4l 90-160 1D 521 / 2D 467.

## Code (b-hive_ttcc, uncommitted; backups *.bak_20261027_before_disco)
- `utils/torch/mass_decorrelation.py` (new): config keys `aux_m4l`, `mass_scaled_features`, `disco`; input layout;
  weighted distance correlation (reference DisCo implementation, arXiv:2001.05310; unit-tested: independent 0.0007,
  linear 1.0, weight 2 == duplicated events exactly).
- `tasks/dataset.py`: passes `aux_m4l` to the processor (only when set).
- `utils/coffea_processors/lz4_hczz_processor.py`: `aux_m4l` keeps the auxiliary m4l as the LAST lt column.
- `utils/torch/LZ4Dataset.py`: strips that column (checks stored = inputs + 1), divides `mass_scaled_features` by it,
  yields m4l in the `mass_cat` slot; refuses aux_m4l together with in_mass_window.
- `utils/models/base_model.py`: `disco_term()`: loss += lambda * dCorr^2(P(Signal), m4l) over qqZZ+ggZZ events of the
  batch, in training and validation (best-model selection on the total loss); prints the average term per epoch.
- Analysis scorer `mva_inference.prepare_features` (all 3 checkouts): divides `mass_scaled_features` by m4l
  (zz_mass_inclusive / Z+X m4l). validate_scoring.py: alignment undoes the scaling (allclose) for those columns;
  reference switched to `_scored_v1_jetfix` (scored_v1 deleted, user).
- Smoke test (config `hc_zzto4l_mw_training_4class_decortest`, 8 files, 2 CPU epochs, massinv + DisCo 10): dataset
  105 columns = 98 inputs + m4l + meta; training runs, DisCo term 0.3-0.6; scorer vs training inputs (scaled)
  185 events ALL 98 COLUMNS MATCH (check_train.py, aux/scaling aware).

## Models (configs in b-hive config/, = nom4l 90-160 + keys)
| tag | config | change |
| massinv | hc_zzto4l_mw_training_4class_massinv | cjet_HT, jet_ht, jet_pt, lepton_pt, z_pt, pT_4l divided by m4l |
| disco2/10/50 | hc_zzto4l_mw_training_4class_nom4l_disco{2,10,50} | DisCo lambda 2/10/50 on qqZZ+ggZZ |
Training: condor 16525602 (H100; dataset hcZZ_big4class_<tag>_20261027, training train_loose_ymassbal2_<tag>_20261027,
same flags as nom4l). Chains: DAGs 16525612 (massinv), 16525613 (disco2), 16525614 (disco10), 16525615 (disco50):
wait -> 8 scoring + Z+X -> pipeline.sh (validation, sculpting, 1D optbin10/S10, 2D hand) ->
`../nom4l_card_2026_10_27/<tag>/limits.txt`, sculpting in `<tag>/sculpting/`. events.npz removed at the end (space).

## Found 10-27 (pre-existing, affects ALL cards incl. 429): Z+X MVA jet inputs
The Z+X CR parquet (zx_background_estimate_CRmerged_jecv4_105160_2026_10_22) has only LEADING-jet scalars (jet_pt,
jet_eta, jet_phi, btag...), no per-jet arrays, and run_zx_inference.py does not pass jet_phi through. So for Z+X:
jet_phi input = 0 always; jets 2-3 get copies of the leading jet (scalar column, idx ignored). 34% of the Z+X yield
has >= 1 jet, 18% >= 2 jets. Fix = regenerate the Z+X CR with per-jet arrays (_0/_1/_2) + pass jet_phi; user decision.
Kept as is for now so all comparisons are like for like.

## RESULTS (10-28 03:25) — expected r, full systematics; sculpting = qqZZ top-20%-score shape / inclusive, 105-160, max/min
| model | 1D optbin10 | 1D S10 | 2D hand 9x4 | AUC S/all, S/qqZZ, S/OH | sculpting max/min |
| prod (m4l input)  | 427 | 426 | 440 | 0.851 0.959 0.833 | ~4300 (peak) |
| nom4l 90-160      | 523 | 521 | 467 (opt2D 444.5) | 0.824 0.828 0.823 | 4.9 (hump x1.7 at 120-125) |
| massinv           | 626 | 629 | 481 | 0.790 0.726 0.800 | 1.9 (slope 1.13 -> 0.71) |
| disco2            | 816 | 789 | 575 | 0.713 0.585 0.733 | 1.8 |
| disco10           | 902 | 811 | 595 | 0.638 0.539 0.653 | 1.3 |
| disco50           | 849 | 699 | 553 | 0.589 0.518 0.600 | 1.5 |
Training val. DisCo term (lambda x dCorr^2): disco2 0.037, disco10 0.226, disco50 0.446; disco50 val. acc 38% (vs 57-60).
DisCo scores bunch up (S10 edges 0.129-0.133, 0.508-0.511): the S/ZZ separation in 105-160 is mostly m4l-correlated,
so removing the correlation removes the separation (AUC S/qqZZ 0.83 -> 0.59 already at lambda 2). DisCo trainings not
tuned (lambda/lr scan, no warm start); validate_performance FAILED for disco10/50 (report only; training-like eval reads
the aux-m4l npz, not checked further).
Conclusion: mass-correlated is best in 1D (426); decorrelation costs +48% (massinv) to +65-90% (DisCo) in 1D. In 2D
(m4l explicit) the cost shrinks: nom4l 467 (444.5 optimised), massinv 481 vs prod 440.
