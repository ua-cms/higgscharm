# 2026-10-27: HcZZ 2022/23 with the SM MVA WITHOUT m4l (user: production MVA sculpts m4l, see ../mass_sculpting_2026_10_27)

Model: b-hive `hc_zzto4l_mw_training_4class_nom4l` / `hcZZ_big4class_nom4l_20261019` / `train_loose_ymassbal2_nom4l_20261019`
(SM 4-class, 98 inputs = production minus m4l, in_mass_window; m4l in [90,160) still a selection; 2D c-tag categories;
40 epochs, lr 5e-4, trained 10-19 for the CuH22 study, see SMEFT/cuh22_smmva_20261019/retrain_nom4l/README.md).
User decision 10-27: use this model (no new training).

Plan: (1) score card inputs, (2) sculpting check, (3) 1D card + limit vs 429, (4) 2D m4l x score card + limit.

## Scoring
- Z+X CR: `zx_background_mva_CRmerged_jecv4_105160_nom4l_2026_10_27` (scripts/run_zx_inference.py, LCG_105, input
  `zx_background_estimate_CRmerged_jecv4_105160_2026_10_22`, era per file); event counts as the 10-22 scoring. Done.
- Backgrounds `_jecshifts_bkg_scored_nom4l`, signal `_fschad_scored_nom4l` (nominal + shift dirs): condor 16525420
  (8 jobs, era x bkg/sig), `~/condor_hczz_2026_10_27_score_nom4l/score.sh` (= scored_v1 options, new model/config).

## Second model (user, 10-27): no m4l input, trained on the card window 105 <= m4l < 160
- Config `b-hive_ttcc/config/hc_zzto4l_mw_training_4class_nom4l_105160.yml` (= nom4l, selection m4l>=105, mass_window.min 105).
- Condor 16525422 (H100): dataset `hcZZ_big4class_nom4l_105160_20261027` (filelist hplusc_mva_4class_train.txt) + training
  `train_loose_ymassbal2_nom4l_105160_20261027` (same flags as 10-19). `~/condor_hczz_2026_10_27_train_nom4l_105160/`.
- Then: score the same inputs, same card chain, compare with the 90-160 no-m4l model and 429.

## 2D card
`create_datacards_unroll_m4l.py`: runs the stitched_decorr builder with eos_io.read_parquet patched so mva_score_Signal ->
(i_m4l + s)/N (all reads: MC, shifts, signal, Z+X); --bin-edges in the unrolled variable (`--print-edges M4L SCORE`).

## Submitted 10-27 ~17:20 (session shut down after this; everything runs on condor)
- Model 90-160 (`nom4l9016`): scoring 16525420 (sig 2022preEE/2023postBPix done, 126 files = scored_v1); card job 16525424
  (`~/condor_hczz_2026_10_27_card_nom4l/job.sh`, waits for 8x "exit 0" in the score outs, then pipeline.sh) -> `nom4l9016/`:
  `sculpting/` (m4l plots with the new score), `card_optbin10old`, `card_S10`, `card_2D`, `limits.txt`.
- Model 105-160 (`nom4l105160`): training 16525422; DAG 16525425 `~/condor_hczz_2026_10_27_chain_nom4l_105160/chain.dag`:
  WAIT (training out "law exit 0" + model) -> 8 scoring (`_scored_nom4l105160`) + Z+X (`zx_..._nom4l105160_2026_10_27`) -> CARD
  (same job.sh/pipeline) -> `nom4l105160/limits.txt`.
- Reference: 429.0 (production MVA with m4l, optbin10, v4v3full).

## FINDING 10-27 ~17:55: scorer jet inputs != training jet inputs (affects the production 429 card too)
- `analysis/postprocess/mva_inference.py::prepare_features` reads `cjets_pt/eta/phi/mass` and `cjets_btagPNet*` BEFORE
  `jet_*`; b-hive trained on `jet_*` (all jets; dataset npz fields jet_pt...). `cjets` = c-tagged subset, so when they
  differ the scorer fills other jets. Example ZZto4L_10 2022postEE ev 17: jets [62.7, 27.0], cjets [27.0] -> scorer jet0 = 27.0.
- Training file vs its dataset npz (88 events): 91/98 inputs identical, mismatch in jet_pt/eta/phi[0,1] and jet_is_L0[1].
- Card inputs (2022postEE qqZZ + ggH, 105-160): 2879 / 32226 = 8.9% events with first-3 cjets != first-3 jets.
- Training-like check (30 files): scorer accuracy 0.56 vs recorded val acc 0.64 (partly this, partly sample size).
- Not fixed (user decision pending). Running jobs left as is: same scorer as 429, so comparisons are like for like.
  Pipeline now runs validate_scoring.py (blocking) + validate_performance.py (report only) before the cards.
- Proposed fix: prefer the column names the config uses (jet_*), then rescore production + nom4l and rebuild cards.

## FIX 10-27 evening (user: "fix the scorer bug")
- `mva_inference.prepare_features`: jet pt/eta/phi/mass and PNet B/CvB/CvL now read `jet_*` first, `cjets_*` only as
  fallback (with a logged warning if jet_pt/eta/phi are absent). UParT (2024/25) path unchanged (already jet_*).
- Applied identically in EOS `Higgscharmnew/higgscharm` (condor scorer), AFS `Higgscharmnew/higgscharm`, and
  `Higgscharm2026/HcZZ_eossync` (HcZZ_full). Not committed.
- Validation vs the b-hive nom4l training npz (`check_train.py`, scratch copy below): 5 files (ZZto4L 2022postEE [the
  file that was 91/98], H+b 2022postEE, H+c 2023postBPix, ZZto4L + ggH 2023preBPix): 4560 events, ALL 98 columns identical.
- Jobs already running (16525424, DAG 16525425) imported the OLD scorer: their scores/cards are like-for-like with 429,
  not the fixed version. Still to do: rescore production (scored_v1 -> 429 card) + nom4l + nom4l105160, rebuild cards.

## Rescoring with the fixed scorer, submitted 10-27 19:10 (`~/condor_hczz_2026_10_27_rescore_jetfix/`)
- Generic scripts: `models.sh <prod|nom4l9016|nom4l105160>` (config/model/dataset/output dirs), `score.sh`, `zx.sh`
  (both refuse to run if the scorer lacks the fix), `card.sh` (= pipeline.sh with tag `<model>_jetfix`).
- `prod` (production MVA, models/best_model.pt + models/ config; b-hive copy of that config differs, not used):
  DAG 16525497 -> `_scored_v1_jetfix`, Z+X `zx_..._v1_jetfix_2026_10_27`, cards in `prod_jetfix/`
  (card_optbin10old = the 429 card definition -> compare directly with 429.0).
- `nom4l9016`: DAG 16525498 -> `_scored_nom4l_jetfix`, `nom4l9016_jetfix/` (compare 527/525 old scorer).
- `nom4l105160`: NOT submitted (EOS 90.5%, ~95 GB free; ~25 GB per model). `condor_submit_dag chain_nom4l105160.dag`
  once space is freed or the old-scorer nom4l105160 card is done and its scored dirs can go.
- The production model npz check is not possible: its training parquets were since overwritten; its training dataset uses
  jet_* (checked), and the code path is the one validated 98/98 above.

## 10-27 ~19:25: old-scorer outputs deleted (user)
- Old DAG 16525425 (nom4l105160, old scorer) removed before its card; no old-scorer 105-160 limit.
- `eos rm -r` (recyclable, pxids in session log): `_jecshifts_bkg_scored_nom4l`, `_fschad_scored_nom4l`,
  `_jecshifts_bkg_scored_nom4l105160`, `_fschad_scored_nom4l105160`, `zx_..._nom4l_2026_10_27`, `zx_..._nom4l105160_2026_10_27`,
  and `nom4l9016/events.npz`. Kept: nom4l9016 cards/limits (old-scorer record: optbin10old 527, S10 525, 2D 469),
  production `_scored_v1` (429 reference + validate_scoring REF).
- EOS 90.5% -> 87.2%; then `chain_nom4l105160.dag` submitted (see below).
- nom4l105160 rescoring: DAG 16525518 -> `nom4l105160_jetfix/`.
- 10-27: fix also validated vs the 105-160 training npz: 4 files (ZZ, H+c, ggH, H+b), 4345 events, ALL 98 columns identical.

## Sculpting with the fixed scorer (10-27 ~22:40; plots in <tag>_jetfix/sculpting/)
qqZZ in 105-160, shape(score > cut) / shape(all), 5 GeV bins 105..160:
  prod        s>0.5: 0.0 0.1 1.6 5.7 3.6 0.0 0.0 0.0 0.0 0.0 0.0   (narrow peak, RMS in 115-135 2.1-3.0 GeV)
  nom4l9016   s>0.5: 0.7 1.0 1.6 1.9 1.7 1.2 1.0 0.8 0.6 0.4 0.3   (no peak: RMS 5.4-5.7 ~ flat 5.8; broad hump)
  nom4l105160 s>0.5: 1.1 1.0 1.4 1.9 1.7 1.3 1.0 0.8 0.5 0.4 0.3
No-m4l: no 125 peak, but a broad x1.7-1.9 hump at 120-130 and x0.3 at 155-160 (max/min 6-7 for s > 0.5-0.7).
Likely cause: the lepton pt/eta/phi (4 x 3), Z and 4l-system inputs determine m4l (massless leptons), so the network
can learn an approximate m4l. In a 2D m4l x score template fit this correlation is modelled from MC (not a bias by
itself); for a 1D score fit or a score cut + m4l fit it is. Decorrelation (e.g. pT/m4l inputs, DisCo/adversarial,
m4l-flat bkg reweighting) would be needed to remove it.

## RESULTS with the fixed scorer (10-27, chains done 22:50) — expected r (50%)
| model | 1D optbin10 (429 edges) | 1D S10 re-opt | 2D hand m4l(9) x score(4) |
| prod (with m4l) | 427.0 (old 429) | 426.0 | 440.0 |
| nom4l 90-160 | 523.0 (old 527) | 521.0 (old 525) | 467.0 (old 469) |
| nom4l 105-160 | 537.5 | 533.0 | 469.5 |
Next: joint 2D optimisation (Analysis/opt2d_2026_10_27), then unbinned.
- CONFIRMED (10-27): m4l is recoverable from the MVA lepton inputs: m4l built from z{1,2}_l{1,2} pt/eta/phi (massless)
  = zz_mass_inclusive within 0.1 GeV for 94% of qqZZ events in 105-160 (592 ev, 15 files 2022postEE; rest ~FSR).
  So "no m4l" removes the column, not the information -> broad sculpting. A sculpting-free model needs mass-invariant
  lepton inputs (pT/m4l, 4l rest-frame angles, Z masses) or a decorrelation term (DisCo/adversarial) — user decision.
