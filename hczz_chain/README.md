# hczz_chain: H+c → ZZ* → 4ℓ, from parquets to limits

The complete HcZZ analysis chain as it produced the 2026-11 results, runnable from this repository.
`config.sh` holds every external location and choice; `run.sh <stage>` runs one stage; `make_dag.sh` writes the HTCondor DAG
of everything after the production. Each stage reproduces the command of the 2026-11 chain
(`/afs/cern.ch/user/s/snandaku/condor_hczz_2026_11_09_final/*_final.sh`, `condor_hczz_2026_11_16_kf128/`) with the tools in
`tools/` and the inputs in `inputs/`.

```
produce ──> prodcheck ──┬─> score (per era, bkg + sig) ──> sumw ──┐
 (parquets)  (all jobs) │                                         ├─> card2223 ──┐
                        └─> zx (merge CR -> estimate -> score) ───┘              ├─> combo (2022/23 + 2024)
                        ├─> score 2024 ───────────────────────────┬─> card2024 ──┘
                        └─> zx 2024 ──────────────────────────────┘   (kappa, plain)
```

## Running
```bash
cd <repo>; source hczz_chain/config.sh             # optional: inspect / override (export HCZZ_RUN=..., HCZZ_MODEL=...)
voms-proxy-init -voms cms -valid 192:00 -out ~/private/x509up_u$(id -u)
hczz_chain/run.sh produce 2022preEE all            # dry run: lists the jobs
hczz_chain/run.sh produce 2022preEE all --submit   # submits (repeat per era; already-submitted datasets are skipped)
hczz_chain/make_dag.sh all && cd ~/condor_hczz_chain/$(basename $HCZZ_RUN) && condor_submit_dag chain.dag
```
Every stage can also be run by hand, e.g. `hczz_chain/run.sh card2024 kappa`. Limits are appended to
`$HCZZ_RUN/cards/<model>_<kappa>/{2223,2024_kappa,2024_plain}/limits.txt` and `$HCZZ_RUN/combo/<...>/limits.txt`.

## Stages
| stage | what | main code | output |
|---|---|---|---|
| `produce <era> [sr\|sig\|cr\|all]` | coffea-2026 processor → parquets with every variation (JES regrouped, JER, lepton scale/res, ...). MC datasets = the final production (`datasets.yaml`, from its submission record); data = every data key of the era fileset | `submit_condor.py`, workflows `hplusc_mva_4class_ctag2d_final_bkg` (SR), `..._final_fschad` (2022/23 stitched signal), `hplusc_mva_4class_CR_merged_final` (Z+X CRs) | `$HCZZ_OUTPUTS/<workflow>/<era>/<dataset>/` |
| `prodcheck <eras> [--wait]` | every job wrote its `.coffea` (from `partitions.json`, reruns of failed jobs count) | `tools/prodcheck.py` | gate |
| `score <era> <bkg\|sig>` | MVA scores of every event; k-fold models score each parquet chunk with the model that did not train on it (fold = md5(chunk name) % 5, enforced, see "Model"); then `tools/check_scored.py` requires a complete scored copy (same rows + score columns) of every raw parquet, else the node fails (rc 5; rc 4 = per-file scoring errors, e.g. EOS quota). `HCZZ_SCORE_DATASETS=<prefixes>` rescoring only some datasets after such a failure | `analysis/postprocess/run_mva_postprocess.py --model-name`, `kfold.py`, `mva_registry.py`, `tools/check_scored.py` | `<workflow>_scored_<model>/` |
| `sumw` | pre-jet-veto generated Σw per sample and variation (card normalisation) | `tools/make_sumw_table.py` | `$HCZZ_RUN/sumw/sumw_table.json` |
| `zx <2223\|2024>` | Z+X: merge the CR parquets per jet, estimate 2P2F + 3P1F in 105–160 GeV with the **pkatris opposite-sign fake rates** (`$HCZZ_FR_DIR/<era>/fake_rates.pkl`, read in place), jet-kinematics check, MVA-score the Z+X events (row index % 5 for k-fold) | `scripts/merge_cr_parquets.py`, `scripts/estimate_zx_background.py`, `tools/check_zx_jets.py`, `scripts/run_zx_inference.py --model-name` | `$HCZZ_RUN/zx_estimate/`, `$HCZZ_RUN/zx_mva_<model>/` |
| `card2223` | 2022/23 card: event dump → 2D (D_κ, m4l) bin optimiser (40 bins) → datacard with all systematics, FS-stitched signal (4FS 0c + 3FS ≥1c, σ 73.30 fb), Z+X → blind expected limit | `tools/dump_events_2d.py`, `tools/optimise_2d.py`, `tools/create_datacards_unroll_2d.py` → `combine/scripts/create_datacards_ctag2d_100150_stitched_decorr.py` | `cards/.../2223/card_b0p1r0p5cat40/`, `limits.txt` |
| `card2024 <kappa\|plain> [stitched\|4fs]` | 2024 card (UParT categories), D_κ or P(S); signal FS-stitched like 2022/23 (4FS HPlusCharm* n_genc10 == 0 + 3FS `Stitch3FS_cH_*_2024` n_genc10 ≥ 1, default since 2026-11-16, `HCZZ_SIGNAL_2024`) or the plain 4FS alone (`4fs`, the earlier 2024 cards) | `tools/*_2024.py` → `combine/scripts/create_datacards_2024.py` | `cards/.../2024_<kind>/` |
| `score <era> hb` + cards with `HCZZ_HB_MODE=stitched` | H+b background as the LHCHWG-2026-007 flavour-scheme stitch (as the signal): 5FS FxFx n_genb10 == 0 (σ_NLO 753.7 fb, K 574.8/753.7) + 4FS n_genb10 ≥ 1 (397.6 fb, K 519.6/397.6), pre-veto genb sumw, sum rescaled to the bbH NNLO (MiNNLOPS 5FS) 574.8 fb; own scale/PDF weights (5FS transferred); 2024: 5FS alone (no 4FS sample). Default `pooled` = the old H+b at the fileset xsec | `combine/scripts/hb_stitch.py`, workflow `hplusc_mva_4class_ctag2d_final_hb` | `<workflow>_scored_<model>/`, cards/combos with suffix `_hbst`, `hb_stitch_summary.json` |
| `combo <kappa\|plain>` | 2022/23 + 2024 combined limit, 2024 experimental nuisances renamed (decorrelated) | `tools/decorrelate_2024.py`, `combineCards.py` | `combo/.../limits.txt` |

Limits: `combine -M AsymptoticLimits -m 120 --run blind --rAbsAcc 1e-5 --rRelAcc 1e-5` in `$HCZZ_COMBINE_SRC` (separate CMSSW shell).

## Model
`$HCZZ_MODEL` is a name in `models/registry.yaml` (md5-checked at every use). Default `kf_dd_cjdphi_h128l3_3fs24`
(2026-11-16): 5-fold cross-application, 62 inputs, MLP 128×3 (207,620 parameters), DisCo λ 1.42 on ZZ in 105–160 GeV, 2024
signal stitched in training as in the cards (4FS 0c + 3FS ≥1c); held-out equal-weight AUC D_κ 0.773 (2022/23) / 0.775 (2024),
fold spread ≤ 0.004 (`models/kf_dd_cjdphi_h128l3_3fs24/heldout_equal_weight_auc.txt`). `kf_dd_cjdphi_h128l3` = the same
recipe with the plain 4FS 2024 signal. Card discriminant D_κ = P(S) / (P(S) + 0.40 P(qqZZ) + 0.15 P(ggZZ) + 0.45 P(OH))
(`HCZZ_KAPPA`). The training itself runs in b-hive (branch `HcZZ-mva-2026-10`); `training/` has the k-fold row builders
(`build_kfold.py`, `build_kfold_3fs24.py`, rows from the final production) and the training job **as they ran** (absolute
paths of the 2026-11 run).

### Out-of-fold scoring (k-fold models)
An event may only be scored by a model that did not see it in training:
1. **Fold of an event** = `int(md5(parquet chunk file name)) % 5`. The chunk name (input-file UUID + entry range) is the same
   in the nominal and in every variation directory, so an event has one fold everywhere. The training-row builders and the
   scorer use the same function (`chunk_fold`, `training/build_kfold*.py` = `analysis/postprocess/kfold.py`).
2. **Training**: model `kf<k>` is trained on the rows of folds ≠ k and evaluated on fold k
   (`hcZZ_kf<k>[_3fs24]` / `_test`). Rows come from exactly the parquets the cards score: `final_bkg` (background, 2024
   signal incl. the 2024 3FS) and `final_fschad` (2022/23 signal). The 5FS H+b chunks of `final_hb` have the same names as in
   `final_bkg` (checked 2026-11-17), so they get the same fold. Training-only MC (`final_trainonly`, CJLST 2023 mcfm701 ggZZ)
   is never scored.
3. **Scoring**: `KFoldPostProcessor` loads the 5 models and scores each parquet chunk with model `chunk_fold(chunk)`. It
   refuses to run unless model k's path contains `kf<k>`, so a mis-ordered list cannot score an event with a model that
   trained on it. Data and MC follow the same rule. Z+X parquets are not chunk-named: there the fold is the row index % 5
   (Z+X is not in the training).
4. **Use**: pass `--model-name kf_dd_cjdphi_h128l3_3fs24` (or `HCZZ_MODEL`); the registry sets the 5 models (`kfold_models`),
   do not pass `--kfold-models` as well.

## Inputs (`inputs/`)
- `preveto_genc_4fs.json`: pre-jet-veto GEN-c fractions of the 4FS signal (FS stitching, Analysis/fs_stitch_direct_2026_10_07)
- `gg_scale_new4fs/gg_<era>.json`: 4FS μR/μF scale σ per c-hadron class from our rebuilt 4FS H→ZZ (Analysis/fs_stitch_new4fs_2026_10_13)
- `new4fs_pooled.json`: the same, pooled, for 2024
External (by path in `config.sh`): fake rates (pkatris), the combine CMSSW area, the production outputs.

## Notes
- The chain scripts take the outputs base from `HCZZ_OUTPUTS` (also read by `scripts/merge_cr_parquets.py`, `estimate_zx_background.py`, `tools/make_sumw_table.py`).
- 2024 Z+X needs the UParTAK4 jet scores in the Z+X inference (fixed 2026-11-16 in `scripts/run_zx_inference.py`; before, every 2024 Z+X inference failed).
- Data dataset names follow the coffea-2026 filesets of this repository (`MuonRun2022E`, ...); the Z+X estimate selects data by name prefix and the cards use Asimov data, so the renaming does not change the chain. A full production from this branch has not been run yet (the 2026-11 parquets come from the AFS production checkout, validated against this branch on single files, `docs`/Notes 2026-11-13).
