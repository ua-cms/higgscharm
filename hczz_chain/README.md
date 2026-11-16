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
hczz_chain/make_dag.sh all && cd $HCZZ_RUN/dag && condor_submit_dag chain.dag
```
Every stage can also be run by hand, e.g. `hczz_chain/run.sh card2024 kappa`. Limits are appended to
`$HCZZ_RUN/cards/<model>_<kappa>/{2223,2024_kappa,2024_plain}/limits.txt` and `$HCZZ_RUN/combo/<...>/limits.txt`.

## Stages
| stage | what | main code | output |
|---|---|---|---|
| `produce <era> [sr\|sig\|cr\|all]` | coffea-2026 processor → parquets with every variation (JES regrouped, JER, lepton scale/res, ...). MC datasets = the final production (`datasets.yaml`, from its submission record); data = every data key of the era fileset | `submit_condor.py`, workflows `hplusc_mva_4class_ctag2d_final_bkg` (SR), `..._final_fschad` (2022/23 stitched signal), `hplusc_mva_4class_CR_merged_final` (Z+X CRs) | `$HCZZ_OUTPUTS/<workflow>/<era>/<dataset>/` |
| `prodcheck <eras> [--wait]` | every job wrote its `.coffea` (from `partitions.json`, reruns of failed jobs count) | `tools/prodcheck.py` | gate |
| `score <era> <bkg\|sig>` | MVA scores of every event; k-fold models score each parquet chunk with the model that did not train on it (fold = md5(chunk name) % 5, enforced) | `analysis/postprocess/run_mva_postprocess.py --model-name`, `kfold.py`, `mva_registry.py` | `<workflow>_scored_<model>/` |
| `sumw` | pre-jet-veto generated Σw per sample and variation (card normalisation) | `tools/make_sumw_table.py` | `$HCZZ_RUN/sumw/sumw_table.json` |
| `zx <2223\|2024>` | Z+X: merge the CR parquets per jet, estimate 2P2F + 3P1F in 105–160 GeV with the **pkatris opposite-sign fake rates** (`$HCZZ_FR_DIR/<era>/fake_rates.pkl`, read in place), jet-kinematics check, MVA-score the Z+X events (row index % 5 for k-fold) | `scripts/merge_cr_parquets.py`, `scripts/estimate_zx_background.py`, `tools/check_zx_jets.py`, `scripts/run_zx_inference.py --model-name` | `$HCZZ_RUN/zx_estimate/`, `$HCZZ_RUN/zx_mva_<model>/` |
| `card2223` | 2022/23 card: event dump → 2D (D_κ, m4l) bin optimiser (40 bins) → datacard with all systematics, FS-stitched signal (4FS 0c + 3FS ≥1c, σ 73.30 fb), Z+X → blind expected limit | `tools/dump_events_2d.py`, `tools/optimise_2d.py`, `tools/create_datacards_unroll_2d.py` → `combine/scripts/create_datacards_ctag2d_100150_stitched_decorr.py` | `cards/.../2223/card_b0p1r0p5cat40/`, `limits.txt` |
| `card2024 <kappa\|plain>` | 2024 card (4FS signal, UParT categories), D_κ or P(S) | `tools/*_2024.py` → `combine/scripts/create_datacards_2024.py` | `cards/.../2024_<kind>/` |
| `combo <kappa\|plain>` | 2022/23 + 2024 combined limit, 2024 experimental nuisances renamed (decorrelated) | `tools/decorrelate_2024.py`, `combineCards.py` | `combo/.../limits.txt` |

Limits: `combine -M AsymptoticLimits -m 120 --run blind --rAbsAcc 1e-5 --rRelAcc 1e-5` in `$HCZZ_COMBINE_SRC` (separate CMSSW shell).

## Model
`$HCZZ_MODEL` is a name in `models/registry.yaml` (md5-checked at every use). Default `kf_dd_cjdphi_h128l3`: 5-fold
cross-application, 62 inputs, MLP 128×3 (207,620 parameters), DisCo λ 1.42 on ZZ in 105–160 GeV; card discriminant
D_κ = P(S) / (P(S) + 0.40 P(qqZZ) + 0.15 P(ggZZ) + 0.45 P(OH)) (`HCZZ_KAPPA`). The training itself runs in b-hive;
`training/` has the k-fold row builder (`build_kfold.py`, rows from the final production) and the training job **as they ran**
(absolute paths of the 2026-11 run).

## Inputs (`inputs/`)
- `preveto_genc_4fs.json`: pre-jet-veto GEN-c fractions of the 4FS signal (FS stitching, Analysis/fs_stitch_direct_2026_10_07)
- `gg_scale_new4fs/gg_<era>.json`: 4FS μR/μF scale σ per c-hadron class from our rebuilt 4FS H→ZZ (Analysis/fs_stitch_new4fs_2026_10_13)
- `new4fs_pooled.json`: the same, pooled, for 2024
External (by path in `config.sh`): fake rates (pkatris), the combine CMSSW area, the production outputs.

## Notes
- The chain scripts take the outputs base from `HCZZ_OUTPUTS` (also read by `scripts/merge_cr_parquets.py`, `estimate_zx_background.py`, `tools/make_sumw_table.py`).
- 2024 Z+X needs the UParTAK4 jet scores in the Z+X inference (fixed 2026-11-16 in `scripts/run_zx_inference.py`; before, every 2024 Z+X inference failed).
- Data dataset names follow the coffea-2026 filesets of this repository (`MuonRun2022E`, ...); the Z+X estimate selects data by name prefix and the cards use Asimov data, so the renaming does not change the chain. A full production from this branch has not been run yet (the 2026-11 parquets come from the AFS production checkout, validated against this branch on single files, `docs`/Notes 2026-11-13).
