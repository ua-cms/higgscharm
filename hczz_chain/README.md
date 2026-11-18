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

## End to end: every command, from a fresh checkout to the limits
Everything below is what produced the 2026-11 nominal (2022/23 r < 448.0, model `kf_dd_cjdphi_h128l3_3fs24`). Steps 0–3 and 7
run on lxplus (AlmaLinux 9); steps 4–6 are HTCondor jobs. Paths are the defaults of `config.sh`; override any `HCZZ_*`
with `export` before sourcing it (e.g. a new `HCZZ_RUN` for a fresh run directory).

### 0. Checkout, proxy, environments (once per lxplus session)
```bash
git clone -b HcZZ_full_c26 git@github.com:ua-cms/higgscharm.git && cd higgscharm   # or the existing HcZZ_eossync checkout
export HCZZ_RUN=/eos/user/${USER:0:1}/$USER/Analysis/hczz_chain_run_<tag>     # new run dir (default .../hczz_chain_run)
source hczz_chain/config.sh                                                     # all locations + choices; check them
voms-proxy-init -voms cms -valid 192:00 -out ~/private/x509up_u$(id -u); export X509_USER_PROXY=~/private/x509up_u$(id -u)
export EOS_MGM_URL=root://eosuser.cern.ch; eos quota | grep -A1 project           # scored copies need ~75 GB (2022/23) + ~200 GB (2024) + 11 GB reserve
fs lq ~                                                                          # AFS home must not be full (condor logs go there)
```
Environments used by the stages (set by `run.sh` itself): production `setup.sh` (LCG_110 + rucio), Z+X / Σw / cards
`$HCZZ_LCG_POST` (LCG_107), scoring + training conda env `b_hive_new` (`$HCZZ_CONDA`), limits `$HCZZ_COMBINE_SRC`
(CMSSW_14_1_0_pre4 + HiggsAnalysis/CombinedLimit v10.5.0, after `scram b python`).

### 1. Parquet production (coffea-2026, condor)
```bash
for e in 2022preEE 2022postEE 2023preBPix 2023postBPix 2024; do
  hczz_chain/run.sh produce $e all            # dry run: lists every (workflow, era, dataset) job
  hczz_chain/run.sh produce $e all --submit   # SR (bkg + data, 2024 signal incl. 3FS), sig (2022/23 4FS + 3FS), CR (Z+X), hb (2022/23 H+b)
done                                          # recorded in $HCZZ_RUN/submitted.txt: a rerun submits only what is missing
# training-only ggZZ (CJLST 2023 mcfm701 4e/4mu; MVA training only, never in a card; not part of `produce`):
for e in 2023preBPix 2023postBPix; do for d in GluGlutoContinto2Zto4E_mcfm701 GluGlutoContinto2Zto4Mu_mcfm701; do
  python3 submit_condor.py -w hplusc_mva_4class_ctag2d_final_trainonly -y $e -d $d --nfiles 15 --output_format parquet --jobflavor workday --submit   # writes to $HCZZ_OUTPUTS (exported by config.sh)
done; done
hczz_chain/run.sh prodcheck 2223 2024         # every job wrote its .coffea (add --wait to block until complete)
```
Failed jobs (XRootD, AFS proxy race at a start burst, walltime): resubmit the failed partitions of that dataset with
`submit_condor.py` (same arguments; site-pinned reruns used `--sites`), then rerun `prodcheck`. Outputs:
`$HCZZ_OUTPUTS/<workflow>/<era>/<dataset>/` (`base/` = nominal, one dir per variation).

### 2. MVA training rows (only to retrain; the nominal model is in `models/`)
The row builder has its paths at the top (`REPO`, `O` = production outputs, `BH` = b-hive checkout, `OUT` = rows dir);
edit them for a new run. Run from the conda env `b_hive_new`, as condor jobs (`tomorrow`) for the full extraction:
```bash
source $HCZZ_CONDA; conda activate b_hive_new; cd $OUT_DIR   # = OUT of the script (a copy of hczz_chain/training/build_kfold_3fs24.py lives there)
for e in 2022preEE 2022postEE 2023preBPix 2023postBPix 2024; do python3 -W ignore -u build_kfold_3fs24.py extract $e; done
python3 -W ignore -u build_kfold_3fs24.py compose   # -> b-hive output/DatasetConstructorTask/<cfg>_kfold/hcZZ_kf<k>_3fs24[_test]
```
`extract <era> --limit N` makes a small test set. 2022/23 rows of the nominal model are those of `build_kfold.py` (11-13,
symlinked); only 2024 was re-extracted for `_3fs24`. Fold = md5(chunk name) % 5 (see "Out-of-fold scoring").

### 3. MVA training (b-hive branch `HcZZ-mva-2026-10`, 5 GPU jobs) and registration
```bash
mkdir -p ~/condor_kfold_train && cp hczz_chain/training/train_kfold_h128l3_3fs24.{sh,sub} ~/condor_kfold_train/
cd ~/condor_kfold_train && condor_submit train_kfold_h128l3_3fs24.sub     # k = 0..4: law TrainingTask 40 epochs, MLP 128x3, DisCo 1.42; held-out inference + auc.txt
# after all 5: equal-weight held-out AUCs P(S) vs D_kappa + m4l sculpting (LCG_107)
cd <OUT>; python3 <repo>/hczz_chain/training/heldout_auc_ps_vs_dk.py train_eval_h128l3_3fs24:128x3_3fs24 | tee ps_vs_dk_3fs24.txt
python3 <repo>/hczz_chain/training/register_3fs24.py   # copies best_model.pt x5 + config into models/<name>/, writes the registry entry with md5s
```
For a new model: change the names in the three scripts, register it, set `HCZZ_MODEL=<name>` (or `default:` in
`models/registry.yaml`), commit `models/<name>/`. The scorer checks the md5s at every use.

### 4. Scoring, Z+X, Σw, cards, limits: one DAG
```bash
hczz_chain/make_dag.sh all          # 2223 | 2024 | all -> ~/condor_hczz_chain/$(basename $HCZZ_RUN)/chain.dag
cd ~/condor_hczz_chain/$(basename $HCZZ_RUN) && condor_submit_dag chain.dag
```
`all` = gates (prodcheck --wait) → out-of-fold scoring per era (bkg + sig [+ hb]) → Z+X (merge, estimate, score) → Σw →
`card2223`, `card2024 kappa|plain` → `combo kappa|plain`. Every node gets the settings of the submitting shell (incl.
`HCZZ_ELE_SHIFTS HCZZ_JES_REGROUPED HCZZ_ZZ_KFACTOR_WEIGHTS`, which must be 1 as in the production). Watch with
`condor_q -dag -nobatch`; node logs are next to `chain.dag`.

### 5. The same stages by hand (or to redo one)
```bash
for e in 2022preEE 2022postEE 2023preBPix 2023postBPix; do hczz_chain/run.sh score $e bkg; hczz_chain/run.sh score $e sig; done
hczz_chain/run.sh score 2024 bkg
hczz_chain/run.sh zx 2223; hczz_chain/run.sh zx 2024
hczz_chain/run.sh sumw
hczz_chain/run.sh card2223                     # -> $HCZZ_RUN/cards/<model>_k040_015_045/2223/limits.txt
hczz_chain/run.sh card2024 kappa; hczz_chain/run.sh card2024 plain
hczz_chain/run.sh combo kappa
```
Optional: stitched H+b (`export HCZZ_HB_MODE=stitched`, then `run.sh score <era> hb` and the cards; outputs `_hbst`).

### 6. Recovery
- **Scoring node rc 4/5** (per-file errors, e.g. EOS quota `Errno 122`; `check_scored.py` lists the incomplete datasets):
  free space, then rescore only those datasets: `HCZZ_SCORE_DATASETS=ZZto4L_13 hczz_chain/run.sh score 2022postEE bkg`
  (no skip-existing: always per dataset, never the whole era). Then rerun Σw and the cards.
- **DAG node failed**: fix, then `condor_submit_dag chain.dag` again in the same dir (DAGMan runs the rescue file, done nodes
  are skipped). A node waiting for space can be held by hand (`condor_hold`, released with `condor_release`).
- **Card stage `missing shift dir .../CMS_scale_j_<year>`** or **`no table sumw for (CMS_scale_j_<source>...)`**: the
  builder switches were not exported (step 4); rerun `sumw` and the card with them.

### 7. Results
`$HCZZ_RUN/cards/<model>_<kappa>/{2223,2024_kappa,2024_plain}/limits.txt` (each card dir: `card_b0p1r0p5cat40/`
datacard + histograms + `limit.log`, `layout_b0p1r0p5cat40.json`, optimiser log) and `$HCZZ_RUN/combo/<...>/limits.txt`.
Limits: blind expected AsymptoticLimits (`combine -M AsymptoticLimits -m 120 --run blind --rAbsAcc 1e-5 --rRelAcc 1e-5`).

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
(trained 2026-11-16, nominal + registry default since 2026-11-18, 2022/23 r < 448.0): 5-fold cross-application, 62 inputs, MLP 128×3 (207,620 parameters), DisCo λ 1.42 on ZZ in 105–160 GeV, 2024
signal stitched in training as in the cards (4FS 0c + 3FS ≥1c); held-out equal-weight AUC D_κ 0.773 (2022/23) / 0.775 (2024),
fold spread ≤ 0.004 (`models/kf_dd_cjdphi_h128l3_3fs24/heldout_equal_weight_auc.txt`). `kf_dd_cjdphi_h128l3` = the same
recipe with the plain 4FS 2024 signal. Card discriminant D_κ = P(S) / (P(S) + 0.40 P(qqZZ) + 0.15 P(ggZZ) + 0.45 P(OH))
(`HCZZ_KAPPA`). The training itself runs in b-hive (branch `HcZZ-mva-2026-10`); `training/` has the k-fold row builders
(`build_kfold.py`, `build_kfold_3fs24.py`, rows from the final production), the training jobs (`train_kfold_h128l3[_3fs24].{sh,sub}`),
the held-out inference (`infer_heldout.py`), the AUC/sculpting evaluation (`heldout_auc_ps_vs_dk.py`) and the registration
(`register_3fs24.py`), **as they ran** (absolute paths of the 2026-11 run).

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
- Data dataset names follow the coffea-2026 filesets of this repository (`MuonRun2022E`, ...); the Z+X estimate selects data by name prefix and the cards use Asimov data, so the renaming does not change the chain. The branch `submit_condor.py` (upstream) has no `--eos`; since 2026-11-18 `analysis/utils/path_handler.py` takes the outputs base from `HCZZ_OUTPUTS`, so `run.sh produce` writes where the chain reads. A full production from this branch has not been run yet (the 2026-11 parquets come from the AFS production checkout, validated against this branch on single files, `docs`/Notes 2026-11-13).
