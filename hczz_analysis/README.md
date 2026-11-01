# hczz_analysis: scripts behind the 2026-10 HcZZ results

Snapshot (2026-11-01) of the analysis-level scripts that produced the current HcZZ cards and
studies. They ran from outside this repository and are kept here **as they ran**: paths are
hard-coded to the original locations, so to re-run, either run them in place or adjust the paths.
Scripts only (`.py .sh .sub .dag .md .C`, files < 1 MB, no `.bak*`, no outputs or card dirs).

| here | original location |
|---|---|
| `analysis/<folder>/` | `/eos/user/s/snandaku/Analysis/<folder>/` |
| `condor/<folder>/` | `/afs/cern.ch/user/s/snandaku/<folder>/` (HTCondor submit dirs) |

Selection = every folder reachable by path references from the 10-30 lepss chain
(`condor_hczz_2026_10_30_lepss`, `lepss_reprocess_2026_10_30`), the MVA optimisation
(`condor_hczz_2026_10_30_mvaopt`, `mva_opt_2026_10_30`) and the fsext training
(`condor_hczz_2026_10_28_fsext`), plus the 2024 card chain.
Training code itself lives in b-hive: `Saranya-Nandakumar/b-hive_MVA`, branch `HcZZ-mva-2026-10` (04fe24d).

## What produced what
| result | entry point |
|---|---|
| **Nominal r < 448.0** (prod 1D optbin10, lepss) | `condor/condor_hczz_2026_10_30_lepss/lepss.dag` -> `card.sh` -> `analysis/lepss_reprocess_2026_10_30/pipeline_lepss.sh` (step 1: `analysis/unbinned_mva_2026_10_18/dump_events.py` with the optbin10 edges, limit via `analysis/fsrw_2026_10_02/run_combine.sh`) |
| Z+X 35.7 ± 4.5 (lepss) | `condor/condor_hczz_2026_10_30_lepss/zxbuild.sh`, `zx.sh` |
| Scoring of prod / massinvlep / fsextv2 | `condor/condor_hczz_2026_10_30_lepss/score.sh`, models in `models.sh` |
| fsextv2 / massinvlep 2D cat40, unbinned DSCB, channel split | `condor/condor_hczz_2026_10_30_lepss/twod.sh`; `analysis/opt2d_2026_10_27/`, `analysis/unbinned2d_2026_10_30/` |
| optbin10 edges | `analysis/optbin_2026_10_21/optimise_binning.py` |
| m4l sculpting checks | `analysis/mass_sculpting_2026_10_27/` |
| sumw table | `analysis/sumw_fix_2026_10_02/`, `condor/condor_hczz_2026_10_30_lepss/sumw.sh` |
| FS stitching inputs (pre-veto GEN-c, gg scale jsons) | `analysis/fs_stitch_direct_2026_10_07/`, `analysis/fs_stitch_new4fs_2026_10_13/`, `analysis/fs_stitch_gg_2026_10_02/` |
| Mass decorrelation / new-input trainings | `condor/condor_hczz_2026_10_27_train_decor/`, `condor/condor_hczz_2026_10_28_fsext/train_fs.sh`; studies in `analysis/decorrelation_2026_10_27/`, `analysis/mva_newinputs_2026_10_28/` |
| MVA optimisation (κ 456 -> 446, network scan, feature screens) | `condor/condor_hczz_2026_10_30_mvaopt/` (`kcard.sh` uses `HCZZ_KAPPA`, see `combine/scripts/eos_io.py`), `analysis/mva_opt_2026_10_30/` |
| 2024 card (r 270.0) | `analysis/builder_2024_2026_10_25/`, `analysis/hczz_2024_prod_upart_2026_10_25/` |
| V4/V3 card (429) / no-m4l cards | `analysis/v4v3_card_2026_10_23/`, `analysis/nom4l_card_2026_10_27/` (validate_scoring/performance, unroll_m4l builder used by the lepss pipeline) |

Builders called by these scripts are in `combine/scripts/` of this repository (byte-identical to the
production checkout `/eos/user/s/snandaku/Higgscharmnew/higgscharm/combine/scripts`).
