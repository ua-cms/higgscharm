# H+c MVA Analysis: End-to-End Guide

Complete workflow from NanoAOD processing to MVA inference.

---

## Setup for a New User

### 1. Clone the repositories

```bash
# higgscharm (analysis framework)
git clone <higgscharm-repo-url> ~/higgscharm

# b-hive (MVA training framework)
git clone <b-hive-repo-url> /eos/home-<initial>/<username>/HToWW/b-hive
```

### 2. Update user-specific paths

There are **3 hardcoded paths** that must be changed for a new user:

| File | Line | What to change |
|------|------|----------------|
| `higgscharm/run_postprocess.py` | 32 | `OUTPUT_DIR = Path("/eos/home-c/cgupta/higgscharm/outputs")` → your EOS outputs path |
| `higgscharm/run_postprocess.py` | 130 | `default="/eos/home-c/cgupta/HToWW/b-hive"` → your b-hive repo path |
| `b-hive/train_MVA.sh` | 16 | `FILELIST="/eos/user/c/cgupta/higgscharm/outputs/..."` → your filelist path |

**Note:** The processing step (`runner.py`) auto-detects your EOS path from `$USER`, so no changes needed there. The `--eos` flag writes outputs to `/eos/user/<initial>/<username>/higgscharm/outputs/`.

### 3. Environment

- **Processing (higgscharm):** Runs on HTCondor via Singularity (`coffeateam/coffea-dask:latest-py3.9`). No local env needed for submission.
- **Postprocessing (higgscharm):** Needs `coffea`, `pandas`, `pyarrow`, `pyyaml`. Works with Python 3.9+.
- **Training (b-hive):** Needs `torch`, `law`, `scipy`, `numpy`. Requires Python 3.10+ (uses `match/case` syntax).
- **Inference (`--infer` flag):** Needs `torch` + b-hive importable. Requires Python 3.10+.

### 4. Set `B_HIVE_DIR` environment variable

b-hive's `ConfigLoader` needs this to find config files:

```bash
export B_HIVE_DIR=/eos/home-<initial>/<username>/HToWW/b-hive
```

---

## Step 1: Processing (higgscharm)

Run the coffea processor on NanoAOD files via HTCondor. Produces per-sample coffea + parquet outputs.

```bash
cd ~/higgscharm   # your higgscharm directory

# Initialize grid proxy (required for xrootd access to NanoAOD)
voms-proxy-init --voms cms

# Submit all datasets for hww_MVA workflow (2022postEE)
python3 runner.py -w hww_MVA -y 2022postEE --submit --eos --output_format parquet

# Or submit a single dataset
python3 submit_condor.py -w hww_MVA --dataset TTto2L2Nu -y 2022postEE --submit --eos --nfiles 15
```

**Monitor jobs:**
```bash
watch condor_q
python3 jobs_status.py -w hww_MVA -y 2022postEE --eos
```

**Outputs:**
```
/eos/user/<initial>/<username>/higgscharm/outputs/hww_MVA/2022postEE/
  <sample_name>_<partition>/base/*.coffea    # per-partition coffea histograms
  <sample_name>_<partition>/base/*.parquet   # per-partition parquet files
```

---

## Step 2: Postprocessing with MVA Labels (higgscharm)

Merge per-partition outputs into per-process files, add one-hot truth labels for MVA training, and generate filelists for b-hive.

```bash
cd ~/higgscharm

python3 run_postprocess.py -w hww_MVA -y 2022postEE \
  --postprocess --output_format parquet --mva
```

**What `--mva` does:**
- Adds one-hot truth columns: `is_higgs`, `is_tt`, `is_st`, `is_diboson`, `is_vjets`
- Adds `weight` column (from `weight_nominal`)
- Generates filelists at `<output_dir>/filelists/`

**Outputs:**
```
<OUTPUT_DIR>/hww_MVA/2022postEE/
  <process>.parquet              # per-process parquets with truth labels (tt.parquet, H+c.parquet, ...)
  <process>.coffea               # per-process coffea histograms
  filelists/base.txt             # filelist pointing to process parquets (input for b-hive)
  2022postEE_processed_histograms.coffea
  base/cutflow_base_*.csv        # cutflow tables
```

**Parquet columns** (per-process, e.g. `tt.parquet`):
- 17 MVA features: `dilepton_pt`, `lepton1_pt`, `lepton2_pt`, `cjet_cand_pt`, `met_pt`, `mtl1`, `mtl2`, `dilepton_mass`, `delta_R_ll_l1`, `delta_R_ll_l2`, `delta_R_ll_c`, `delta_phi_l1PlusMET_c`, `delta_phi_l1_MET`, `delta_phi_l2_MET`, `cjet_cand_cvsl_pnet`, `cjet_cand_cvsb_pnet`, `nSV`
- Weights: `weight_nominal`, `weight_genweight`, systematics (`weight_*Up/Down`)
- Truth labels: `is_higgs`, `is_tt`, `is_st`, `is_diboson`, `is_vjets`
- Training weight: `weight`

---

## Step 3: Training (b-hive)

Train the multi-class MLP using b-hive's law-based pipeline. The filelist from Step 2 points b-hive to the process parquets.

```bash
cd <your-b-hive-directory>

# Full pipeline (dataset construction + training + inference + ROC curves)
./train_MVA.sh \
  --version hww_multiclass_v7 \
  --filelist <OUTPUT_DIR>/hww_MVA/2022postEE/filelists/base.txt \
  --epochs 50
```

Or run individual steps:

```bash
# 3a. Dataset construction (converts parquets to b-hive's internal format)
law run DatasetConstructorTask \
  --config HPlusCHToWW_multiclass \
  --filelist <OUTPUT_DIR>/hww_MVA/2022postEE/filelists/base.txt \
  --dataset-version hww_multiclass_v7 \
  --coffea-worker 4 \
  --chunk-size 5000000

# 3b. Training
law run TrainingTask \
  --config HPlusCHToWW_multiclass \
  --filelist <OUTPUT_DIR>/hww_MVA/2022postEE/filelists/base.txt \
  --dataset-version hww_multiclass_v7 \
  --training-version hww_multiclass_v7 \
  --model-name SimpleMLP_MultiClass \
  --epochs 50 \
  --batch-size 1024 \
  --learning-rate 1e-3

# 3c. ROC curves
law run ROCCurveTask \
  --config HPlusCHToWW_multiclass \
  --filelist <OUTPUT_DIR>/hww_MVA/2022postEE/filelists/base.txt \
  --dataset-version hww_multiclass_v7 \
  --training-version hww_multiclass_v7 \
  --model-name SimpleMLP_MultiClass \
  --epochs 50 \
  --batch-size 1024 \
  --learning-rate 1e-3 \
  --test-dataset-version hww_multiclass_v7 \
  --test-filelist <OUTPUT_DIR>/hww_MVA/2022postEE/filelists/base.txt
```

**Model architecture** (`SimpleMLP_MultiClass`):
- Input: 17 global features
- Hidden layers: [128, 64, 32] with BatchNorm + ReLU + Dropout(0.2)
- Output: 5 classes (higgs, tt, st, diboson, vjets) with softmax
- Classes derived from `config/HPlusCHToWW_multiclass.yml` `truths:` field

**Outputs:**
```
<b-hive>/output/TrainingTask/HPlusCHToWW_multiclass/<version>/<version>/SimpleMLP_MultiClass/epochs_50/nominal/
  best_model.pt                  # best checkpoint (used for inference in Step 4)
  training_loss.npy              # loss curves

<b-hive>/output/ROCCurveTask/...
  roc_curves.pdf                 # signal vs background ROC curves
```

---

## Step 4: Inference on Analysis Parquets (higgscharm)

Run the trained model on the postprocessed parquets. Adds `mva_score_*` columns and saves copies to `mva/` subdirectory. **Originals are untouched.**

```bash
cd ~/higgscharm

python3 run_postprocess.py -w hww_MVA -y 2022postEE \
  --output_format parquet \
  --infer \
  --model-path <b-hive>/output/TrainingTask/HPlusCHToWW_multiclass/<version>/<version>/SimpleMLP_MultiClass/epochs_50/nominal/best_model.pt
```

**Flags:**
| Flag | Default | Description |
|------|---------|-------------|
| `--infer` | - | Enable inference mode |
| `--model-path` | (required) | Path to `best_model.pt` from Step 3 |
| `--bhive-path` | (set in argparse) | Path to your b-hive repo root |
| `--bhive-config` | `HPlusCHToWW_multiclass` | b-hive config name |
| `--bhive-model-name` | `SimpleMLP_MultiClass` | Model class name |
| `--output_format` | `coffea` | **Must be `parquet`** when using `--infer` |

**You can combine Steps 2 and 4 in one command:**
```bash
python3 run_postprocess.py -w hww_MVA -y 2022postEE \
  --postprocess --output_format parquet --mva \
  --infer --model-path <path-to-best_model.pt>
```

**Outputs:**
```
<OUTPUT_DIR>/hww_MVA/2022postEE/
  tt.parquet                     # original (untouched)
  mva/
    tt.parquet                   # copy with 5 extra score columns
    H+c.parquet
    DY+Jets.parquet
    ...
```

**Added columns** (in `mva/*.parquet`):
- `mva_score_higgs` — P(signal)
- `mva_score_tt` — P(ttbar)
- `mva_score_st` — P(single top)
- `mva_score_diboson` — P(diboson)
- `mva_score_vjets` — P(V+jets)
- Scores sum to 1.0 per event

---

## Verification

```python
import pandas as pd

OUTPUT_DIR = "/eos/user/<initial>/<username>/higgscharm/outputs"

# Check scores exist and sum to 1
df = pd.read_parquet(f'{OUTPUT_DIR}/hww_MVA/2022postEE/mva/tt.parquet')
print(df.filter(like='mva_score').head())
print(df.filter(like='mva_score').sum(axis=1).describe())

# Signal should have highest mva_score_higgs
for proc in ['H+c', 'tt', 'DY+Jets']:
    df = pd.read_parquet(f'{OUTPUT_DIR}/hww_MVA/2022postEE/mva/{proc}.parquet')
    print(f"{proc}: mean higgs score = {df['mva_score_higgs'].mean():.4f}")
```

---

## Configuration: Adding/Changing Classes

Truth labels are defined in two places (must stay in sync):

1. **higgscharm** `analysis/postprocess/utils.py`:
   - `PROCESS_GROUPS` list: defines class names and their order
   - `PROCESS_TO_GROUP` dict: maps physics process names (from filesets) to group names

2. **b-hive** `config/HPlusCHToWW_multiclass.yml`:
   - `truths:` list: `is_<group>` column names (order must match `PROCESS_GROUPS`)
   - `reference_flavour:` signal class for ROC curves (e.g. `is_higgs`)

Everything else derives automatically:
- `PROCESS_GROUP_IDS` is computed from `PROCESS_GROUPS`
- `SimpleMLP_MultiClass.classes` is built from config `truths` in `__init__`
- `signal_class` comes from config `reference_flavour`
- Inference reads class names from `model.classes`

---

## Quick Reference

| Step | Directory | Command |
|------|-----------|---------|
| Process | higgscharm | `python3 runner.py -w hww_MVA -y 2022postEE --submit --eos --output_format parquet` |
| Monitor | higgscharm | `python3 jobs_status.py -w hww_MVA -y 2022postEE --eos` |
| Postprocess | higgscharm | `python3 run_postprocess.py -w hww_MVA -y 2022postEE --postprocess --output_format parquet --mva` |
| Train | b-hive | `./train_MVA.sh --version <version> --filelist <OUTPUT_DIR>/.../filelists/base.txt --epochs 50` |
| Infer | higgscharm | `python3 run_postprocess.py -w hww_MVA -y 2022postEE --output_format parquet --infer --model-path <path-to-best_model.pt>` |
