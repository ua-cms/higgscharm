# H+c MVA Analysis Changes

This document describes the changes made to integrate MVA inference for the H+c → ZZ → 4ℓ analysis using b-hive.

## Overview

The MVA integration follows a clean separation of concerns:
- **Processor (`base.py`)**: Unchanged - handles event selection and feature extraction
- **Workflow YAML**: Defines all MVA training features to be saved in parquet output
- **Post-process module**: Runs MVA inference on parquet files using b-hive models

This approach keeps the core processor clean and allows flexible MVA model updates without modifying the processing pipeline.

---

## Files Added

### 1. `analysis/workflows/hplusc_mva_changes.yaml`

New workflow configuration that extends `hplusc.yaml` with comprehensive MVA training features.

**Key additions:**
- Signal dataset (`smsignal`) added to datasets
- Extended histogram axes for MVA features
- MVA configuration section for post-processing

**Features included:**

| Category | Features |
|----------|----------|
| ZZ System | `zz_mass_inclusive`, `zz_pt_inclusive`, `zz_eta_inclusive`, `zz_phi_inclusive`, `deltaR_ZZ` |
| Z1 Boson | `z1_pt`, `z1_eta`, `z1_phi`, `z1_mass` |
| Z2 Boson | `z2_pt`, `z2_eta`, `z2_phi`, `z2_mass` |
| Jets (global) | `jet_multiplicity`, `jet_ht` |
| Jets (per-jet) | `jet_pt`, `jet_eta`, `jet_phi`, `jet_mass` |
| Jet b-tagging | `jet_btagPNetB`, `jet_btagPNetCvL`, `jet_btagPNetCvB`, `jet_btagPNetQvG`, `jet_btagRobustParTAK4B`, `jet_btagRobustParTAK4CvB` |
| Leading jet | `leadingjet_cvsl`, `leadingjet_cvsb`, `leadingjet_b`, `leadingjet_flavour` |
| Subleading jet | `subleadingjet_cvsl`, `subleadingjet_cvsb`, `subleadingjet_flavour` |
| c-jets | `cjets_pt`, `cjets_eta`, `cjets_phi`, `cjets_h_dphi_inclusive` |
| Leptons | `z1_l1_pt/eta/phi`, `z1_l2_pt/eta/phi`, `z2_l1_pt/eta/phi`, `z2_l2_pt/eta/phi` |

---

### 2. `analysis/postprocess/mva_inference.py`

Post-processing module for running MVA inference on parquet files.

**Classes:**
- `MVAPostProcessor`: Main class for MVA inference

**Functions:**
- `run_mva_inference()`: Convenience function for batch processing

**Features:**
- Loads models from b-hive_ttcc (supports `.pt` and `.onnx` formats)
- Configurable via command-line flags for model and config paths
- Mass window cut support (default: 100-150 GeV)
- Adds MVA scores to parquet output

**Output columns added:**
- `mva_signal_score`: Signal class probability
- `mva_class_prediction`: Predicted class index (0-4)
- `mva_score_qqgg_toZZ`: Class 0 probability
- `mva_score_ggH`: Class 1 probability
- `mva_score_Signal`: Class 2 probability
- `mva_score_HPlusB`: Class 3 probability
- `mva_score_Other`: Class 4 probability

---

### 3. `analysis/postprocess/__init__.py`

Updated to export MVA inference functions:
```python
from analysis.postprocess.mva_inference import (
    MVAPostProcessor,
    run_mva_inference,
)
```

---

## MVA Classification

**5-class labels:**
| Index | Class Name | Description |
|-------|------------|-------------|
| 0 | `qqgg_toZZ` | Combined qq→ZZ and gg→ZZ continuum |
| 1 | `ggH` | gg→H→ZZ→4ℓ (Higgs via gluon fusion) |
| 2 | `Signal` | H+c signal (SomeSMSignal) |
| 3 | `HPlusB` | H+b background |
| 4 | `Other` | Other backgrounds (DY, tt, WZ, etc.) |

---

## Usage

### Step 1: Run the processor

```bash
python runner.py -w hplusc_mva_changes -y 2022postEE -o parquet -l /path/to/output
```

This generates parquet files with all MVA training features.

### Step 2: Run MVA inference (post-processing)

**Option A: Python script**
```python
from analysis.postprocess.mva_inference import run_mva_inference

run_mva_inference(
    workflow='hplusc_mva_changes',
    year='2022postEE',
    output_dir='/path/to/output',
    bhive_config_path='/eos/user/s/snandaku/b-hive_ttcc/config/hc_zzto4l_part_training.yml',
    bhive_model_path='/eos/user/s/snandaku/b-hive_ttcc/output/TrainingTask/hc_zzto4l_part_training/hcZZ_allera_80mw/train_11aller384no5mw222/MLP_HcZZ_MW_Deep/epochs_40/nominal/best_model.pt',
)
```

**Option B: Direct usage**
```python
from analysis.postprocess.mva_inference import MVAPostProcessor

processor = MVAPostProcessor(
    bhive_config_path='/eos/user/s/snandaku/b-hive_ttcc/config/hc_zzto4l_part_training.yml',
    bhive_model_path='/eos/user/s/snandaku/b-hive_ttcc/output/TrainingTask/hc_zzto4l_part_training/hcZZ_allera_80mw/train_11aller384no5mw222/MLP_HcZZ_MW_Deep/epochs_40/nominal/best_model.pt',
    apply_mass_window=True,  # Mass window range is read from config
)

# Process single file
df = processor.process_parquet('input.parquet', 'output.parquet')

# Process directory
processor.process_parquets('/path/to/parquets/')
```

---

## b-hive Integration

The MVA inference module integrates with b-hive_ttcc located at:
```
/eos/user/s/snandaku/b-hive_ttcc/
```

**Required files:**
- Config: `config/hc_zzto4l_part_training.yml`
- Model: `output/TrainingTask/hc_zzto4l_part_training/.../best_model.pt`

**Config features used (all read dynamically from config):**
- `global_features`: Event-level features
- `cpf_candidates`: Jet features (per-jet)
- `vtx_features`: Z boson features
- `npf_candidates`: Lepton features
- `lt_candidates`: Higgs 4-vector features
- `n_cpf_candidates`: Number of jets
- `n_vtx_candidates`: Number of Z candidates
- `n_npf_candidates`: Number of leptons
- `n_lt_candidates`: Number of Higgs candidates
- `mass_window`: Min/max for mass window cut

---

## Mass Window

The MVA inference applies a mass window cut by default:
- **Range**: 100 < m4ℓ < 150 GeV
- Events outside the window get `mva_signal_score = -1.0` and `mva_class_prediction = -1`
- Can be disabled with `apply_mass_window=False`

---

## Dependencies

**Required:**
- PyTorch (for `.pt` models) or onnxruntime (for `.onnx` models)
- pandas
- numpy
- PyYAML

**Optional:**
- b-hive_ttcc in Python path (for model architecture loading)

---

## File Structure

```
higgscharm/
├── analysis/
│   ├── postprocess/
│   │   ├── __init__.py          # Updated exports
│   │   ├── mva_inference.py     # NEW: MVA post-processor
│   │   └── ...
│   ├── processors/
│   │   └── base.py              # UNCHANGED
│   └── workflows/
│       ├── hplusc.yaml          # Original workflow
│       ├── hplusc_mva_changes.yaml  # NEW: MVA workflow
│       └── ...
└── docs/
    └── MVA_CHANGES.md           # This file
```

---

## Notes

1. **No changes to `base.py`**: The processor remains untouched. All MVA-specific logic is in the workflow YAML and post-process module.

2. **Flexible model updates**: To use a different model, simply change the `--bhive-model` path. No code changes required.

3. **Feature mapping**: The `prepare_features()` method maps workflow output columns to b-hive config feature names automatically.

4. **Lazy model loading**: The model is only loaded when `predict()` is called, avoiding overhead if inference is not needed.

5. **Dynamic model dimensions**: Model architecture (input_dim, hidden_dim, num_layers, num_classes) is auto-detected from the checkpoint file - no hardcoding required.

6. **Dynamic feature loading**: All feature definitions are read from the b-hive config file at runtime - nothing is hardcoded in the inference module.

---

## Updates (2026-02-28)

### Truth Labels Added to Workflows

Truth labels are now computed directly in the workflow YAML based on dataset names and saved to parquet output.

#### 5-class labels (`hplusc_mva_changes.yaml`)

| Label | Datasets | Description |
|-------|----------|-------------|
| `is_qqgg_toZZ` | `ZZto4L`, `GluGluToContinto2Z*`, `GluGlutoContinto2Z*` | qq→ZZ and gg→ZZ continuum |
| `is_ggH` | `GluGluHtoZZ*` | Gluon fusion Higgs |
| `is_Signal` | `SomeSMSignal`, `HPlusCharm*` | H+c signal |
| `is_HPlusB` | `HPlusBottom*`, `*HB` | H+b background |
| `is_Other` | `VBFHto*`, `ZHto*`, `W*H*`, `TTH_*`, `bbH_*` | Other Higgs production |

**Expression example:**
```yaml
is_Signal:
  expression: ak.ones_like(objects['best_zzcandidate'].p4.pt) * (1.0 if ('SomeSMSignal' in dataset or 'HPlusCharm' in dataset) else 0.0)
```

#### 4-class labels (`hplusc_mva_4class.yaml`)

| Label | Class | Datasets | Description |
|-------|-------|----------|-------------|
| `is_qqZZ` | 0 | `ZZto4L` | qq→ZZ continuum |
| `is_ggZZ` | 1 | `GluGluToContinto2Z*`, `GluGlutoContinto2Z*` | gg→ZZ continuum |
| `is_Signal` | 2 | `SomeSMSignal`, `HPlusCharm*` | H+c signal |
| `is_Other_Higgs` | 3 | `HPlusBottom*`, `*HB`, `GluGluHtoZZ*`, `VBFHto*`, `ZHto*`, `W*H*`, `TTH_*`, `bbH_*` | Combined Higgs backgrounds |

---

### Additional c-jet Features Added

Both `hplusc_mva_changes.yaml` and `hplusc_mva_4class.yaml` now include extended c-jet features to match jet features.

| Feature | Expression | Description |
|---------|------------|-------------|
| `n_cjet` | `ak.num(objects['cjets'])` | c-jet multiplicity |
| `cjet_HT` | `ak.sum(objects['cjets'].pt, axis=1)` | Scalar sum of c-jet pT |
| `cjets_pt` | `objects['cjets'].pt` | c-jet pT (per-jet) |
| `cjets_eta` | `objects['cjets'].eta` | c-jet η (per-jet) |
| `cjets_phi` | `objects['cjets'].phi` | c-jet φ (per-jet) |
| `cjets_mass` | `objects['cjets'].mass` | c-jet mass (per-jet) |
| `cjets_btagPNetB` | `objects['cjets'].btagPNetB` | PNet B-tag score |
| `cjets_btagPNetCvL` | `objects['cjets'].btagPNetCvL` | PNet CvL score |
| `cjets_btagPNetCvB` | `objects['cjets'].btagPNetCvB` | PNet CvB score |
| `cjets_btagPNetQvG` | `objects['cjets'].btagPNetQvG` | PNet QvG score |
| `cjets_h_dphi_inclusive` | `np.abs(objects['zzcandidate_cjet_dphi_inclusive'])` | \|Δφ(leading c-jet, H→4ℓ)\| |

**Note on `cjets_h_dphi_inclusive`:** This is the absolute azimuthal angle difference between the leading c-tagged jet and the Higgs candidate (4-lepton system). Computed via `select_candidate_cjet_dphi()` in `selections/utils.py`. Useful for discrimination since H+c events tend to have the c-jet back-to-back with the Higgs (Δφ ~ π).

---

### Re-running Workflows

After these changes, regenerate parquet files to include the new features:

```bash
# 5-class workflow
python runner.py -w hplusc_mva_changes -y 2022postEE -o parquet

# 4-class workflow
python runner.py -w hplusc_mva_4class -y 2022postEE -o parquet
```

---

### Parquet Writer Fix for Jagged Arrays

**File:** `analysis/utils/parquet_writer.py`

The parquet writer was flattening jagged arrays (e.g., `cjets_pt` with variable number of jets per event) by taking only the first element using `ak.firsts()`. This has been fixed to preserve full jagged arrays.

**Before (line 38-40):**
```python
if array.ndim == 2:
    out[variable] = ak.firsts(array)  # Only kept first element!
```

**After:**
```python
out[variable] = array  # Preserve full jagged array
table = ak.to_arrow_table(ak.Array(out))  # Proper conversion
```

This fix also required updating `utils/dataset/structured_arrays.py` in b-hive to handle the extra dimension when reading 1D features.

---

### Combined Lepton Arrays for MVA

Both workflows now include combined lepton arrays (4 leptons per event) for MVA training:

| Feature | Description |
|---------|-------------|
| `lepton_pt` | Lepton pT [GeV] |
| `lepton_eta` | Lepton η |
| `lepton_phi` | Lepton φ |
| `lepton_mass` | Lepton mass [GeV] |
| `lepton_charge` | Lepton charge |
| `lepton_pfRelIso03_all` | PF relative isolation (ΔR < 0.3) |
| `lepton_sip3d` | Signed 3D impact parameter significance |
| `lepton_is_tight` | Tight lepton ID flag |
| `lepton_mvaHZZIso` | HZZ MVA isolation score |
| `lepton_isPFcand` | Is PF candidate |
| `lepton_highPtId` | High-pT muon ID |

**Expression example:**
```yaml
lepton_pt:
  expression: ak.concatenate([ak.singletons(objects['best_zzcandidate'].z1.l1.pt), ak.singletons(objects['best_zzcandidate'].z1.l2.pt), ak.singletons(objects['best_zzcandidate'].z2.l1.pt), ak.singletons(objects['best_zzcandidate'].z2.l2.pt)], axis=1)
```

**Note on selections:** The `sip3d < 4` cut is already applied during lepton selection (in `is_relaxed` working point), so stored values are in range [0, 4). The feature still provides discriminating power within this range.

---

### Object Selections Update

**File:** `analysis/selections/object_selections.py`

Updated `select_zzto4l_leptons()` to propagate additional lepton fields to ZZ candidate leptons (matching Higgscharmfresh):

**Added placeholder fields:**
```python
# For muons (electrons have mvaHZZIso from NanoAOD)
muons["mvaHZZIso"] = ak.ones_like(muons.pt) * -999

# For electrons (muons have these from NanoAOD)
electrons["isPFcand"] = ak.ones_like(electrons.pt, dtype=bool)
electrons["highPtId"] = ak.zeros_like(electrons.pt, dtype=int)
```

**Fields now available on ZZ candidate leptons:**
- `sip3d` - from NanoAOD (both muons and electrons)
- `pfRelIso03_all` - corrected for FSR photons
- `mvaHZZIso` - from NanoAOD (electrons) or placeholder (muons)
- `isPFcand` - from NanoAOD (muons) or placeholder (electrons)
- `highPtId` - from NanoAOD (muons) or placeholder (electrons)
- `is_tight` - computed during selection

These fields are accessible via `objects['best_zzcandidate'].z1.l1.sip3d` etc.
