# MVA Models

This directory stores trained MVA model checkpoints used for post-processing inference in the H+c → ZZ → 4ℓ analysis.

## Model format

Models are saved as PyTorch `.pt` checkpoint files produced by the [b-hive](https://github.com/deoache/b-hive) training framework. Each checkpoint encodes the model architecture parameters (`input_dim`, `hidden_dim`, `num_layers`, `num_classes`) so no separate architecture config is needed at inference time.

## Available models

| File | Architecture | Classes | Description |
|------|-------------|---------|-------------|
| `best_model.pt` | MLP_HcZZ_MW_Deep | 4 | 4-class classifier: qqZZ, ggZZ, Signal, Other_Higgs |

## Running MVA inference

After running the standard postprocessing step (`--postprocess`), run MVA inference with:

```bash
python3 run_postprocess.py \
  --workflow hplusc_mva_4class \
  --year <year> \
  --output_format parquet \
  --mva-inference \
  --mva-config /path/to/config.yml \
  --mva-model /path/to/best_model.pt \
  --mva-output /eos/user/<u>/<username>/higgscharm/outputs/hplusc_mvascores/<year>
```

To also apply a mass window cut (100 < m4l < 150 GeV) before inference, add `--mva-mass-window`:

```bash
python3 run_postprocess.py \
  --workflow hplusc_mva_4class \
  --year <year> \
  --output_format parquet \
  --mva-inference \
  --mva-mass-window \
  --mva-config /path/to/config.yml \
  --mva-model /path/to/best_model.pt \
  --mva-output /eos/user/<u>/<username>/higgscharm/outputs/hplusc_mvascores/<year>
```

You can also combine `--postprocess` and `--mva-inference` in a single command to run both steps together:

```bash
python3 run_postprocess.py \
  --workflow hplusc_mva_4class \
  --year <year> \
  --postprocess \
  --output_format parquet \
  --mva-inference \
  --mva-mass-window \
  --mva-config /path/to/config.yml \
  --mva-model /path/to/best_model.pt
```

## CLI flags

| Flag | Description |
|------|-------------|
| `--mva-inference` | Enable MVA inference step |
| `--mva-config` | Path to b-hive config YAML (required with `--mva-inference`) |
| `--mva-model` | Path to trained `.pt` model file (required with `--mva-inference`) |
| `--mva-output` | Output directory for scored parquets and histograms (default: `outputs/<workflow>_mvascores/<year>`) |
| `--mva-mass-window` | Apply 100 < m4l < 150 GeV mass window before inference |

## Outputs

For each sample, the inference step produces:

- **Scored parquets** saved under `<mva-output>/<sample>/base/` — original parquet columns plus:
  - `mva_score_<class>` — softmax score for each class (e.g. `mva_score_Signal`, `mva_score_qqZZ`, ...)
  - `mva_signal_score` — signal class score
  - `mva_class_prediction` — predicted class index (argmax)
- **Score histograms** saved as `<mva-output>/<sample>_mva_scores.coffea` — one `hist.Hist` per score column with axes `[score, process, variation]`

## Config

The b-hive config YAML defines the feature list used at training time. The same config must be used at inference to ensure the feature vector matches the model's `input_dim`. Configs are stored in the b-hive repository under `config/`.
