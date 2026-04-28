"""
Post-process MVA Inference module for H+c -> ZZ -> 4l analysis.

Runs MVA inference on parquet files using b-hive model and config.
All feature definitions and class names are read from the config - nothing is hardcoded.

Config should contain:
    - class_names (or labels/classes): List of class names, e.g. ['qqZZ', 'ggZZ', 'Signal', 'Other_Higgs']
    - mass_window: {min: 100, max: 150}
    - global_features, cpf_candidates, vtx_features, etc. for feature definitions

Usage:
    from analysis.postprocess.mva_inference import MVAPostProcessor

    mva_processor = MVAPostProcessor(
        bhive_config_path='/eos/user/s/snandaku/b-hive_ttcc/config/hc_zzto4l_4class.yml',
        bhive_model_path='/eos/user/s/snandaku/b-hive_ttcc/output/.../best_model.pt',
    )
    mva_processor.process_parquets(input_dir)
"""

import os
import sys
import yaml
import glob
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional, Dict

logging.basicConfig(level=logging.INFO)

# Try to import PyTorch
try:
    import torch
    import torch.nn as nn
    HAS_PYTORCH = True
except ImportError:
    HAS_PYTORCH = False

# Try to import ONNX
try:
    import onnxruntime as ort
    HAS_ONNX = True
except ImportError:
    HAS_ONNX = False


# =============================================================================
# MLP Model Definition (matches b-hive MLP_HcZZ_MW_Deep)
# =============================================================================
if HAS_PYTORCH:
    class ResidualBlock(nn.Module):
        def __init__(self, dim, dropout=0.3):
            super().__init__()
            self.norm = nn.LayerNorm(dim)
            self.fc1 = nn.Linear(dim, dim * 2)
            self.fc2 = nn.Linear(dim * 2, dim)
            self.dropout = nn.Dropout(dropout)
            self.act = nn.GELU()

        def forward(self, x):
            residual = x
            x = self.norm(x)
            x = self.fc1(x)
            x = self.act(x)
            x = self.dropout(x)
            x = self.fc2(x)
            x = self.dropout(x)
            return x + residual

    class MLP_HcZZ_MW_Deep(nn.Module):
        def __init__(self, input_dim, num_classes=5, hidden_dim=384, num_layers=12, dropout=0.4):
            super().__init__()
            self.input_proj = nn.Sequential(
                nn.Linear(input_dim, hidden_dim),
                nn.BatchNorm1d(hidden_dim),
                nn.ReLU(),
            )
            self.blocks = nn.ModuleList([ResidualBlock(hidden_dim, dropout) for _ in range(num_layers)])
            self.head = nn.Sequential(
                nn.LayerNorm(hidden_dim),
                nn.Linear(hidden_dim, num_classes),
            )

        def forward(self, x):
            x = self.input_proj(x)
            for block in self.blocks:
                x = block(x)
            return self.head(x)


# =============================================================================
# MVA Jet Tag Boundaries (for computing jet_is_* features)
# =============================================================================
MVA_JET_TAG_BOUNDARIES = {
    "C0": {"x": (0.15, 0.4), "y": (0.0, 0.9)},
    "C1": {"x": (0.4, 0.7339), "y": (0.0, 0.9)},
    "C2": {"x": (0.7339, 1.0), "y": (0.1851, 0.2688)},
    "C3": {"x": (0.7339, 1.0), "y": (0.0382, 0.1851)},
    "C4": {"x": (0.7339, 1.0), "y": (0.0, 0.0382)},
    "B0": {"x": (0.7339, 1.0), "y": (0.2688, 0.4057)},
    "B1": {"x": (0.7339, 1.0), "y": (0.4057, 0.4068)},
    "B2": {"x": (0.7339, 1.0), "y": (0.4068, 0.6788)},
    "B3": {"x": (0.7339, 1.0), "y": (0.6788, 0.8406)},
    "B4": {"x": (0.7339, 1.0), "y": (0.8406, 1.0)},
    "L0": {"x": (0.0, 0.15), "y": (0.0, 0.9)},
}


class MVAPostProcessor:
    """Post-processor for MVA inference using b-hive config and model."""

    def __init__(
        self,
        bhive_config_path: str,
        bhive_model_path: str,
        apply_mass_window: bool = True,
    ):
        """
        Args:
            bhive_config_path: Path to b-hive config YAML
            bhive_model_path: Path to model file (.pt)
            apply_mass_window: Whether to apply mass window from config
        """
        self.bhive_config_path = bhive_config_path
        self.bhive_model_path = bhive_model_path
        self.apply_mass_window = apply_mass_window

        # Load config
        self.config = self._load_config()

        # Get mass window from config
        mw = self.config.get('mass_window', {})
        self.mass_window_min = mw.get('min', 100)
        self.mass_window_max = mw.get('max', 150)

        # Get class names from config (read dynamically, not hardcoded)
        # Try multiple possible keys in the config
        self.class_names = (
            self.config.get('class_names') or
            self.config.get('labels') or
            self.config.get('classes') or
            None  # Will be set from model if not in config
        )
        self._num_classes = None  # Will be set when model is loaded

        # Model loaded lazily
        self._model = None

    def _load_config(self) -> Dict:
        """Load b-hive config YAML."""
        if not os.path.exists(self.bhive_config_path):
            raise FileNotFoundError(f"Config not found: {self.bhive_config_path}")
        with open(self.bhive_config_path, 'r') as f:
            return yaml.safe_load(f)

    def _load_model(self):
        """Load PyTorch model."""
        if self._model is not None:
            return

        if not HAS_PYTORCH:
            raise ImportError("PyTorch required")

        checkpoint = torch.load(self.bhive_model_path, map_location='cpu', weights_only=False)

        # Get input dim from checkpoint
        state_dict = checkpoint.get('model_state_dict', checkpoint)
        input_dim = state_dict['input_proj.0.weight'].shape[1]
        num_classes = state_dict['head.1.weight'].shape[0]
        hidden_dim = state_dict['input_proj.0.weight'].shape[0]
        num_layers = sum(1 for k in state_dict if k.startswith('blocks.') and k.endswith('.norm.weight'))

        self._num_classes = num_classes

        # Set default class names if not provided in config
        if self.class_names is None:
            self.class_names = [f'class_{i}' for i in range(num_classes)]
            logging.warning(f"No class_names in config, using defaults: {self.class_names}")
        elif len(self.class_names) != num_classes:
            logging.warning(f"Config has {len(self.class_names)} class names but model has {num_classes} classes")
            # Adjust to match model
            if len(self.class_names) < num_classes:
                self.class_names = list(self.class_names) + [f'class_{i}' for i in range(len(self.class_names), num_classes)]
            else:
                self.class_names = self.class_names[:num_classes]

        logging.info(f"Model: input_dim={input_dim}, hidden_dim={hidden_dim}, num_layers={num_layers}, num_classes={num_classes}")
        logging.info(f"Class names: {self.class_names}")

        self._model = MLP_HcZZ_MW_Deep(input_dim, num_classes, hidden_dim, num_layers)
        self._model.load_state_dict(state_dict)
        self._model.eval()

    def _compute_jet_tags(self, B, CvB, CvL):
        """Compute jet tag categories from PNet scores."""
        pBvsC = 1.0 - CvB
        pBplusC = B + (1.0 - B) * CvL
        pBplusC = np.clip(pBplusC, 0, 1)
        pBvsC = np.clip(pBvsC, 0, 1)

        tags = {}
        for cat, bounds in MVA_JET_TAG_BOUNDARIES.items():
            x_min, x_max = bounds["x"]
            y_min, y_max = bounds["y"]
            mask = (pBplusC >= x_min) & (pBplusC < x_max) & (pBvsC >= y_min) & (pBvsC < y_max)
            tags[f'jet_is_{cat}'] = mask.astype(np.float32)
        return tags

    def _get_col(self, df, names, default=0.0, idx=None):
        """Get column by trying multiple names. Handles array-valued columns.

        Args:
            df: DataFrame
            names: Column name(s) to try
            default: Default value if column not found
            idx: If column contains arrays, extract this index (None = flatten/take first)
        """
        if isinstance(names, str):
            names = [names]

        for name in names:
            if name not in df.columns:
                continue

            col_data = df[name].values
            if len(col_data) == 0:
                continue

            sample = col_data[0]

            # Check if this is an array-valued column
            is_array_col = isinstance(sample, np.ndarray)

            if is_array_col:
                # Column contains arrays - extract appropriately
                extract_idx = idx if idx is not None else 0
                result = []
                for arr in col_data:
                    if isinstance(arr, np.ndarray):
                        if len(arr) > extract_idx:
                            val = arr[extract_idx]
                        elif len(arr) > 0:
                            val = arr[0]  # Fallback to first element
                        else:
                            val = default
                    else:
                        val = arr if arr is not None else default
                    result.append(float(val) if val is not None else default)
                return np.nan_to_num(np.array(result, dtype=np.float32), nan=default)
            else:
                # Scalar values - use directly (ignore idx)
                result = df[name].fillna(default).values
                # Handle any remaining object types
                if result.dtype == object:
                    result = np.array([float(x) if x is not None else default for x in result], dtype=np.float32)
                return np.nan_to_num(result.astype(np.float32), nan=default)

        return np.full(len(df), default, dtype=np.float32)

    def prepare_features(self, df: pd.DataFrame) -> np.ndarray:
        """Prepare features based on config."""
        n = len(df)

        # Get feature lists from config
        global_feats = self.config.get('global_features', [])
        cpf_feats = self.config.get('cpf_candidates', [])
        vtx_feats = self.config.get('vtx_features', [])
        npf_feats = self.config.get('npf_candidates', [])
        lt_feats = self.config.get('lt_candidates', [])

        n_cpf = self.config.get('n_cpf_candidates', 3)
        n_vtx = self.config.get('n_vtx_candidates', 2)
        n_npf = self.config.get('n_npf_candidates', 4)
        n_lt = self.config.get('n_lt_candidates', 1)

        all_features = []

        # Column name mappings
        col_map = {
            'n_jet': ['jet_multiplicity', 'n_jet'],
            'Jet_HT': ['jet_ht', 'Jet_HT'],
            'n_lepton': ['n_lepton'],
            'm4l': ['zz_mass_inclusive', 'm4l'],
            'eta_4l': ['zz_eta_inclusive', 'eta_4l'],
            'pT_4l': ['zz_pt_inclusive', 'pT_4l'],
            'phi_4l': ['zz_phi_inclusive', 'phi_4l'],
            'deltaR_ZZ': ['deltaR_ZZ'],
            'z_pt': ['z1_pt', 'z2_pt'],
            'z_eta': ['z1_eta', 'z2_eta'],
            'z_phi': ['z1_phi', 'z2_phi'],
            'z_mass': ['z1_mass', 'z2_mass'],
        }

        # --- Global features ---
        for feat in global_feats:
            if feat == 'in_mass_window':
                m4l = self._get_col(df, ['zz_mass_inclusive', 'm4l'])
                all_features.append(((m4l >= 100) & (m4l < 150)).astype(np.float32))
            elif feat == 'n_ctagged_jets':
                # Count jets in C categories
                count = np.zeros(n, dtype=np.float32)
                for cat in ['C0', 'C1', 'C2', 'C3', 'C4']:
                    col = f'leadjet_is_{cat}'
                    if col in df.columns:
                        count += df[col].fillna(0).values
                all_features.append(count)
            elif feat == 'n_btagged_jets':
                count = np.zeros(n, dtype=np.float32)
                for cat in ['B0', 'B1', 'B2', 'B3', 'B4']:
                    col = f'leadjet_is_{cat}'
                    if col in df.columns:
                        count += df[col].fillna(0).values
                all_features.append(count)
            elif feat == 'n_lepton':
                all_features.append(np.full(n, 4, dtype=np.float32))
            else:
                names = col_map.get(feat, [feat])
                all_features.append(self._get_col(df, names))

        # --- CPF (c-Jets) features ---
        # Compute jet tags for each c-jet
        for j in range(n_cpf):
            # Get c-jet tagging scores for this jet index
            B = self._get_col(df, ['cjets_btagPNetB', 'jet_btagPNetB'], idx=j)
            CvB = self._get_col(df, ['cjets_btagPNetCvB', 'jet_btagPNetCvB'], idx=j)
            CvL = self._get_col(df, ['cjets_btagPNetCvL', 'jet_btagPNetCvL'], idx=j)
            jet_tags_j = self._compute_jet_tags(B, CvB, CvL)

            for feat in cpf_feats:
                if feat.startswith('cjet_is_') or feat.startswith('jet_is_'):
                    cat = feat.replace('cjet_is_', '').replace('jet_is_', '')
                    if f'jet_is_{cat}' in jet_tags_j:
                        all_features.append(jet_tags_j[f'jet_is_{cat}'])
                    else:
                        all_features.append(np.zeros(n, dtype=np.float32))
                elif feat in ['cjets_pt', 'jet_pt']:
                    all_features.append(self._get_col(df, ['cjets_pt', 'jet_pt'], idx=j))
                elif feat in ['cjets_eta', 'jet_eta']:
                    all_features.append(self._get_col(df, ['cjets_eta', 'jet_eta'], idx=j))
                elif feat in ['cjets_phi', 'jet_phi']:
                    all_features.append(self._get_col(df, ['cjets_phi', 'jet_phi'], idx=j))
                elif feat in ['cjets_mass', 'jet_mass']:
                    all_features.append(self._get_col(df, ['cjets_mass', 'jet_mass'], idx=j))
                else:
                    # Generic array column with index
                    all_features.append(self._get_col(df, [feat], idx=j))

        # --- VTX (Z bosons) features ---
        for j in range(n_vtx):
            for feat in vtx_feats:
                if feat == 'z_pt':
                    # Try scalar columns first, then array column with index
                    all_features.append(self._get_col(df, [f'z{j+1}_pt', 'z_pt'], idx=j))
                elif feat == 'z_eta':
                    all_features.append(self._get_col(df, [f'z{j+1}_eta', 'z_eta'], idx=j))
                elif feat == 'z_phi':
                    all_features.append(self._get_col(df, [f'z{j+1}_phi', 'z_phi'], idx=j))
                elif feat == 'z_mass':
                    all_features.append(self._get_col(df, [f'z{j+1}_mass', 'z_mass'], idx=j))
                else:
                    all_features.append(self._get_col(df, [f'{feat}_{j}', feat], idx=j))

        # --- NPF (Leptons) features ---
        # Leptons can be stored as z1_l1_*, z1_l2_*, z2_l1_*, z2_l2_* or as lepton_* arrays
        lepton_prefixes = ['z1_l1', 'z1_l2', 'z2_l1', 'z2_l2']
        for j in range(n_npf):
            prefix = lepton_prefixes[j] if j < len(lepton_prefixes) else f'lepton_{j}'
            for feat in npf_feats:
                if feat == 'lepton_pt':
                    # Try scalar column first, then array column with index
                    all_features.append(self._get_col(df, [f'{prefix}_pt', 'lepton_pt'], idx=j))
                elif feat == 'lepton_eta':
                    all_features.append(self._get_col(df, [f'{prefix}_eta', 'lepton_eta'], idx=j))
                elif feat == 'lepton_phi':
                    all_features.append(self._get_col(df, [f'{prefix}_phi', 'lepton_phi'], idx=j))
                elif feat == 'lepton_mass':
                    # Try array column first, then use default 0
                    all_features.append(self._get_col(df, ['lepton_mass'], default=0.0, idx=j))
                elif feat == 'lepton_charge':
                    all_features.append(self._get_col(df, [f'{prefix}_charge', 'lepton_charge'], idx=j))
                elif feat == 'lepton_pfRelIso03_all':
                    all_features.append(self._get_col(df, [f'{prefix}_pfRelIso03_all', 'lepton_pfRelIso03_all'], idx=j))
                elif feat == 'lepton_sip3d':
                    all_features.append(self._get_col(df, [f'{prefix}_sip3d', 'lepton_sip3d'], idx=j))
                elif feat == 'lepton_is_tight':
                    all_features.append(self._get_col(df, [f'{prefix}_is_tight', 'lepton_is_tight'], idx=j))
                elif feat == 'lepton_mvaHZZIso':
                    all_features.append(self._get_col(df, [f'{prefix}_mvaHZZIso', 'lepton_mvaHZZIso'], idx=j))
                elif feat == 'lepton_isPFcand':
                    all_features.append(self._get_col(df, [f'{prefix}_isPFcand', 'lepton_isPFcand'], idx=j))
                elif feat == 'lepton_highPtId':
                    all_features.append(self._get_col(df, [f'{prefix}_highPtId', 'lepton_highPtId'], idx=j))
                else:
                    all_features.append(self._get_col(df, [f'{feat}_{j}', feat], idx=j))

        # --- LT (Higgs) features ---
        for feat in lt_feats:
            names = col_map.get(feat, [feat])
            all_features.append(self._get_col(df, names))

        return np.column_stack(all_features)

    def predict(self, features: np.ndarray) -> Dict[str, np.ndarray]:
        """Run inference."""
        self._load_model()

        with torch.no_grad():
            x = torch.from_numpy(features).float()
            logits = self._model(x)
            scores = torch.softmax(logits, dim=1).numpy()

        # Find signal index (try 'Signal', then look for any class containing 'signal')
        signal_idx = None
        for i, name in enumerate(self.class_names):
            if name == 'Signal' or 'signal' in name.lower():
                signal_idx = i
                break
        if signal_idx is None:
            signal_idx = 0  # Default to first class if no signal class found
            logging.warning(f"No 'Signal' class found in {self.class_names}, using class 0 for signal_score")

        return {
            'scores': scores,
            'signal_score': scores[:, signal_idx],
            'class_prediction': np.argmax(scores, axis=1),
        }

    def process_parquet(self, input_file: str, output_file: Optional[str] = None) -> pd.DataFrame:
        """Process single parquet file."""
        df = pd.read_parquet(input_file)
        n = len(df)

        if n == 0:
            return df

        # Load model first to get class names
        self._load_model()

        # Get m4l
        m4l = self._get_col(df, ['zz_mass_inclusive', 'm4l'])

        if self.apply_mass_window:
            mask = (m4l >= self.mass_window_min) & (m4l < self.mass_window_max)
            n_in = mask.sum()

            df['mva_signal_score'] = -1.0
            df['mva_class_prediction'] = -1
            for cls in self.class_names:
                df[f'mva_score_{cls}'] = -1.0

            if n_in > 0:
                features = self.prepare_features(df[mask])
                result = self.predict(features)

                df.loc[mask, 'mva_signal_score'] = result['signal_score']
                df.loc[mask, 'mva_class_prediction'] = result['class_prediction']
                for i, cls in enumerate(self.class_names):
                    df.loc[mask, f'mva_score_{cls}'] = result['scores'][:, i]

                logging.info(f"Processed {n_in}/{n} events in mass window")
        else:
            features = self.prepare_features(df)
            result = self.predict(features)
            df['mva_signal_score'] = result['signal_score']
            df['mva_class_prediction'] = result['class_prediction']
            for i, cls in enumerate(self.class_names):
                df[f'mva_score_{cls}'] = result['scores'][:, i]

        output_path = output_file or input_file
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logging.info(f"Saved: {output_path}")

        return df

    def process_parquets(self, input_dir: str, output_dir: Optional[str] = None, pattern: str = "**/*.parquet"):
        """Process all parquets in directory."""
        files = glob.glob(os.path.join(input_dir, pattern), recursive=True)
        logging.info(f"Found {len(files)} parquet files")

        for f in files:
            out = os.path.join(output_dir, os.path.relpath(f, input_dir)) if output_dir else f
            try:
                self.process_parquet(f, out)
            except Exception as e:
                logging.error(f"Error processing {f}: {e}")


def run_mva_inference(
    workflow: str,
    year: str,
    output_dir: str,
    bhive_config_path: str,
    bhive_model_path: str,
    apply_mass_window: bool = True,
):
    """Convenience function to run MVA inference."""
    input_dir = os.path.join(output_dir, workflow, year)
    if not os.path.exists(input_dir):
        logging.error(f"Not found: {input_dir}")
        return

    processor = MVAPostProcessor(bhive_config_path, bhive_model_path, apply_mass_window)
    processor.process_parquets(input_dir)
    logging.info(f"MVA inference complete for {workflow}/{year}")
