"""2026-11-14 (user): k-fold (5) cross-application of the HcZZ MVA, out-of-fold scoring enforced.

Model k is trained on every fold except k (Analysis/kfold_2026_11_13/build_kfold.py, datasets hcZZ_kf<k>), so an MC event of
fold k may only be scored by model k. Fold = int(md5(parquet chunk file name)) % 5, the same function the training-row
extraction used: the chunk name (input file UUID + entry range) is identical in every variation directory, so an event has
one fold (and one model) everywhere. Events that never enter training (data, Z+X, MC outside the training window) follow the
same rule, so data and MC are treated alike. Z+X parquets are not chunk-named: there the fold is the row index % 5.
"""
import hashlib
import os
import re

import numpy as np

from analysis.postprocess.mva_inference import MVAPostProcessor

NFOLD = 5


def chunk_fold(name):
    """Fold of a parquet chunk (file name without directory); identical in all variation directories."""
    return int(hashlib.md5(os.path.basename(name).encode()).hexdigest(), 16) % NFOLD


class KFoldPostProcessor:
    """Drop-in for MVAPostProcessor in run_mva_postprocess.py / run_zx_inference.py with one model per fold.
    model_paths[k] must be the model trained WITHOUT fold k: its path has to contain 'kf<k>' (training-version kf<k>), else
    the constructor raises, so a mis-ordered list can never score an event with a model that saw it."""

    _SHARED = ("era", "mass_window_min", "mass_window_max")

    def __init__(self, bhive_config_path, model_paths, apply_mass_window=True, era=None):
        if len(model_paths) != NFOLD:
            raise ValueError(f"k-fold needs {NFOLD} models, got {len(model_paths)}")
        for k, p in enumerate(model_paths):
            if not re.search(rf"(^|[/_]){'kf'}{k}([/_]|$)", p):
                raise ValueError(f"out-of-fold rule: model {k} must be the one trained without fold {k} ('kf{k}' in its path): {p}")
            if not os.path.exists(p):
                raise FileNotFoundError(p)
        object.__setattr__(self, "procs", [MVAPostProcessor(bhive_config_path=bhive_config_path, bhive_model_path=p,
                                                            apply_mass_window=apply_mass_window, era=era) for p in model_paths])

    def __setattr__(self, name, value):
        if name in self._SHARED:
            for p in self.procs:
                setattr(p, name, value)
        else:
            object.__setattr__(self, name, value)

    def __getattr__(self, name):   # read-only attributes (config, class_names, era, ...) from model 0
        return getattr(self.procs[0], name)

    def _load_model(self):
        for p in self.procs:
            p._load_model()
        names = [p.class_names for p in self.procs]
        if any(n != names[0] for n in names):
            raise ValueError(f"k-fold models disagree on the classes: {names}")

    @property
    def class_names(self):
        self._load_model()
        return self.procs[0].class_names

    def process_parquet(self, input_file, output_file=None):
        return self.procs[chunk_fold(input_file)].process_parquet(input_file, output_file)

    def prepare_features(self, df):
        return self.procs[0].prepare_features(df)   # same config for all folds

    def predict(self, features, folds=None):
        """Per-event model choice: folds[i] (default row index % NFOLD, used for the Z+X parquets)."""
        n = len(features)
        folds = np.arange(n) % NFOLD if folds is None else np.asarray(folds)
        out = None
        for k, p in enumerate(self.procs):
            sel = np.where(folds == k)[0]
            if not len(sel):
                continue
            r = p.predict(features[sel])
            if out is None:
                out = {key: np.zeros((n,) + v.shape[1:], v.dtype) for key, v in r.items()}
            for key, v in r.items():
                out[key][sel] = v
        if out is None:
            out = self.procs[0].predict(features)
        return out
