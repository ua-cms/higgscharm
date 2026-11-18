#!/usr/bin/env python3
"""2026-11-06: standalone b-hive inference (same steps as tasks/inference.py, no law: the training jobs delete the
per-epoch checkpoints, so law would consider the TrainingTask incomplete and retrain). Writes prediction/truth/
kinematics/process/weights/m4l .npy into <outdir>.
Usage: infer.py <config name> <best_model.pt> <test dataset dir> <outdir> [device]"""
import os, sys, json
from types import SimpleNamespace
BH = "/eos/user/s/snandaku/b-hive_ttcc"; sys.path.insert(0, BH); os.chdir(BH); os.environ.setdefault("B_HIVE_DIR", BH)
import numpy as np, torch
from torch.utils.data import DataLoader
from utils.config.config_loader import ConfigLoader
from utils.models.models import BTaggingModels
from utils.torch.DatasetLoader import DatasetLoader
from utils.weighting.batches import expected_batches
cfg_name, model_path, ds, out = sys.argv[1:5]; device = sys.argv[5] if len(sys.argv) > 5 else ("cuda" if torch.cuda.is_available() else "cpu")
os.makedirs(out, exist_ok=True)
config = ConfigLoader.load_config(cfg_name)
# 2026-11-16: HCZZ_INFER_MODEL selects the b-hive model class (default 4-class; 3-class ZZ-merged k-fold test)
model = BTaggingModels(os.environ.get("HCZZ_INFER_MODEL", "MLP_HcZZ_MW_Deep_4class"), config).to(device)
model.create_integers_defaults(); model.create_feature_shapes(); model.mixed_precision = device == "cuda"; model.use_torch_compile = False
model.load_state_dict(torch.load(model_path, map_location=torch.device(device), weights_only=False)["model_state_dict"])
files = [f for f in open(f"{ds}/processed_files.txt").read().split("\n") if f and os.path.exists(f)]
chunks = np.array(json.load(open(f"{ds}/weights.json"))["chunk_size"])
data = DatasetLoader(config["dataset"])(files=files, model=model, config=config, data_type="inference", verbose=False,
                                        batch_size=1024, out_precision="float32")
dl = DataLoader(data, batch_size=None, num_workers=4, pin_memory=True)
dl.nits_expected = expected_batches(chunks, 1024, 4, mode="test")[0]
o = {k: SimpleNamespace(path=f"{out}/{k}.npy") for k in ("prediction", "truth", "kinematics", "process", "weights", "inference_time")}
pred, truth, *_ = model.predict_model(dl, o, None, device)
print(f"{out}: {len(pred)} events, m4l saved: {os.path.exists(out + '/m4l.npy')}")
