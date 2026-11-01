#!/usr/bin/env python3
"""2026-10-30 MVA optimisation: b-hive inference on a test dataset, keeping m4l (mass_cat = aux_m4l for the
mass-decorrelated configs) and the process id per event. Same model build / dataset loader as tasks/inference.py.
usage: predict_test.py <config> <model_name> <training dir (best_model.pt)> <test dataset dir> <out.npz> [device]
"""
import sys, os, json
sys.path.insert(0, "/eos/user/s/snandaku/b-hive_ttcc"); os.chdir("/eos/user/s/snandaku/b-hive_ttcc")
import numpy as np, torch
from torch.utils.data import DataLoader
from utils.config.config_loader import ConfigLoader
from utils.models.models import BTaggingModels
from utils.torch.DatasetLoader import DatasetLoader
from utils.weighting.batches import expected_batches

cfg_name, model_name, tdir, ddir, out = sys.argv[1:6]
dev = sys.argv[6] if len(sys.argv) > 6 else ("cuda" if torch.cuda.is_available() else "cpu")
config = ConfigLoader.load_config(cfg_name)
model = BTaggingModels(model_name, config).to(dev)
model.create_integers_defaults(); model.create_feature_shapes()
model.mixed_precision = False; model.use_torch_compile = False
bm = torch.load(os.path.join(tdir, "best_model.pt"), map_location=dev, weights_only=False)
model.load_state_dict(bm["model_state_dict"]); model.eval()
files = [f for f in open(os.path.join(ddir, "processed_files.txt")).read().split("\n") if f and os.path.exists(f)]
meta = json.load(open(os.path.join(ddir, "weights.json"))) if os.path.exists(os.path.join(ddir, "weights.json")) else {"chunk_size": [1]}
data = DatasetLoader(config["dataset"])(files=files, model=model, config=config, data_type="inference", verbose=False,
                                         batch_size=4096, out_precision=32)
dl = DataLoader(data, batch_size=None, num_workers=4, pin_memory=False)
cs = np.array(meta["chunk_size"]); cs = np.ones(len(files)) * cs if len(cs) == 1 else cs
dl.nits_expected = expected_batches(cs, 4096, 4, mode="test")[0]
P, T, Wt, Pr, M = [], [], [], [], []
loss_fn = torch.nn.CrossEntropyLoss(reduction="none")
with torch.no_grad():
    for x, truth, w, proc, mcat in dl:
        x = x.float().to(dev)
        inpt, _ = model.get_inpt(x, truth=truth.float().to(dev), loss_fn=loss_fn, attack=None, device=dev)
        p = torch.softmax(model(inpt).float(), dim=1)
        P.append(p.cpu().numpy()); T.append(truth.numpy().astype(int)); Wt.append(w.numpy())
        Pr.append(proc.numpy()); M.append(mcat.numpy())
np.savez(out, pred=np.concatenate(P), truth=np.concatenate(T), weight=np.concatenate(Wt),
         process=np.concatenate(Pr), m4l=np.concatenate(M), classes=np.array(model.classes))
print("saved", out, len(np.concatenate(T)))
