"""Registry of the HcZZ MVA models (models/registry.yaml).

resolve_model(name) returns the absolute config/model paths of a registered model after checking their md5 sums,
so a scored production can always be traced to the exact checkpoint. name=None selects the registry default.
"""
import hashlib
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
REGISTRY = REPO / "models" / "registry.yaml"


def load_registry(path=REGISTRY):
    with open(path) as f:
        return yaml.safe_load(f)


def md5sum(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def resolve_model(name=None, era=None, registry=None):
    """dict(name, config, model, entry) of a registered model; raises on unknown name, missing file, md5 mismatch or
    an era the model was not made for."""
    registry = registry or load_registry()
    name = name or registry.get("default")
    if not name:
        raise ValueError(f"no model name given and no default in {REGISTRY}")
    models = registry["models"]
    if name not in models:
        raise KeyError(f"model '{name}' not in {REGISTRY} (available: {sorted(models)})")
    entry = models[name]
    paths = {key: REPO / entry[key] for key in ("config", "model")}
    for key, path in paths.items():
        if not path.exists():
            raise FileNotFoundError(f"{name}: {key} file {path} not found")
        found = md5sum(path)
        if found != entry["md5"][key]:
            raise ValueError(f"{name}: md5 of {path} is {found}, registry expects {entry['md5'][key]}")
    if era is not None and era not in [str(e) for e in entry.get("eras", [])]:
        raise ValueError(f"{name}: era {era} not in the model's eras {entry.get('eras')}")
    out = {"name": name, "config": str(paths["config"]), "model": str(paths["model"]), "entry": entry}
    if "kfold_models" in entry:
        # k-fold entry: kfold_models[k] = model trained without fold k (scored out-of-fold by analysis/postprocess/kfold.py)
        kf = [REPO / p for p in entry["kfold_models"]]
        sums = entry["md5"].get("kfold", [])
        if len(kf) != 5 or len(sums) != 5:
            raise ValueError(f"{name}: a k-fold entry needs 5 kfold_models and 5 md5.kfold sums")
        for k, (path, want) in enumerate(zip(kf, sums)):
            if not path.exists():
                raise FileNotFoundError(f"{name}: k-fold model {k} {path} not found")
            if md5sum(path) != want:
                raise ValueError(f"{name}: md5 of k-fold model {k} {path} is {md5sum(path)}, registry expects {want}")
        out["kfold_models"] = [str(p) for p in kf]
    return out


def shell_exports(name=None, era=None):
    """Shell lines exporting HCZZ_CFG / HCZZ_MODEL_PT / HCZZ_KFMODELS of a registered model (used by hczz_chain/)."""
    r = resolve_model(name, era=era)
    return "\n".join([f"export HCZZ_MODEL_NAME={r['name']}", f"export HCZZ_CFG={r['config']}", f"export HCZZ_MODEL_PT={r['model']}",
                      f"export HCZZ_KFMODELS={','.join(r.get('kfold_models', []))}"])


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Resolve a registered HcZZ MVA model (md5-checked)")
    ap.add_argument("name", nargs="?", default=None, help="model name (default: registry default)")
    ap.add_argument("--era", default=None)
    ap.add_argument("--shell", action="store_true", help="print shell export lines")
    a = ap.parse_args()
    print(shell_exports(a.name, a.era) if a.shell else resolve_model(a.name, era=a.era))
