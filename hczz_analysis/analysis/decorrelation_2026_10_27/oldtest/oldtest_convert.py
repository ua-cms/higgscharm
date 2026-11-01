#!/usr/bin/env python3
"""2026-10-28 (user): evaluate the decorrelation models on the OLD production test set (the one behind the production
Signal_vs_all AUC 0.910: DatasetConstructorTask/hc_zzto4l_mw_training_4class_nomass/hcZZ_big4classtestjetnomasslooseoldcbal).

The parquets behind it no longer exist, but its per-file .npz (structured arrays, raw features, written by the
LZ4HcZZProcessor) do. This script rewrites them for another config and builds a b-hive test dataset
(output/DatasetConstructorTask/<config>/<version>/) with b-hive's own saveOutput + merge_datasets, so that
`law run ROCCurveTask --test-dataset-version <version>` evaluates it exactly like any test dataset.

Conversion: global_features and lt_arr are reduced to the config's stored features (m4l dropped from the inputs;
kept as the last lt column when the config has aux_m4l, where LZ4Dataset strips it and divides the mass-scaled
features by it). All other arrays are copied. No event selection (the old test set is already 90 <= m4l < 160).

--extra-src: more source dirs of production-config npz (10-28: clean ggZZ from the current test set).
--exclude-filelist: drop every source file whose NanoAOD chunk (uuid + entry range) is in that filelist
(e.g. the training filelist of the new models -> a clean subset).

usage (from the b-hive directory, b_hive_new env):
  python3 oldtest_convert.py <config> <dataset-version> [--exclude-filelist f ...]
"""
import argparse, glob, json, os, re, sys
import numpy as np
from numpy.lib import recfunctions as rfn

sys.path.insert(0, os.getcwd())
from utils.config.config_loader import ConfigLoader
from utils.coffea_processors.processor_loader import ProcessorLoader
from utils.dataset.merging import merge_datasets

SRC = "/eos/user/s/snandaku/b-hive_ttcc/output/DatasetConstructorTask/hc_zzto4l_mw_training_4class_nomass/hcZZ_big4classtestjetnomasslooseoldcbal"
OUT = "/eos/user/s/snandaku/b-hive_ttcc/output/DatasetConstructorTask"
KEY = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_%2FEvents%3B1_\d+-\d+)")

ap = argparse.ArgumentParser()
ap.add_argument("config"); ap.add_argument("version")
ap.add_argument("--exclude-filelist", nargs="*", default=[])
ap.add_argument("--src", default=SRC, help="main npz source dir (default: the old production test set)")
ap.add_argument("--extra-src", nargs="*", default=[], help="more npz source dirs (written with the production config), e.g. the clean ggZZ of the current test set")
a = ap.parse_args()
SRC = a.src

config = ConfigLoader.load_config(a.config)
# stored global features exactly as DatasetConstructorTask
computed = set()
mw = config.get("mass_window")
allf = sum((config.get(k, []) for k in ("global_features", "cpf_candidates", "npf_candidates", "vtx_features", "lt_candidates")), [])
if mw and "in_mass_window" in config.get("global_features", []) and mw.get("feature", "m4l") in allf:
    computed.add("in_mass_window")
gstore = [f for f in config.get("global_features", []) if f not in computed]
aux = bool(config.get("aux_m4l", False))
ltstore = list(config.get("lt_candidates", [])) + (["m4l"] if aux else [])

excl = set()
for fl in a.exclude_filelist:
    for l in open(fl):
        m = KEY.search(os.path.basename(l.strip()))
        if m: excl.add(m.group(1))

path = os.path.join(OUT, a.config, a.version)
os.makedirs(path, exist_ok=True)
proc = ProcessorLoader(
    config.get("processor", "PFCandidateAndVertexProcessing"), output_directory=path,
    bins_pt=config.get("bins_pt"), bins_eta=config.get("bins_eta"), processes=config.get("processes"),
    global_features=gstore, global_custom_features=config.get("global_custom_features", []),
    cpf_candidates=config.get("cpf_candidates", []), cpf_custom_features=config.get("cpf_custom_features", []),
    npf_candidates=config.get("npf_candidates", []), npf_custom_features=config.get("npf_custom_features", []),
    vtx_features=config.get("vtx_features", []), vtx_custom_features=config.get("vtx_custom_features", []),
    lt_candidates=config.get("lt_candidates", []), lt_custom_features=config.get("lt_custom_features", []),
    n_cpf_candidates=config.get("n_cpf_candidates", 50), n_npf_candidates=config.get("n_npf_candidates", 50),
    n_vtx_features=config.get("n_vtx_candidates", 5), n_lt_candidates=config.get("n_lt_candidates", 5),
    pt_key=config.get("pt_key", "jet_pt"), eta_key=config.get("eta_key", "jet_eta"),
    truths=config.get("truths"), precision=config.get("data_precision", "float32"),
    selections=config.get("selections"), mass_window=config.get("mass_window"),
    **({"aux_m4l": True} if aux else {}))


def keep(arr, names, what):
    miss = [n for n in names if n not in arr.dtype.names]
    if miss: raise ValueError(f"{what}: missing {miss} in old npz ({arr.dtype.names})")
    return rfn.repack_fields(arr[names])


def same(arr, names, what):
    if list(arr.dtype.names) != list(names): raise ValueError(f"{what}: old {arr.dtype.names} != config {names}")
    return arr


files, nsk, nev, cls = [], 0, 0, np.zeros(4)
for f in sorted(glob.glob(SRC + "/*.npz")) + sum((sorted(glob.glob(x + "/*.npz")) for x in a.extra_src), []):
    m = KEY.search(os.path.basename(f))
    if m is None: raise ValueError(f"no chunk key in {f}")
    if m.group(1) in excl: nsk += 1; continue
    d = np.load(f, allow_pickle=True)
    g = keep(d["global_features"], gstore, "global")
    lt = keep(d["lt_arr"], ltstore, "lt")
    cpf = same(d["cpf_arr"], config.get("cpf_candidates", []), "cpf")
    npf = same(d["npf_arr"], config.get("npf_candidates", []), "npf")
    vtx = same(d["vtx_arr"], config.get("vtx_features", []), "vtx")
    out = os.path.join(path, os.path.basename(f))
    proc.saveOutput(out, g, cpf, npf, vtx, lt, d["truth"], d["process"], d["weight_nominal"])
    files.append(out); nev += len(g)
    cls += np.array([d["truth"][k].sum() for k in d["truth"].dtype.names])
print(f"{a.config} -> {path}: {len(files)} files, {nsk} excluded, {nev} events, per class {cls.astype(int).tolist()}")
print(f"global {gstore}\nlt {ltstore}")

hist = np.load(os.path.join(SRC, "histogram.npy"))
allf = merge_datasets(files, path, label="file", processor=config.get("processor"), chunk_size=100000, workers=8,
                      shuffle=True, histograms=hist, reference_key=list(config["truths"]).index(config["reference_flavour"]),
                      bins_pt=config["bins_pt"], bins_eta=config["bins_eta"], pt_key=config.get("pt_key", "jet_pt"),
                      eta_key=config.get("eta_key", "jet_eta"), global_features=gstore,
                      precision=config.get("data_precision", "float32"), xsec_config={}, sumw_per_process=None)
np.save(os.path.join(path, "histogram.npy"), hist)
open(os.path.join(path, "processed_files.txt"), "w").write("\n".join(allf))
json.dump({"source": SRC, "extra_src": a.extra_src, "excluded_files": nsk, "files": len(files), "events": nev, "per_class": cls.tolist(),
           "exclude_filelists": a.exclude_filelist}, open(os.path.join(path, "oldtest_provenance.json"), "w"), indent=1)
print("done", allf)
