#!/usr/bin/env python3
"""
Smoke test for the GEN b-jet split added for the H+b 4FS/5FS stitching (2026-11-16, analysis/processors/base.py.genb_candidate):
runs the CURRENT base.py and the candidate over the same single HPlusBottom4FS_2022preEE file in hplusc_mva_4class_ctag2d_final_bkg
and checks (1) same parquet files, row counts and GEN columns, schema = old + n_genb10; (2) every old sidecar key unchanged;
(3) sumw_genb{0,1,2p} add up to sumw, sumw_preveto_genb{0,1,2p} to sumw_preveto, 9 scale entries each; (4) the split equals an
independent uproot count (GenJet hadronFlavour == 5, pT > 10). Usage (LCG_105, proxy): python3 smoketest_genb.py [out_dir]
"""
import importlib.util
import json
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
from coffea import processor
from coffea.nanoevents import NanoAODSchema

HERE = Path(__file__).resolve().parent
FILESET = Path("/afs/cern.ch/user/s/snandaku/Higgscharmnew/higgscharm/analysis/filesets/fileset_2022preEE_nanov12_lxplus.json")
WORKFLOW = "hplusc_mva_4class_ctag2d_final_bkg"
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(tempfile.gettempdir()) / "hczz_genb_smoketest"


def load_processor(path, name):
    from importlib.machinery import SourceFileLoader
    spec = importlib.util.spec_from_file_location(name, path, loader=SourceFileLoader(name, str(path)))
    mod = importlib.util.module_from_spec(spec)
    # coffea cloudpickles the processor; a module that is not in sys.modules would be pickled
    # by value and lose the names the selection strings use through eval()
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod.BaseProcessor


def run(proc_cls, fileset, out_dir):
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    processor.run_uproot_job(
        fileset, treename="Events",
        processor_instance=proc_cls(workflow=WORKFLOW, year="2022preEE",
                                    output_format="parquet", output_location=str(out_dir) + "/"),
        executor=processor.iterative_executor, executor_args={"schema": NanoAODSchema, "workers": 1},
    )


def standalone_genb(url):
    import uproot
    import awkward as ak
    a = uproot.open(url)["Events"].arrays(["genWeight", "GenJet_pt", "GenJet_hadronFlavour"])
    n = ak.to_numpy(ak.sum((a.GenJet_hadronFlavour == 5) & (a.GenJet_pt > 10.0), axis=1))
    w = ak.to_numpy(a.genWeight).astype(np.float64)
    return [float(w[n == 0].sum()), float(w[n == 1].sum()), float(w[n >= 2].sum())], float(w.sum())


def main():
    import pyarrow.parquet as pq
    one_file = json.loads(FILESET.read_text())["HPlusBottom4FS_2022preEE"][0]
    fileset = {"HPlusBottom4FS_2022preEE": [one_file]}
    print(f"Testing against: {one_file}")
    old_cls = load_processor(HERE / "analysis/processors/base.py", "base_old")
    new_cls = load_processor(HERE / "analysis/processors/base.py.genb_candidate", "base_new")
    run(old_cls, fileset, OUT / "old")
    run(new_cls, fileset, OUT / "new")
    old_pq = {p.relative_to(OUT / "old"): p for p in (OUT / "old").glob("**/*.parquet")}
    new_pq = {p.relative_to(OUT / "new"): p for p in (OUT / "new").glob("**/*.parquet")}
    assert old_pq and set(old_pq) == set(new_pq), "parquet file sets differ"
    gen_cols = ["n_genc10", "n_chad5", "n_chad10", "gen_chad1_pt"]
    nb = []
    for rel in old_pq:
        a, b = pq.read_table(old_pq[rel]), pq.read_table(new_pq[rel])
        assert b.schema.names == a.schema.names + ["n_genb10"] or set(b.schema.names) == set(a.schema.names) | {"n_genb10"}, f"schema: {rel}"
        assert a.num_rows == b.num_rows, f"row count differs: {rel}"
        for c in gen_cols:
            if c in a.schema.names:
                assert a.column(c).equals(b.column(c)), f"GEN column {c} differs: {rel}"
        if str(rel).startswith("base") or "/base/" in str(rel):
            nb += b.column("n_genb10").to_pylist()
    print(f"1. {len(old_pq)} parquets: same files, row counts and GEN columns; new column n_genb10 (selected events in base: {len(nb)}, "
          f"0b/1b/>=2b {sum(1 for x in nb if x == 0)}/{sum(1 for x in nb if x == 1)}/{sum(1 for x in nb if x >= 2)})")
    js_old = sorted((OUT / "old").glob("**/sumw/*.json")); js_new = sorted((OUT / "new").glob("**/sumw/*.json"))
    assert len(js_old) == len(js_new) >= 1
    r_old, r_new = json.loads(js_old[0].read_text()), json.loads(js_new[0].read_text())
    for k, v in r_old.items():
        assert r_new.get(k) == v, f"sidecar key changed: {k}"
    added = sorted(set(r_new) - set(r_old))
    print(f"2. all {len(r_old)} old sidecar keys unchanged; added: {added}")
    pre = [r_new[f"sumw_preveto_genb{b}"] for b in ("0", "1", "2p")]; post = [r_new[f"sumw_genb{b}"] for b in ("0", "1", "2p")]
    assert abs(sum(pre) - r_new["sumw_preveto"]) < 1e-6 * abs(r_new["sumw_preveto"]), (pre, r_new["sumw_preveto"])
    assert abs(sum(post) - r_new["sumw"]) < 1e-6 * abs(r_new["sumw"]), (post, r_new["sumw"])
    for b in ("0", "1", "2p"):
        assert len(r_new[f"sumw_preveto_genb{b}_scalevar"]) == 9 and len(r_new[f"sumw_genb{b}_scalevar"]) == 9
    print(f"3. preveto genb 0/1/2p {np.round(pre, 3)} (sum = sumw_preveto {r_new['sumw_preveto']:.3f}); postveto {np.round(post, 3)} (sum = sumw {r_new['sumw']:.3f})")
    print(f"   fractions pre {np.round(np.array(pre) / sum(pre), 4)}  post {np.round(np.array(post) / sum(post), 4)}")
    ref, tot = standalone_genb(one_file)
    assert np.allclose(pre, ref, rtol=1e-6), (pre, ref)
    print(f"4. standalone uproot GEN b-jet sums {np.round(ref, 3)} match")
    print("\nSMOKE TEST PASSED")


if __name__ == "__main__":
    main()
