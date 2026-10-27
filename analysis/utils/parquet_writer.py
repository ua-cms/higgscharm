import numpy as np
import os
import json
import shutil
import tempfile
import pathlib
import awkward as ak
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Optional
from analysis.filesets.utils import get_dataset_config


def _fix_null(typ):
    # Replace any bare Arrow `null` type with a concrete, nullable type.
    # Covers both flat null columns and the `list<item: null>` case that
    # arises when a jagged variable is all-empty-lists (no inner values for
    # awkward to infer a type from). Parquet refuses to write a null-typed
    # field unless it is nullable, hence "NullType Arrow field must be
    # nullable".
    if pa.types.is_null(typ):
        return pa.float64()
    if pa.types.is_list(typ):
        return pa.list_(pa.field("item", _fix_null(typ.value_type), nullable=True))
    if pa.types.is_large_list(typ):
        return pa.large_list(pa.field("item", _fix_null(typ.value_type), nullable=True))
    return typ


def _fix_option_in_list(array):
    """
    Collapse awkward field types that deterministically trigger pyarrow's
    "Lists with non-zero length null components are not supported" on
    write -- confirmed 2026-09-16 against the real pyarrow==19.0.1/
    awkward==2.8.1 combo bundled in the coffea-base-almalinux9:0.7.30 grid
    image (the "extensionarray" workaround below doesn't apply there: that
    kwarg isn't present in this ak.to_arrow_table, so the plain
    ak.to_parquet fallback runs instead, and it hits this error for BOTH
    ak.packed() and a from-scratch to_list/from_iter rebuild -- it's a
    genuine type-shape incompatibility, not a buffer-sharing artifact).
    Verified against real hplusc_mva_4class_CR output (GluGluToContinto2Zto2E2Mu,
    2022preEE): every one of ~25 failing columns (m4l_3p1f, z1_mass_2p2f,
    cjets_h_dphi_3p1f, etc.) has one of two shapes, needing two different,
    semantics-preserving treatments:

      "N * option[var * ?T]" -- a per-event list of length<=1 wrapping a
          single "best candidate" value that may be missing (e.g.
          m4l_3p1f: one m4l per event, or None if this event has no 3P1F
          candidate). ak.firsts() collapses this to a true "N * ?T" scalar
          option, which is what these fields are actually meant to be --
          confirmed this alone fixes the write, verified against the real
          failing file.
      "N * option[var * T]"  -- a genuinely multi-valued per-event list
          (e.g. cjets_h_dphi_3p1f: one dphi per c-jet) that can be
          entirely missing for events with no candidate. ak.firsts() here
          would silently keep only the first c-jet and drop the rest --
          wrong. Replacing the missing whole-list with an empty list
          (ak.fill_none(..., [], axis=0)) preserves every real
          multi-element list untouched and also resolves the write.

    Any other type (plain scalars, and genuinely jagged jet-collection
    axes like jets.phi/cjets.phi that are never option-wrapped in the
    first place) passes through unchanged.
    """
    t = str(ak.type(array))
    if "option[var * ?" in t:
        return ak.firsts(array)
    if "option[var *" in t:
        return ak.fill_none(array, [], axis=0)
    return array


def _xrd_copy(local_file: str, destination: str, attempts: int = 4):
    """
    xrootd copy with retries. Several coffea workers of one job write into the same
    new EOS directory at once, and EOS can then answer one of them with
    "[3018] Unable to mkdir: File exists" (seen 2026-09-27 in 2 of 52 3FS jobs).
    The directory exists by the next attempt, so a short retry is enough.
    """
    import time
    import XRootD.client

    for attempt in range(attempts):
        copyproc = XRootD.client.CopyProcess()
        copyproc.add_job(local_file, destination, force=True)
        copyproc.prepare()
        status, _ = copyproc.run()
        del copyproc
        if status.status == 0:
            return
        if attempt < attempts - 1:
            time.sleep(2 * (attempt + 1))
    raise Exception(status.message)


def dump_pa_table(
    arrays: dict,
    fname: str,
    location: str,
    subdirs: Optional[List[str]] = None,
    extra_metadata: Optional[dict] = None,
):
    subdirs = subdirs or []
    xrd_prefix = "root://"
    pfx_len = len(xrd_prefix)
    xrootd = False
    if xrd_prefix in location:
        try:
            import XRootD
            import XRootD.client

            xrootd = True
        except ImportError as err:
            raise ImportError(
                "Install XRootD python bindings with: conda install -c conda-forge xroot"
            ) from err
    # stage in a private temp dir, not the cwd: condor jobs run from the shared
    # checkout, and 2024/2025 jobs on the same input file produce the same fname
    local_file = os.path.join(tempfile.mkdtemp(prefix="pqstage_"), fname)
    merged_subdirs = "/".join(subdirs) if xrootd else os.path.sep.join(subdirs)
    destination = (
        location + merged_subdirs + f"/{fname}"
        if xrootd
        else os.path.join(location, os.path.join(merged_subdirs, fname))
    )
    out = {}
    for variable, array in arrays.items():
        # awkward arrays: collapse option-in-list shapes that pyarrow cannot write
        # (see _fix_option_in_list; keeps genuine jagged lists such as per-jet columns).
        # Plain Python lists (no .ndim) pass through unchanged.
        if isinstance(array, np.ndarray):
            array = ak.Array(array)  # weights, n_genc10, ... come as numpy arrays
        out[variable] = _fix_option_in_list(array) if isinstance(array, ak.Array) else array

    import inspect
    import pyarrow as pa
    import pyarrow.parquet as pq

    if all(isinstance(a, ak.Array) for a in out.values()):
        arr = ak.Array(out)
        if "extensionarray" in inspect.signature(ak.to_arrow_table).parameters:
            table = ak.to_arrow_table(arr, extensionarray=False)
        else:
            table = ak.to_arrow_table(arr)
    else:
        table = pa.Table.from_pydict(out)
    if extra_metadata:
        existing = dict(table.schema.metadata or {})
        for k, v in extra_metadata.items():
            kk = k.encode() if isinstance(k, str) else k
            vv = v.encode() if isinstance(v, str) else v
            existing[kk] = vv
        table = table.replace_schema_metadata(existing)

    if len(table) != 0:  # skip dataframes with empty entries
        # Sanitize the schema before writing: coerce null / list<null> fields to
        # concrete nullable types and force every field nullable. Casting an
        # all-empty list<null> to list<float64> keeps the (empty) lists intact,
        # so no data is lost.
        new_schema = pa.schema(
            [pa.field(f.name, _fix_null(f.type), nullable=True) for f in table.schema]
        )
        table = table.cast(new_schema)
        pq.write_table(table, local_file)
        if xrootd:
            _xrd_copy(local_file, destination)
        else:
            dirname = os.path.dirname(destination)
            pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
            shutil.copy(local_file, destination)
            assert os.path.isfile(destination)
        pathlib.Path(local_file).unlink()
    os.rmdir(os.path.dirname(local_file))


def dump_sumw_json(
    sumw: float,
    dataset: str,
    fname: str,
    location: str,
    subdirs: Optional[List[str]] = None,
    extra: Optional[dict] = None,
):
    """
    Persist the pre-selection sum of genWeight for one processed chunk.

    Written unconditionally (even when zero events survive the analysis
    selection) so that downstream xs*lumi normalization can divide by the
    true full-sample sum of weights rather than a post-selection subset.

    extra: additional pre-selection sums stored next to "sumw" (added
    2026-09-27 for the 3FS/4FS stitching: sumw split by number of GEN
    c-jets, nominal and per LHE scale variation -- see base.py).
    """
    subdirs = subdirs or []
    xrd_prefix = "root://"
    xrootd = False
    if xrd_prefix in location:
        try:
            import XRootD
            import XRootD.client

            xrootd = True
        except ImportError as err:
            raise ImportError(
                "Install XRootD python bindings with: conda install -c conda-forge xroot"
            ) from err
    # stage in a private temp dir, not the cwd: condor jobs run from the shared
    # checkout, and 2024/2025 jobs on the same input file produce the same fname
    local_file = os.path.join(tempfile.mkdtemp(prefix="pqstage_"), fname)
    merged_subdirs = "/".join(subdirs) if xrootd else os.path.sep.join(subdirs)
    destination = (
        location + merged_subdirs + f"/{fname}"
        if xrootd
        else os.path.join(location, os.path.join(merged_subdirs, fname))
    )
    with open(local_file, "w") as f:
        json.dump({"dataset": dataset, "sumw": float(sumw), **(extra or {})}, f)
    if xrootd:
        _xrd_copy(local_file, destination)
    else:
        dirname = os.path.dirname(destination)
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
        shutil.copy(local_file, destination)
        assert os.path.isfile(destination)
    pathlib.Path(local_file).unlink()
    os.rmdir(os.path.dirname(local_file))


# Function taken from HiggsDNA
# https://gitlab.cern.ch/HiggsDNA-project/HiggsDNA/-/blob/master/higgs_dna/utils/dumping_utils.py
def dump_ak_array(
    akarr: ak.Array,
    fname: str,
    location: str,
    subdirs: Optional[List[str]] = None,
) -> None:
    """
    Dump an ak array to disk at location/'/'.join(subdirs)/fname.
    """
    subdirs = subdirs or []
    xrd_prefix = "root://"
    pfx_len = len(xrd_prefix)
    xrootd = False
    if xrd_prefix in location:
        try:
            import XRootD  # type: ignore
            import XRootD.client  # type: ignore

            xrootd = True
        except ImportError as err:
            raise ImportError(
                "Install XRootD python bindings with: conda install -c conda-forge xroot"
            ) from err
    # stage in a private temp dir, not the cwd: condor jobs run from the shared
    # checkout, and 2024/2025 jobs on the same input file produce the same fname
    local_file = os.path.join(tempfile.mkdtemp(prefix="pqstage_"), fname)
    merged_subdirs = "/".join(subdirs) if xrootd else os.path.sep.join(subdirs)
    destination = (
        location + merged_subdirs + f"/{fname}"
        if xrootd
        else os.path.join(location, os.path.join(merged_subdirs, fname))
    )
    ak.to_parquet(ak.fill_none(akarr, [], axis=0), local_file)
    if xrootd:
        _xrd_copy(local_file, destination)
    else:
        dirname = os.path.dirname(destination)
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
        shutil.copy(local_file, destination)
        assert os.path.isfile(destination)
    pathlib.Path(local_file).unlink()
    os.rmdir(os.path.dirname(local_file))


def dump_parquet(
    events,
    weights_container,
    variables_map,
    workflow,
    year,
    category,
    output_location,
    partition_key,
    shift,
):
    is_mc = hasattr(events, "genWeight")
    dataset = events.metadata["dataset"]

    if shift is None:
        # nominal: full weight set (nominal + per-syst Up/Down columns)
        if is_mc:
            variations = ["nominal", *weights_container.variations]
            for variation in variations:
                if variation == "nominal":
                    variables_map[f"weight_nominal"] = weights_container.weight()
                    for partial_weight in weights_container.weightStatistics:
                        variables_map[f"weight_{partial_weight}"] = (
                            weights_container.partial_weight(include=[partial_weight])
                        )
                else:
                    variables_map[f"weight_{variation}"] = weights_container.weight(
                        modifier=variation
                    )
        subdirs = [workflow, year, dataset, category]
    else:
        # object shift: kinematics already reflect the shift; only the nominal
        # weight is needed (weight-systematics are not crossed with object shifts).
        # Output goes to its own <shift> subdir so the MVA/combine path can read
        # each variation independently.
        if is_mc:
            variables_map["weight_nominal"] = weights_container.weight()
        subdirs = [workflow, year, dataset, category, shift]

    # Self-normalising metadata: per-shard sumw (genWeight unchanged by object
    # shifts) + xsec/era from the dataset config. merge_parquets aggregates sumw
    # across shards. Lets downstream compute lumi*xsec/sumw without a sidecar.
    extra_metadata = None
    if is_mc:
        dataset_info = get_dataset_config(year).get(dataset, {})
        extra_metadata = {
            "sumw": str(float(ak.sum(events.genWeight))),
            "xsec": str(dataset_info.get("xsec", "")),
            "era": str(dataset_info.get("era", "")),
        }

    fname = partition_key.replace("/", "_") + ".parquet"
    dump_pa_table(
        variables_map, fname, output_location, subdirs, extra_metadata=extra_metadata
    )
