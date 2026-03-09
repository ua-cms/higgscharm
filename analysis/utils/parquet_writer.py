import os
import shutil
import pathlib
import awkward as ak
import uuid
from typing import List, Optional


def dump_pa_table(
    arrays: dict,
    fname: str,
    location: str,
    subdirs: Optional[List[str]] = None,
):
    """
    Write a parquet file either locally or via XRootD.
    """

    import pyarrow as pa
    import pyarrow.parquet as pq
    import numpy as np

    subdirs = subdirs or []

    # -----------------------------------------
    # Detect if destination is XRootD
    # -----------------------------------------
    xrd_prefix = "root://"
    xrootd = xrd_prefix in location

    if xrootd:
        try:
            import XRootD
            import XRootD.client
        except ImportError as err:
            raise ImportError(
                "Install XRootD python bindings with: conda install -c conda-forge xroot"
            ) from err

    # -----------------------------------------
    # Temporary local file
    # -----------------------------------------
    local_file = os.path.abspath(fname)

    merged_subdirs = "/".join(subdirs) if xrootd else os.path.sep.join(subdirs)

    destination = (
        location + merged_subdirs + f"/{fname}"
        if xrootd
        else os.path.join(location, merged_subdirs, fname)
    )

    # -----------------------------------------
    # Convert arrays
    # -----------------------------------------
    out = {}

    for variable, array in arrays.items():

        if isinstance(array, ak.Array):

            if array.ndim == 1:
                out[variable] = ak.to_numpy(ak.fill_none(array, 0))

            else:
                out[variable] = ak.to_list(array)

        elif isinstance(array, np.ndarray):

            if array.ndim == 2:
                out[variable] = array.tolist()
            else:
                out[variable] = array

        else:
            out[variable] = array

    table = pa.Table.from_pydict(out)

    if len(table) == 0:
        return

    # -----------------------------------------
    # Write parquet
    # -----------------------------------------
    pq.write_table(table, local_file)

    if xrootd:

        copyproc = XRootD.client.CopyProcess()
        copyproc.add_job(local_file, destination, force=True)
        copyproc.prepare()

        status, response = copyproc.run()

        if status.status != 0:
            raise RuntimeError(status.message)

    else:

        dirname = os.path.dirname(destination)
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)

        shutil.move(local_file, destination)

        assert os.path.isfile(destination)

    pathlib.Path(local_file).unlink(missing_ok=True)


# ------------------------------------------------------------
# Dump awkward array helper
# ------------------------------------------------------------
def dump_ak_array(
    akarr: ak.Array,
    fname: str,
    location: str,
    subdirs: Optional[List[str]] = None,
):

    subdirs = subdirs or []

    xrd_prefix = "root://"
    xrootd = xrd_prefix in location

    if xrootd:
        try:
            import XRootD
            import XRootD.client
        except ImportError as err:
            raise ImportError(
                "Install XRootD python bindings with: conda install -c conda-forge xroot"
            ) from err

    local_file = os.path.abspath(fname)

    merged_subdirs = "/".join(subdirs) if xrootd else os.path.sep.join(subdirs)

    destination = (
        location + merged_subdirs + f"/{fname}"
        if xrootd
        else os.path.join(location, merged_subdirs, fname)
    )

    ak.to_parquet(ak.fill_none(akarr, [], axis=0), local_file)

    if xrootd:

        copyproc = XRootD.client.CopyProcess()
        copyproc.add_job(local_file, destination, force=True)
        copyproc.prepare()

        status, response = copyproc.run()

        if status.status != 0:
            raise RuntimeError(status.message)

    else:

        dirname = os.path.dirname(destination)
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)

        shutil.move(local_file, destination)

        assert os.path.isfile(destination)

    pathlib.Path(local_file).unlink(missing_ok=True)


# ------------------------------------------------------------
# Main parquet dumping function used by processor
# ------------------------------------------------------------
def dump_parquet(
    events,
    weights_container,
    variables_map,
    workflow,
    year,
    category,
    output_location,
    shift,
):

    if shift is not None:
        return

    # -----------------------------------------
    # Add weights
    # -----------------------------------------
    if hasattr(events, "genWeight"):

        variations = ["nominal", *weights_container.variations]

        for variation in variations:

            if variation == "nominal":

                variables_map["weight_nominal"] = weights_container.weight()

                for partial_weight in weights_container.weightStatistics:

                    variables_map[f"weight_{partial_weight}"] = (
                        weights_container.partial_weight(include=[partial_weight])
                    )

            else:

                variables_map[f"weight_{variation}"] = weights_container.weight(
                    modifier=variation
                )

    # -----------------------------------------
    # Dataset name
    # -----------------------------------------
    dataset_full = events.metadata["dataset"]
    dataset = dataset_full.rsplit("_", 1)[0]

    # -----------------------------------------
    # Unique parquet chunk id
    # -----------------------------------------
    chunk_id = uuid.uuid4().hex[:8]

    fname = f"{dataset_full}_{chunk_id}.parquet"

    # -----------------------------------------
    # Save inside dataset folder
    # -----------------------------------------
    subdirs = [workflow, year, dataset]

    dump_pa_table(
        variables_map,
        fname,
        output_location,
        subdirs,
    )