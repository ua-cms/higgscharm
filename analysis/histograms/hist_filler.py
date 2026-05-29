import numpy as np
import awkward as ak


def normalize(array: ak.Array):
    if array.ndim == 2:
        return ak.fill_none(ak.flatten(array), np.nan)
    else:
        return ak.fill_none(array, np.nan)


def get_flow_array(histogram, variable, variables_map):
    histogram_edges = histogram.axes[variable].edges
    epsilon = (histogram_edges[-1] - histogram_edges[-2]) / 2
    hist_max_bin_edge = histogram_edges[-1] - epsilon
    hist_min_bin_edge = histogram_edges[0]
    return np.maximum(
        np.minimum(normalize(variables_map[variable]), hist_max_bin_edge),
        hist_min_bin_edge,
    )


def get_variable_array(histogram, histogram_config, variable, variables_map, flow):
    axis_type = histogram_config.axes[variable].type
    if axis_type in ["IntCategory", "Integer"]:
        variable_array = normalize(variables_map[variable])
        variable_array = ak.to_numpy(variable_array).astype(int)
    elif axis_type == "Boolean":
        variable_array = normalize(variables_map[variable])
        variable_array = ak.to_numpy(variable_array).astype(bool)
    elif flow:
        # add underflow/overflow to first/last bin
        variable_array = get_flow_array(
            histogram=histogram,
            variable=variable,
            variables_map=variables_map,
        )
    else:
        variable_array = normalize(variables_map[variable])

    return variable_array


def fill_histogram(
    histograms,
    histogram_config,
    variables_map,
    category,
    weights,
    variation,
    flow=True,
):
    if histogram_config.layout == "individual":
        for variable in histograms:
            if variable not in variables_map:
                continue
            variable_array = get_variable_array(
                histogram=histograms[variable],
                histogram_config=histogram_config,
                variable=variable,
                variables_map=variables_map,
                flow=flow,
            )
            fill_args = {variable: variable_array}
            if len(histogram_config.categories) > 1:
                fill_args.update({"category": category})
            if histogram_config.add_syst_axis:
                fill_args.update({"variation": variation})
            if histogram_config.add_weight:
                fill_args.update(
                    {
                        "weight": (
                            ak.flatten(ak.ones_like(variables_map[variable]) * weights)
                            if variables_map[variable].ndim == 2
                            else weights
                        ),
                    }
                )
            histograms[variable].fill(**fill_args)
    else:
        for key, variables in histogram_config.layout.items():
            # Separate jagged (per-jet) from scalar (per-event) variables in this group
            jagged_vars = [
                v for v in variables
                if v in variables_map and getattr(variables_map[v], "ndim", 1) == 2
            ]
            scalar_vars = [
                v for v in variables
                if v in variables_map and getattr(variables_map[v], "ndim", 1) < 2
            ]
            # Use the first jagged variable as the reference for broadcasting
            ref_jagged = variables_map[jagged_vars[0]] if jagged_vars else None

            fill_args = {}
            for variable in variables:
                arr = get_variable_array(
                    histogram=histograms[key],
                    histogram_config=histogram_config,
                    variable=variable,
                    variables_map=variables_map,
                    flow=flow,
                )
                # Scalar variables need to be broadcast to match flattened jagged length
                if ref_jagged is not None and variable in scalar_vars:
                    arr = ak.to_numpy(
                        ak.flatten(ak.broadcast_arrays(ak.Array(arr), ref_jagged)[0])
                    )
                fill_args[variable] = arr
            if len(histogram_config.categories) > 1:
                fill_args.update({"category": category})
            if histogram_config.add_syst_axis:
                fill_args.update({"variation": variation})
            if histogram_config.add_weight:
                fill_args.update(
                    {
                        "weight": (
                            ak.to_numpy(ak.flatten(ak.ones_like(ref_jagged) * weights))
                            if ref_jagged is not None
                            else weights
                        ),
                    }
                )
            import logging
            logging.debug(
                f"fill layout '{key}': "
                + ", ".join(
                    f"{k}={len(v) if hasattr(v,'__len__') else '?'}"
                    for k, v in fill_args.items()
                    if k not in ("variation", "category")
                )
            )
            histograms[key].fill(**fill_args)


def fill_histograms(
    histograms,
    histogram_config,
    variables_map,
    category,
    variation,
    flow,
    is_mc,
    weights_container,
):
    if is_mc:
        variations = ["nominal"] + list(weights_container.variations)
        for variation in variations:
            if variation == "nominal":
                region_weight = weights_container.weight()
            else:
                region_weight = weights_container.weight(modifier=variation)
            fill_histogram(
                histograms=histograms,
                histogram_config=histogram_config,
                variables_map=variables_map,
                weights=region_weight,
                variation=variation,
                category=category,
                flow=True,
            )
    else:
        region_weight = weights_container.weight()
        fill_histogram(
            histograms=histograms,
            histogram_config=histogram_config,
            variables_map=variables_map,
            weights=region_weight,
            variation="nominal",
            category=category,
            flow=True,
        )
