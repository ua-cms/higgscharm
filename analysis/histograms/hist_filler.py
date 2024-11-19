import numpy as np
import awkward as ak


def normalize(array: ak.Array):
    if array.ndim == 2:
        return ak.fill_none(ak.flatten(array), np.nan)
    else:
        return ak.fill_none(array, np.nan)


def get_flow_array(histogram, feature, feature_map):
    histogram_edges = histogram.axes[feature].edges
    epsilon = (histogram_edges[-1] - histogram_edges[-2]) / 2
    hist_max_bin_edge = histogram_edges[-1] - epsilon
    hist_min_bin_edge = histogram_edges[0]

    return np.maximum(
        np.minimum(normalize(feature_map[feature]), hist_max_bin_edge),
        hist_min_bin_edge,
    )


def get_variable_array(histogram, histogram_config, feature, feature_map, flow):
    if histogram_config.axes[feature]["type"] == "IntCategory":
        variable_array = normalize(feature_map[feature])
        # convert to integer array
        variable_array = ak.to_numpy(variable_array).astype(int)
    elif flow:
        # add underflow/overflow to first/last bin
        variable_array = get_flow_array(
            histogram=histogram,
            feature=feature,
            feature_map=feature_map,
        )
    else:
        variable_array = normalize(feature_map[feature])

    return variable_array


def fill_histogram(
    histograms, histogram_config, feature_map, category, weights, variation, flow=True
):
    if histogram_config.layout == "individual":
        for feature in histograms:
            variable_array = get_variable_array(
                histograms[feature], histogram_config, feature, feature_map, flow
            )
            fill_args = {
                feature: variable_array,
                "variation": variation,
                "category": category,
                "weight": (
                    ak.flatten(ak.ones_like(feature_map[feature]) * weights)
                    if feature_map[feature].ndim == 2
                    else weights
                ),
            }
            histograms[feature].fill(**fill_args)
    else:
        for key, features in histogram_config.layout.items():
            fill_args = {}
            for feature in features:
                fill_args[feature] = get_variable_array(
                    histograms[key], histogram_config, feature, feature_map, flow
                )
            fill_args.update(
                {
                    "variation": variation,
                    "category": category,
                    "weight": (
                        ak.flatten(ak.ones_like(feature_map[feature]) * weights)
                        if feature_map[feature].ndim == 2
                        else weights
                    ),
                }
            )
            histograms[key].fill(**fill_args)