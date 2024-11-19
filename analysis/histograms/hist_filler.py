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


def get_features_array(histograms: dict, layout: str | dict):
    if isinstance(layout, str):
        filler_args = {}
        for feature in histograms:
            filler_args[feature] = get_variable_array(
                histograms[feature], histogram_config, feature, feature_map, flow
            )
    elif isinstance(layout, dict):
        for key, features in layout.items():
            filler_args = {}
            for feature in features:
                filler_args[feature] = get_variable_array(
                    histograms[key], histogram_config, feature, feature_map, flow
                )


def get_filler_args(variation, category, variable_array, weights):
    return {
        "variation": variation,
        "category": category,
        "weight": (
            ak.flatten(ak.ones_like(variable_array) * weights)
            if feature_map[feature].ndim == 2
            else weights
        ),
    }


def fill_histogram(
    filler_args, hist_to_fill, variation, category, feature_map, weights
):
    filler_args = get_filler_args(variation, category, feature_map[feature], weights)

    hist_to_fill.fill(**filler_args)


def fill_histograms(
    histograms, histogram_config, feature_map, category, weights, variation, flow=True
):
    if histogram_config.layout == "individual":
        for feature in histograms:
            filler_args = get_filler_args(variation, category, variable_array, weights)
            filler_args[feature] = get_variable_array(
                histograms[feature], histogram_config, feature, feature_map, flow
            )
            histograms[feature].fill(**filler_args)
    else:
        for key, features in histogram_config.layout.items():
            filler_args = {}
            filler_args = get_filler_args(variation, category, variable_array, weights)
            for feature in features:
                filler_args[feature] = get_variable_array(
                    histograms[key], histogram_config, feature, feature_map, flow
                )
            filler_args.update(
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
            histograms[key].fill(**filler_args)
