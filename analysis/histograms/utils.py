import hist
import hist.dask as hda
from analysis.configs.histogram_config import HistogramConfig


def build_axis(axis_config: dict):
    """build a hist axis object from an axis config"""
    axis_opt = {
        "StrCategory": hist.axis.StrCategory,
        "IntCategory": hist.axis.IntCategory,
        "Regular": hist.axis.Regular,
        "Variable": hist.axis.Variable,
    }
    axis_args = {}
    for name in axis_config:
        axis_args["name"] = name
        hist_type = axis_config[name]["type"]
        for arg_name, arg_value in axis_config[name].items():
            if arg_name == "type":
                continue
            axis_args[arg_name] = arg_value
    hist_args = {k: v for k, v in axis_args.items()}
    axis = axis_opt[hist_type](**hist_args)
    return axis


def build_histogram(histogram_config: HistogramConfig):
    """
    build a hist.dask histogram for each axis config in HistogramConfig.
    Optionally include 'systematic' and 'weight' axes to histograms.
    """
    syst_axis = build_axis(
        {"variation": {"type": "StrCategory", "categories": [], "growth": True}}
    )
    dataset_axis = build_axis(
        {"dataset": {"type": "StrCategory", "categories": [], "growth": True}}
    )
    if histogram_config.individual:
        histograms = {}
        for name, args in histogram_config.axes.items():
            axes = [build_axis({name: args})]
            if histogram_config.add_dataset_axis:
                axes.append(dataset_axis)
            if histogram_config.add_syst_axis:
                axes.append(syst_axis)
            if histogram_config.add_weight:
                axes.append(hist.storage.Weight())
            histograms[name] = hda.hist.Hist(*axes)
        return histograms
    else:
        axes = []
        for name, args in histogram_config.axes.items():
            axes.append(build_axis({name: args}))
        if histogram_config.add_dataset_axis:
            axes.append(dataset_axis)
        if histogram_config.add_syst_axis:
            axes.append(syst_axis)
        if histogram_config.add_weight:
            axes.append(hist.storage.Weight())
        histogram = hda.hist.Hist(*axes)
        return histogram