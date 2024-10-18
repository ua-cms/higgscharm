import os
import glob
import yaml
import logging
import argparse
from analysis.utils import paths
from analysis.configs import load_config
from analysis.postprocess.plotter import Plotter
from analysis.postprocess.postprocessor import Postprocessor
from analysis.postprocess.utils import print_header, accumulate, setup_logger


def clear_output_directory(output_dir):
    """Delete all result files in the output directory."""
    extensions = ["*.csv", "*.txt", "*.png", "*.pdf", "*.yaml"]
    for ext in extensions:
        files = glob.glob(os.path.join(output_dir, ext))
        for file in files:
            os.remove(file)


def plot(args, processed_histograms, histograms_config, lumi, cat_axis=None):
    plotter = Plotter(
        processor=args.processor,
        processed_histograms=processed_histograms,
        year=args.year,
        lumi=lumi,
        cat_axis=cat_axis,
        output_dir=args.output_dir,
    )
    print_header("plotting histograms")
    for key, features in histograms_config.layout.items():
        for feature in features:
            logging.info(feature)
            plotter.plot_feature_hist(
                feature=feature,
                feature_label=histograms_config.axes[feature]["label"],
                yratio_limits=(0, 2),
                savefig=True,
            )


def main(args):
    if not args.output_dir:
        args.output_dir = paths.processor_path(
            processor=args.processor,
            year=args.year,
        )
    # delete previous results
    clear_output_directory(args.output_dir)

    # set up logger
    setup_logger(args.output_dir)

    # save processor config
    processor_config = load_config(
        config_type="processor", config_name=args.processor, year=args.year
    )
    with open(f"{args.output_dir}/config.yaml", "w") as file:
        file.write(processor_config.to_yaml())

    # process (group and accumulate) outputs
    postprocessor = Postprocessor(
        processor=args.processor,
        year=args.year,
        output_dir=args.output_dir,
    )
    processed_histograms = postprocessor.histograms
    lumi = postprocessor.luminosities["Total"]
    # plot histograms
    histograms_config = processor_config.histogram_config
    if histograms_config.add_cat_axis:
        for k in histograms_config.add_cat_axis:
            categories = histograms_config.add_cat_axis[k]["categories"] + [sum]
            for category in categories:
                logging.info(f"plotting {category} category of {k} axis")
                plot(args, processed_histograms, histograms_config, lumi, (k, category))
    else:
        plot(args, processed_histograms, histograms_config, lumi, None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ztoll",
        help="processor to be used {ztomumu}",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022EE",
        help="year of the data {2022, 2022EE, full2022}",
    )
    parser.add_argument(
        "--output_dir",
        dest="output_dir",
        type=str,
        default=None,
        help="Path to the outputs directory",
    )
    args = parser.parse_args()
    main(args)
