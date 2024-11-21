import yaml
import logging
import argparse
from analysis.utils import paths
from analysis.postprocess.plotter import Plotter
from analysis.configs import ProcessorConfigBuilder
from analysis.postprocess.postprocessor import Postprocessor
from analysis.postprocess.utils import (
    print_header,
    setup_logger,
    clear_output_directory,
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
    # load processor config
    config_builder = ProcessorConfigBuilder(processor=args.processor, year=args.year)
    processor_config = config_builder.build_processor_config()
    # get categories
    categories = processor_config.event_selection["categories"]
    for category in categories:
        clear_output_directory(f"{args.output_dir}/{category}")
    # save processor config
    logging.info(processor_config.to_yaml())
    # process (group and accumulate) outputs
    postprocessor = Postprocessor(
        processor=args.processor,
        year=args.year,
        output_dir=args.output_dir,
    )
    processed_histograms = postprocessor.histograms
    lumi = postprocessor.luminosities["Total"]
    # initialize plotter
    plotter = Plotter(
        processor=args.processor,
        processed_histograms=processed_histograms,
        year=args.year,
        lumi=lumi,
        output_dir=args.output_dir,
    )
    # get histogram config
    histogram_config = processor_config.histogram_config
    # get variables to plot
    variables = []
    if histogram_config.layout == "individual":
        variables = list(histogram_config.axes.keys())
    else:
        variables = []
        for key, values in histogram_config.layout.items():
            for v in values:
                variables.append(v)
    # plot histograms
    print_header("Plots")
    for category in categories:
        logging.info(f"plotting histograms for category: {category}")
        for variable in variables:
            logging.info(variable)
            plotter.plot_histograms(
                variable=variable,
                category=category,
                yratio_limits=args.yratio_limits,
                log_scale=args.log_scale,
                savefig=True,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ztomumu",
        help="processor to be used {ztomumu, ztoee}",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022postEE",
        help="year of the data {2022preEE, 2022postEE}",
    )
    parser.add_argument(
        "--log_scale",
        action="store_true",
        help="Enable log scale for y-axis",
    )
    parser.add_argument(
        "--yratio_limits",
        dest="yratio_limits",
        type=float,
        nargs=2,
        default=(0.5, 1.5),
        help="Set y-axis ratio limits as a tuple (e.g., --yratio_limits 0 2)",
    )
    parser.add_argument(
        "--output_dir",
        dest="output_dir",
        type=str,
        default=None,
        help="Path to the outputs directory (optional)",
    )
    args = parser.parse_args()
    main(args)
