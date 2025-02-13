import pickle
import logging
import argparse
from pathlib import Path
from analysis.configs import ProcessorConfigBuilder
from analysis.postprocess.plotter import Plotter
from analysis.postprocess.postprocessor import Postprocessor
from analysis.postprocess.utils import (
    print_header,
    setup_logger,
    clear_output_directory,
)


def main(args):
    # load processor config
    config_builder = ProcessorConfigBuilder(processor=args.processor, year=args.year)
    processor_config = config_builder.build_processor_config()
    # do postprocessing for each selection category
    for category in processor_config.event_selection["categories"]:
        output_dir = Path.cwd() / "outputs" / args.processor / args.year / category
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
        clear_output_directory(str(output_dir), "txt")
        setup_logger(str(output_dir))
        if args.postprocess:
            logging.info(processor_config.to_yaml())
            clear_output_directory(output_dir, "pkl")
            clear_output_directory(output_dir.parent, "root")
            postprocessor = Postprocessor(
                processor=args.processor,
                year=args.year,
                category=category,
                output_dir=output_dir,
            )
            postprocessor.run_postprocess()
            processed_histograms = postprocessor.proccesed_histograms
            with open(
                f"{output_dir}/{category}_{args.processor}_{args.year}_processed_histograms.pkl",
                "wb",
            ) as handle:
                pickle.dump(
                    processed_histograms, handle, protocol=pickle.HIGHEST_PROTOCOL
                )

        if args.plot:
            if not args.postprocess:
                processed_histograms = pickle.load(
                    open(
                        f"{output_dir}/{category}_{args.processor}_{args.year}_processed_histograms.pkl",
                        "rb",
                    )
                )
            plotter = Plotter(
                processor=args.processor,
                year=args.year,
                processed_histograms=processed_histograms,
                output_dir=output_dir,
            )
            print_header("Plots")
            logging.info(f"plotting histograms for category: {category}")
            for variable in processor_config.histogram_config.variables:
                has_variable = False
                for v in processed_histograms["Data"]:
                    if variable in v:
                        has_variable = True
                        break
                if has_variable:
                    logging.info(variable)
                    plotter.plot_histograms(
                        variable=variable,
                        category=category,
                        yratio_limits=args.yratio_limits,
                        log_scale=args.log_scale,
                        savefig=args.savefig,
                        format=args.format,
                    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        help="processor to be used {ztomumu, ztoee, zzto4l, hww}",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
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
        "--postprocess",
        action="store_true",
        help="Enable postprocessing",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Enable plotting",
    )
    parser.add_argument(
        "--savefig",
        action="store_true",
        help="Enable plot saving",
    )
    parser.add_argument(
        "--format",
        dest="format",
        type=str,
        default="pdf",
        help="extension to be used for plotting {png, pdf}",
    )
    args = parser.parse_args()
    main(args)