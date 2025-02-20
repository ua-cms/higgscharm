import argparse
from analysis.postprocess import coffea_postprocess, root_postprocess

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
        choices=["2022preEE", "2022postEE"],
        help="year of the data",
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
        "--extension",
        dest="extension",
        type=str,
        default="pdf",
        choices=["pdf", "png"],
        help="extension to be used for plotting",
    )
    parser.add_argument(
        "--output_format",
        type=str,
        default="coffea",
        choices=["coffea", "root"],
        help="format of output histograms",
    )
    args = parser.parse_args()

    if args.output_format == "coffea":
        coffea_postprocess(
            processor=args.processor,
            year=args.year,
            yratio_limits=args.yratio_limits,
            log_scale=args.log_scale,
            extension=args.extension,
            postprocess=args.postprocess,
            plot=args.plot,
        )
    elif args.output_format == "root":
        root_postprocess(
            processor=args.processor,
            year=args.year,
            yratio_limits=args.yratio_limits,
            log_scale=args.log_scale,
            extension=args.extension,
            postprocess=args.postprocess,
            plot=args.plot,
        )
