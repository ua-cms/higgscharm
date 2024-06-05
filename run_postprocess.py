import argparse
from analysis.configs import load_config
from analysis.postprocess.plotter import Plotter
from analysis.postprocess.utils import print_header
from analysis.postprocess.postprocessor import Postprocessor


def main(args):
    # process output histograms
    postprocessor = Postprocessor(
        processor=args.processor,
        tagger=args.tagger,
        flavor=args.flavor,
        wp=args.wp,
        year=args.year,
        output_dir=args.output_dir,
    )
    processed_histograms = postprocessor.process_histograms()
    # plot histograms
    plotter = Plotter(
        processor=args.processor,
        processed_histograms=processed_histograms,
        tagger=args.tagger,
        flavor=args.flavor,
        wp=args.wp,
        year=args.year,
        lumi=postprocessor.lumi,
    )
    histograms_config = load_config(config_type="histogram", config_name=args.processor)
    print_header("plotting histograms")
    for key, features in histograms_config.layout.items():
        for feature in features:
            print(feature)
            plotter.plot_feature_hist(
                feature=feature,
                feature_label=histograms_config.axes[feature]["label"],
                yratio_limits=(0, 2),
                savefig=True,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--processor",
        dest="processor",
        type=str,
        default="ztomumu",
        help="processor to be used {signal, tag_eff, taggers, zplusjet}",
    )
    parser.add_argument(
        "--year",
        dest="year",
        type=str,
        default="2022EE",
        help="year of the data {2022EE}",
    )
    parser.add_argument(
        "--tagger",
        dest="tagger",
        type=str,
        default=None,
        help="tagger {pnet, part, deepjet}",
    )
    parser.add_argument(
        "--wp",
        dest="wp",
        type=str,
        default=None,
        help="working point {loose, medium, tight}",
    )
    parser.add_argument(
        "--flavor",
        dest="flavor",
        type=str,
        default=None,
        help="Hadron flavor {c, b}",
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