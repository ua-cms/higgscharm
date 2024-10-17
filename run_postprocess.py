import argparse
from analysis.utils import paths
from analysis.configs import load_config
from analysis.postprocess.plotter import Plotter
from analysis.postprocess.postprocessor import Postprocessor
from analysis.postprocess.utils import print_header, accumulate


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
            print(feature)
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
            tagger=args.tagger,
            flavor=args.flavor,
            wp=args.wp,
            year=args.year,
            lepton_flavor=args.lepton_flavor,
        )
    if args.year != "full2022":
        postprocessor = Postprocessor(
            processor=args.processor,
            tagger=args.tagger,
            flavor=args.flavor,
            wp=args.wp,
            year=args.year,
            output_dir=args.output_dir,
            lepton_flavor=args.lepton_flavor,
        )
        processed_histograms = postprocessor.histograms
        lumi = postprocessor.luminosities["Total"]
    else:
        lumi = 0
        pre_processed_histograms = {}
        for year in ["2022", "2022EE"]:
            postprocessor = Postprocessor(
                processor=args.processor,
                tagger=args.tagger,
                flavor=args.flavor,
                wp=args.wp,
                year=year,
                output_dir=args.output_dir,
                lepton_flavor=args.lepton_flavor,
            )
            pre_processed_histograms[year] = postprocessor.histograms
            lumi += postprocessor.luminosities["Total"]

        accumulated_processed_histograms = {}
        for sample in pre_processed_histograms["2022"]:
            accumulated_processed_histograms[sample] = {}
            for feature in pre_processed_histograms["2022"][sample]:
                accumulated_processed_histograms[sample][feature] = (
                    pre_processed_histograms["2022"][sample][feature]
                    + pre_processed_histograms["2022EE"][sample][feature]
                )
        processed_histograms = accumulated_processed_histograms

    processor_config = load_config(
        config_type="processor", config_name=args.processor, year=args.year
    )
    histograms_config = processor_config.histogram_config
    if histograms_config.add_cat_axis:
        for k in histograms_config.add_cat_axis:
            categories = histograms_config.add_cat_axis[k]["categories"] + [sum]
            for category in categories:
                print(f"plotting {category} category of {k} axis")
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
