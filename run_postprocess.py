import yaml
import json
import glob
import logging
import argparse
import subprocess
import pandas as pd
import awkward as ak
from pathlib import Path
from collections import defaultdict
from coffea.util import load, save
from coffea.processor import accumulate
from analysis.postprocess.plotter import CoffeaPlotter
from analysis.workflows.config import WorkflowConfigBuilder
from analysis.filesets.utils import (
    get_workflow_key_process_map,
    get_process_sample_map,
    get_dataset_config,
    get_datasets_map,
)
from analysis.postprocess.utils import (
    print_header,
    setup_logger,
    clear_output_directory,
    df_to_latex,
    combine_event_tables,
    combine_cutflows,
    format_cutflow_with_efficiency,
    merge_parquets_by_sample,
    get_results_report,
)
from analysis.utils import make_output_directory


def parse_arguments():
    """Parse workflow, campaign, postprocessing, and plotting options."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w",
        "--workflow",
        dest="workflow",
        required=True,
        type=str,
        choices=[
            f.stem for f in (Path.cwd() / "analysis" / "workflows").glob("*.yaml")
        ],
        help="workflow to run",
    )
    parser.add_argument(
        "-y",
        "--year",
        required=True,
        choices=[
            "2016preVFP",
            "2016postVFP",
            "2016",
            "2017",
            "2018",
            "2022preEE",
            "2022postEE",
            "2022",
            "2023preBPix",
            "2023postBPix",
            "2023",
            "2024",
        ],
        help="Data year",
    )
    parser.add_argument(
        "--log", action="store_true", help="Enable log scale for y-axis"
    )
    parser.add_argument(
        "--postprocess", action="store_true", help="Enable postprocessing"
    )
    parser.add_argument("--plot", action="store_true", help="Enable plotting")
    parser.add_argument(
        "--yratio_limits",
        type=float,
        nargs=2,
        default=(0.5, 1.5),
        help="Set y-axis ratio limits",
    )
    parser.add_argument(
        "--extension",
        type=str,
        default="pdf",
        choices=["pdf", "png"],
        help="Output file extension for plots",
    )
    parser.add_argument(
        "--group_by",
        type=str,
        default="process",
        help="Axis to group by (e.g., 'process', or a JSON dict)",
    )
    parser.add_argument(
        "--pass_axis",
        type=str,
        default=None,
        help="Binary axis (e.g., 'is_passing_lepton')",
    )
    parser.add_argument(
        "--nocutflow", action="store_true", help="Enable postprocessing"
    )
    parser.add_argument(
        "--output_format",
        type=str,
        default="coffea",
        choices=["coffea", "parquet"],
        help="Format of output files",
    )
    parser.add_argument(
        "--skipmerging", action="store_true", help="Skip parquet outputs merging"
    )
    parser.add_argument("--blind", action="store_true", help="Blind data")
    parser.add_argument(
        "--save",
        action="store_true",
        help="Enable saving of sample/process sample histograms",
    )
    return parser.parse_args()


def get_sample_name(filename: str, year: str) -> str:
    """Extract the dataset sample name, removing a partition suffix and year prefix."""
    sample_name = Path(filename).stem
    if sample_name.rsplit("_")[-1].isdigit():
        sample_name = "_".join(sample_name.rsplit("_")[:-1])
    return sample_name.replace(f"{year}_", "")


def load_year_histograms(year: str, output_dir: Path):
    """Combine already processed histograms from both sub-campaigns of a year."""
    aux_map = {
        "2016": ["2016preVFP", "2016postVFP"],
        "2022": ["2022preEE", "2022postEE"],
        "2023": ["2023preBPix", "2023postBPix"],
    }
    pre_year, post_year = aux_map[year]
    pre_file = output_dir.parent / pre_year / f"{pre_year}_processed_histograms.coffea"
    post_file = (
        output_dir.parent / post_year / f"{post_year}_processed_histograms.coffea"
    )
    return accumulate([load(pre_file), load(post_file)])


def load_histogram_file(path: Path):
    """Read a Coffea histogram file when it exists, otherwise return None."""
    return load(path) if path.exists() else None


def plot_variable(variable: str, group_by, histogram_config) -> bool:
    """Check whether a histogram contains the requested grouping axis."""
    if isinstance(group_by, str) and group_by == "process":
        return True
    for hist_key, variables in histogram_config.layout.items():
        if variable in variables and group_by["name"] in variables:
            return group_by["name"] != variable
    return False


if __name__ == "__main__":
    args = parse_arguments()

    # Accept either the default process grouping or a JSON configuration
    # describing another histogram axis used for grouping.
    try:
        group_by = json.loads(args.group_by)
    except json.JSONDecodeError:
        group_by = args.group_by

    # Prepare the output directory and redirect subsequent progress messages
    # to the configured logger.
    output_dir = make_output_directory(args)
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)

    clear_output_directory(output_dir, "txt")
    setup_logger(output_dir)

    # Load the workflow definition, including dataset groups, selection
    # categories, and histogram axes shared by all processing stages.
    config_builder = WorkflowConfigBuilder(workflow=args.workflow)
    workflow_config = config_builder.build_workflow_config()
    histogram_config = workflow_config.histogram_config
    event_selection = workflow_config.event_selection
    categories = event_selection["categories"]
    processed_histograms = None

    # Workflows without collision data must not attempt unblinded results.
    if "data" not in workflow_config.datasets:
        args.blind = True

    # Process individual campaigns first. Combined years are handled below
    # using the outputs of their pre/post sub-campaigns.
    if args.postprocess and (args.year not in ["2016", "2022", "2023"]):
        print_header(f"Running postprocess for {args.year}")
        logging.info(f"Reading outputs from: {output_dir}")

        # Find partition-level Coffea outputs, excluding derived cutflow
        # and processed files to avoid counting them a second time.
        output_files = [
            f
            for f in glob.glob(f"{output_dir}/*/*.coffea", recursive=True)
            if not Path(f).stem.startswith("cutflow")
            and not Path(f).stem.startswith("processed")
        ]

        # Associate each partition with its original sample, then determine
        # which physics process each sample contributes to.
        grouped_outputs = defaultdict(list)
        for output_file in output_files:
            sample_name = get_sample_name(output_file, args.year)
            grouped_outputs[sample_name].append(output_file)

        process_samples_map = get_process_sample_map(grouped_outputs.keys(), args.year)

        # Parquet event records are merged separately from Coffea histograms.
        if args.output_format == "parquet":
            if not args.skipmerging:
                merge_parquets_by_sample(output_dir, args.year, categories)

        # Merge all partition-level accumulators belonging to the same
        # sample before applying a single normalization factor.
        print_header("Grouping outputs by sample")
        outputs = {}
        for sample, coffea_files in grouped_outputs.items():
            logging.info(f"Processing {sample}")
            for coffea_file in coffea_files:
                output = load(coffea_file)
                if output:
                    if sample not in outputs:
                        outputs[sample] = output
                    else:
                        outputs[sample] = accumulate([outputs[sample], output])

        # Read the integrated luminosity used to normalize simulated event
        # yields to the expected number of events in data.
        lumi_file = Path.cwd() / "analysis" / "postprocess" / "luminosity.yaml"
        with open(lumi_file, "r") as f:
            luminosities = yaml.safe_load(f)

        print_header(
            f"Scaling outputs to lumi-xsec (Luminosity: {luminosities[args.year]} [1/pb])"
        )

        scaled_histograms = {}
        scaled_cutflows = {}
        dataset_config = get_dataset_config(args.year)

        for sample in outputs:

            scaled_histograms[sample] = {}
            scaled_cutflows[sample] = {}

            # Data retains unit weight; simulated samples are normalized by
            # integrated luminosity times cross section divided by sum of weights.
            weight = 1
            xsec = dataset_config[sample]["xsec"]
            sumw = outputs[sample]["metadata"]["sumw"]
            if dataset_config[sample]["era"] in ["mc", "signal"]:
                weight = (luminosities[args.year] * xsec) / sumw

            # Apply the sample normalization consistently to every histogram.
            report_str = f"{sample} xsec [pb]: {xsec} sumw: {sumw} weight: {weight}"
            logging.info(report_str)
            histograms_to_scale = outputs[sample]["histograms"]
            for entry in histograms_to_scale:
                scaled_histograms[sample][entry] = histograms_to_scale[entry] * weight

            if args.save:
                save(scaled_histograms[sample], f"{output_dir}/{sample}.coffea")

            # Normalize cutflow yields with the same sample weight so the
            # selection tables and histograms use compatible event yields.
            for category in categories:
                scaled_cutflows[sample][category] = {}
                if category in outputs[sample]["metadata"]:
                    for cut, nevents in outputs[sample]["metadata"][category][
                        "cutflow"
                    ].items():
                        scaled_cutflows[sample][category][cut] = nevents * weight

        # Optionally save aggregates for each dataset key defined in the
        # workflow, in addition to the individual sample files.
        if args.save:
            print_header("Grouping histograms by key")
            dataset_map = get_datasets_map(args.year)

            scale_histograms_by_key = {}
            workflow_keys = workflow_config.datasets.values()
            for dataset_key in dataset_map:
                if dataset_key not in ak.flatten(workflow_keys):
                    continue
                logging.info(f"Processing {dataset_key}")
                scale_histograms_by_key[dataset_key] = []
                for sample in scaled_histograms:
                    if sample in dataset_map[dataset_key]:
                        scale_histograms_by_key[dataset_key].append(
                            scaled_histograms[sample]
                        )

                save(
                    accumulate(scale_histograms_by_key[dataset_key]),
                    f"{output_dir}/{dataset_key}.coffea",
                )

        # Combine normalized samples into physics processes used in plots
        # and summary tables, while keeping category-specific cutflows.
        print_header("Grouping histograms by process")
        processed_histograms = {}
        processed_cutflows = {}
        for process, samples in process_samples_map.items():
            logging.info(f"Processing {process}")
            for sample in samples:

                # Add the current sample to the process histogram accumulator.
                if sample in outputs:
                    if process not in processed_histograms:
                        processed_histograms[process] = scaled_histograms[sample]
                    else:
                        processed_histograms[process] = accumulate(
                            [processed_histograms[process], scaled_histograms[sample]]
                        )

            # Add the normalized selection yields of all samples belonging
            # to this process, independently for each analysis category.
            processed_cutflows[process] = {}
            for category in categories:
                processed_cutflows[process][category] = {}
                for sample in process_samples_map[process]:
                    if sample in outputs:
                        if process not in processed_cutflows:
                            processed_cutflows[process][category] = scaled_cutflows[
                                sample
                            ][category]
                        else:
                            processed_cutflows[process][category] = accumulate(
                                [
                                    processed_cutflows[process][category],
                                    scaled_cutflows[sample][category],
                                ]
                            )
            if args.save:
                save(processed_histograms[process], f"{output_dir}/{process}.coffea")

        save(
            processed_histograms,
            f"{output_dir}/{args.year}_processed_histograms.coffea",
        )

        # Build per-category tables from the process-level cutflows and
        # optionally compute workflow-specific event yield reports.
        for category in categories:
            if not args.nocutflow:
                logging.info(f"category: {category}")

                category_dir = output_dir / str(category)
                if not category_dir.exists():
                    category_dir.mkdir(parents=True, exist_ok=True)

                print_header(f"Cutflow")
                cutflow_df = pd.DataFrame()
                for process in process_samples_map:
                    process_cutflow_df = pd.DataFrame(
                        processed_cutflows[process][category], index=[process]
                    ).T
                    cutflow_df = pd.concat([cutflow_df, process_cutflow_df], axis=1)

                # Exclude signals and (when unblinded) observed data from
                # the sum used to construct the total expected background.
                columns_to_drop = []
                key_process_map = get_workflow_key_process_map(
                    workflow_config, args.year
                )
                if "signal" in workflow_config.datasets:
                    signal_keys = [k for k in workflow_config.datasets["signal"]]
                    signals = [key_process_map[key] for key in signal_keys]
                    columns_to_drop += signals

                if not args.blind:
                    columns_to_drop += ["Data"]

                total_background = cutflow_df.drop(columns=columns_to_drop).sum(axis=1)
                cutflow_df["Total Background"] = total_background

                # Preserve the selection-step order specified by the workflow.
                cutflow_index = event_selection["categories"][category]
                cutflow_df = cutflow_df.loc[cutflow_index]

                if not args.blind:
                    to_process = ["Data", "Total Background"]
                else:
                    to_process = ["Total Background"]
                cutflow_df = cutflow_df[
                    to_process
                    + [
                        process
                        for process in cutflow_df.columns
                        if process not in to_process
                    ]
                ]
                logging.info(
                    f'{cutflow_df.applymap(lambda x: f"{x:.3f}" if pd.notnull(x) else "")}\n'
                )
                cutflow_df.to_csv(f"{category_dir}/cutflow_{category}.csv")
                logging.info("\n")

            # The Z-to-lepton workflows also provide a results summary
            # with a CSV export and a LaTeX-ready text table.
            if args.workflow in ["ztoee", "ztomumu"]:
                print_header(f"Results")
                results_df = get_results_report(
                    processed_histograms,
                    workflow_config,
                    category,
                    columns_to_drop,
                    args.blind,
                )
                logging.info(
                    results_df.applymap(lambda x: f"{x:.5f}" if pd.notnull(x) else "")
                )
                logging.info("\n")
                results_df.to_csv(f"{category_dir}/results_{category}.csv")

                # Export a formatted LaTeX table alongside the CSV results.
                latex_table = df_to_latex(results_df, args.blind)
                with open(category_dir / f"results_{category}.txt", "w") as f:
                    f.write(latex_table)

    # Combined-year requests reuse the processed histograms and tabular
    # results of both sub-campaigns instead of reading raw partitions again.
    if args.year in ["2016", "2022", "2023"]:
        if args.postprocess:
            print_header(f"Running postprocess for {args.year}")
            # Sum the already normalized histograms from both campaigns.
            processed_histograms = load_year_histograms(args.year, output_dir)
            save(
                processed_histograms,
                f"{output_dir}/{args.year}_processed_histograms.coffea",
            )
        else:
            postprocess_file = output_dir / f"{args.year}_processed_histograms.coffea"
            if not postprocess_file.exists():
                cmd = f"python3 run_postprocess.py -w {args.workflow} -y {args.year} --postprocess"
                raise ValueError(
                    f"Postprocess file not found. Please run:\n  '{cmd}' first"
                )

            processed_histograms = load(str(postprocess_file))

        identifier_map = {"2016": "VFP", "2022": "EE", "2023": "BPix"}
        identifier = identifier_map[args.year]

        for category in categories:
            logging.info(f"category: {category}")
            category_dir = output_dir / str(category)
            if not category_dir.exists():
                category_dir.mkdir(parents=True, exist_ok=True)
            # The Z-to-lepton workflows also provide a results summary
            # with a CSV export and a LaTeX-ready text table.
            if args.workflow in ["ztoee", "ztomumu"]:
                # Combine the pre/post campaign event yield reports into a
                # single table for the requested full year.
                results_pre = pd.read_csv(
                    output_dir.parent
                    / f"{args.year}pre{identifier}"
                    / category
                    / f"results_{category}.csv",
                    index_col=0,
                )
                results_post = pd.read_csv(
                    output_dir.parent
                    / f"{args.year}post{identifier}"
                    / category
                    / f"results_{category}.csv",
                    index_col=0,
                )
                combined_results = combine_event_tables(
                    results_pre, results_post, args.blind
                )

                print_header(f"Results")
                logging.info(
                    combined_results.applymap(
                        lambda x: f"{x:.5f}" if pd.notnull(x) else ""
                    )
                )
                logging.info("\n")

                combined_results.to_csv(category_dir / f"results_{category}.csv")

                # Export a formatted LaTeX table alongside the CSV results.
                latex_table = df_to_latex(combined_results, args.blind)
                with open(category_dir / f"results_{category}.txt", "w") as f:
                    f.write(latex_table)

            # Combine corresponding selection steps from both campaigns
            # before calculating full-year selection efficiencies.
            if not args.nocutflow:
                print_header(f"Cutflow")
                cutflow_pre = pd.read_csv(
                    output_dir.parent
                    / f"{args.year}pre{identifier}"
                    / category
                    / f"cutflow_{category}.csv",
                    index_col=0,
                )
                cutflow_post = pd.read_csv(
                    output_dir.parent
                    / f"{args.year}post{identifier}"
                    / category
                    / f"cutflow_{category}.csv",
                    index_col=0,
                )
                combined_cutflow = combine_cutflows(cutflow_pre, cutflow_post)
                combined_cutflow.to_csv(category_dir / f"cutflow_{category}.csv")
                logging.info(
                    combined_cutflow.applymap(
                        lambda x: f"{x:.2f}" if pd.notnull(x) else ""
                    )
                )
                logging.info("\n")

                # Express each cutflow step as a percentage of the first
                # selection step, separately for every process column.
                print_header(f"Efficiency")
                eff_df = pd.DataFrame(index=combined_cutflow.index)
                for col in combined_cutflow.columns:
                    eff_df[col] = (
                        combined_cutflow[col] / combined_cutflow[col].iloc[0] * 100
                    )
                eff_df.to_csv(category_dir / f"eff_{category}.csv")
                logging.info(eff_df)
                logging.info("\n")

                cutflow_eff = format_cutflow_with_efficiency(combined_cutflow, eff_df)
                cutflow_eff.to_csv(category_dir / f"cutflow_eff_{category}.csv")

    # Plotting can run after postprocessing or load an existing processed
    # histogram file when only --plot was requested.
    if args.plot:
        subprocess.run("python3 analysis/postprocess/build_color_map.py", shell=True)
        if not args.postprocess:
            postprocess_file = output_dir / f"{args.year}_processed_histograms.coffea"
            processed_histograms = load_histogram_file(postprocess_file)
            if processed_histograms is None:
                raise ValueError(
                    f"Postprocess file not found. Please run:\n"
                    f"  'python3 run_postprocess.py -w {args.workflow} -y {args.year} --postprocess'"
                )

        print_header(f"Running plotter for {args.year}")
        plotter = CoffeaPlotter(
            workflow=args.workflow,
            processed_histograms=processed_histograms,
            year=args.year,
            output_dir=output_dir,
            group_by=group_by,
            pass_axis=args.pass_axis,
        )
        # Iterate over categories and configured variables, skipping axes
        # incompatible with the requested pass/fail or grouping dimensions.
        for category in categories:
            logging.info(
                f"Plotting histograms by '{group_by if group_by == 'process' else group_by['name']}' for category '{category}'"
            )
            for variable in workflow_config.histogram_config.variables:
                if args.pass_axis:
                    if variable == args.pass_axis:
                        continue
                    if histogram_config.layout == "individual":
                        print("There's only individual axes!")
                        break
                    proceed = False
                    for key, variables in histogram_config.layout.items():
                        if (variable in variables) and (args.pass_axis in variables):
                            proceed = True
                            break
                    if not proceed:
                        continue
                if plot_variable(variable, group_by, workflow_config.histogram_config):
                    logging.info(variable)
                    plotter.plot_histograms(
                        variable=variable,
                        category=category,
                        yratio_limits=args.yratio_limits,
                        log=args.log,
                        extension=args.extension,
                        blind=args.blind,
                    )
            # Fake-rate workflows require an additional specialized plot.
            if args.workflow.startswith("zplusl_"):
                plotter.plot_fake_rate(category)
            # Bundle the generated figures for convenient transfer or review.
            subprocess.run(
                f"tar -zcvf {output_dir}/{category}/{args.workflow}_{args.year}_plots.tar.gz {output_dir}/{category}/*.{args.extension}",
                shell=True,
            )
