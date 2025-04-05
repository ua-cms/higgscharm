import gc
import copy
import yaml
import logging
import argparse
import pandas as pd
from pathlib import Path
from collections import defaultdict
from coffea.util import save, load
from coffea.processor import accumulate
from analysis.workflows.config import WorkflowConfigBuilder
from analysis.postprocess.coffea_plotter import CoffeaPlotter
from analysis.postprocess.utils import (
    print_header,
    setup_logger,
    clear_output_directory,
)
from analysis.postprocess.coffea_postprocessor import (
    save_process_histograms,
    load_processed_histograms,
    get_results_report,
    get_cutflow,
)

OUTPUT_DIR = Path.cwd() / "outputs"


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w",
        "--workflow",
        required=True,
        choices=["ztomumu", "ztoee", "zzto4l", "zplusl", "zplusll", "hww"],
        help="Workflow config to run",
    )
    parser.add_argument(
        "-y",
        "--year",
        dest="year",
        type=str,
        choices=[
            "2022",
            "2023",
            "2022preEE",
            "2022postEE",
            "2023preBPix",
            "2023postBPix",
        ],
        help="year of the data",
    )
    parser.add_argument(
        "--log",
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
    return parser.parse_args()


def build_process_sample_map(dataset_configs):
    process_map = defaultdict(list)
    for sample, config in dataset_configs.items():
        process_map[config["process"]].append(sample)
    return process_map


def load_year_histograms(workflow, year, output_format):
    aux_map = {
        "2022": ["2022preEE", "2022postEE"],
        "2023": ["2023preBPix", "2023postBPix"],
    }
    pre_year, post_year = aux_map[year]
    base_path = OUTPUT_DIR / workflow

    pre_file = (
        base_path
        / pre_year
        / f"{pre_year}_processed_histograms.{output_format}"
    )
    post_file = (
        base_path
        / post_year
        / f"{post_year}_processed_histograms.{output_format}"
    )
    return accumulate([load(pre_file), load(post_file)])


def load_histogram_file(path: Path):
    if not path.exists():
        return None
    return load(path)


if __name__ == "__main__":
    args = parse_arguments()

    output_dir = OUTPUT_DIR / args.workflow / args.year
    output_dir.mkdir(parents=True, exist_ok=True)

    clear_output_directory(str(output_dir), "txt")
    setup_logger(output_dir)

    config_builder = WorkflowConfigBuilder(workflow=args.workflow)
    workflow_config = config_builder.build_workflow_config()
    event_selection = workflow_config.event_selection
    categories = workflow_config.event_selection["categories"]
    processed_histograms = None

    if args.year in ["2022", "2023"]:
        processed_histograms = load_year_histograms(
            args.workflow, args.year, args.output_format
        )

    if args.postprocess:
        logging.info(workflow_config.to_yaml())

        fileset_path = Path.cwd() / "analysis/filesets" / f"{args.year}_nanov12.yaml"
        with open(fileset_path, "r") as f:
            dataset_configs = yaml.safe_load(f)

        process_samples_map = build_process_sample_map(dataset_configs)
        process_map = copy.deepcopy(process_samples_map)
        print_header(f"Reading outputs from: {output_dir}")

        for process in process_samples_map:
            p = save_process_histograms(
                year=args.year,
                output_dir=output_dir,
                process_samples_map=process_samples_map,
                process=process,
                categories=categories,
            )
            if not p:
                del process_map[process]
            gc.collect()
        processed_histograms = load_processed_histograms(
            year=args.year,
            output_dir=output_dir,
            process_samples_map=process_map,
        )

        for category in categories:
            logging.info(f"category: {category}")
            category_dir = Path(f"{output_dir}/{category}")

            print_header(f"Cutflow")
            cutflow_df = pd.DataFrame()
            for process in process_map:
                cutflow_file = category_dir / f"cutflow_{category}_{process}.csv"
                cutflow_df = pd.concat(
                    [cutflow_df, pd.read_csv(cutflow_file, index_col=[0])], axis=1
                )

            cutflow_df["Total Background"] = cutflow_df.drop(columns="Data").sum(axis=1)

            cutflow_index = ["initial"] + event_selection["categories"][category]
            cutflow_df = cutflow_df.loc[cutflow_index]

            cutflow_df = cutflow_df[
                ["Data", "Total Background"]
                + [
                    process
                    for process in cutflow_df.columns
                    if process not in ["Data", "Total Background"]
                ]
            ]
            logging.info(
                f'{cutflow_df.applymap(lambda x: f"{x:.3f}" if pd.notnull(x) else "")}\n'
            )
            cutflow_df.to_csv(f"{category_dir}/cutflow_{category}.csv")
            logging.info("\n")

            print_header(f"Results")
            results_df = get_results_report(processed_histograms, category)
            logging.info(
                results_df.applymap(lambda x: f"{x:.5f}" if pd.notnull(x) else "")
            )
            logging.info("\n")
            results_df.to_csv(f"{category_dir}/results_{category}.csv")

    if args.plot:
        if not args.postprocess and args.year not in ["2022", "2023"]:
            postprocess_file = (
                output_dir / f"{args.year}_processed_histograms.{args.output_format}"
            )
            processed_histograms = load_histogram_file(postprocess_file)
            if processed_histograms is None:
                cmd = f"python3 run_postprocess.py -w {args.workflow} -y {args.year} --postprocess"
                raise ValueError(
                    f"Postprocess file not found. Please run:\n  '{cmd}' first"
                )

        print_header("Plots")
        plotter = CoffeaPlotter(
            workflow=args.workflow,
            processed_histograms=processed_histograms,
            year=args.year,
            output_dir=output_dir,
        )

        for category in workflow_config.event_selection["categories"]:
            logging.info(f"Plotting histograms for category: {category}")
            for variable in workflow_config.histogram_config.variables:
                logging.info(variable)
                plotter.plot_histograms(
                    variable=variable,
                    category=category,
                    yratio_limits=args.yratio_limits,
                    log=args.log,
                    extension=args.extension,
                )
