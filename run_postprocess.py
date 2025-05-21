import gc
import yaml
import json
import glob
import logging
import argparse
import pandas as pd
from pathlib import Path
from collections import defaultdict
from coffea.util import load
from coffea.processor import accumulate
from analysis.workflows.config import WorkflowConfigBuilder
from analysis.postprocess.coffea_plotter import CoffeaPlotter
from analysis.postprocess.utils import (
    print_header,
    setup_logger,
    clear_output_directory,
)
from analysis.postprocess.coffea_postprocessor import (
    save_process_histograms_by_process,
    save_process_histograms_by_sample,
    load_processed_histograms,
    get_results_report,
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
        required=True,
        choices=[
            "2022",
            "2023",
            "2022preEE",
            "2022postEE",
            "2023preBPix",
            "2023postBPix",
        ],
        help="Year of the data",
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
        "--output_format",
        type=str,
        default="coffea",
        choices=["coffea", "root"],
        help="Format of output histograms",
    )
    parser.add_argument(
        "--group_by",
        type=str,
        default="process",
        help="Axis to group by (e.g., 'process', or a JSON dict)",
    )
    return parser.parse_args()


def get_sample_name(filename: str, year: str) -> str:
    """return sample name from filename"""
    sample_name = Path(filename).stem
    if sample_name.rsplit("_")[-1].isdigit():
        sample_name = "_".join(sample_name.rsplit("_")[:-1])
    return sample_name.replace(f"{year}_", "")


def build_process_sample_map(datasets: list[str], year: str) -> dict[str, list[str]]:
    """map processes to their corresponding samples based on dataset config"""
    fileset_path = Path.cwd() / "analysis/filesets" / f"{year}_nanov12.yaml"
    with open(fileset_path, "r") as f:
        dataset_configs = yaml.safe_load(f)

    process_map = defaultdict(list)
    for sample in datasets:
        config = dataset_configs[sample]
        process_map[config["process"]].append(sample)
    return process_map


def load_year_histograms(workflow: str, year: str, output_format: str):
    """load and merge histograms from pre/post campaigns"""
    aux_map = {
        "2022": ["2022preEE", "2022postEE"],
        "2023": ["2023preBPix", "2023postBPix"],
    }
    pre_year, post_year = aux_map[year]
    base_path = OUTPUT_DIR / workflow
    pre_file = base_path / pre_year / f"{pre_year}_processed_histograms.{output_format}"
    post_file = (
        base_path / post_year / f"{post_year}_processed_histograms.{output_format}"
    )
    return accumulate([load(pre_file), load(post_file)])


def load_histogram_file(path: Path):
    return load(path) if path.exists() else None


def plot_variable(variable: str, group_by, histogram_config) -> bool:
    """decide whether to plot a given variable under group_by mode"""
    if isinstance(group_by, str) and group_by == "process":
        return True
    for hist_key, variables in histogram_config.layout.items():
        if variable in variables and group_by["name"] in variables:
            return group_by["name"] != variable
    return False


def save_cutflow_report(
    category: str, category_dir: Path, event_selection: dict, process_samples_map: dict
):
    """generate and save the cutflow table for a given category"""
    print_header("Cutflow")
    cutflow_df = pd.DataFrame()

    for process in process_samples_map:
        cutflow_file = category_dir / f"cutflow_{category}_{process}.csv"
        if cutflow_file.exists():
            df = pd.read_csv(cutflow_file, index_col=0)
            cutflow_df = pd.concat([cutflow_df, df], axis=1)
        else:
            logging.warning(f"Missing cutflow file: {cutflow_file}")

    if "Data" in cutflow_df.columns:
        cutflow_df["Total Background"] = cutflow_df.drop(columns="Data").sum(axis=1)
    else:
        cutflow_df["Total Background"] = cutflow_df.sum(axis=1)

    cutflow_index = ["initial"] + event_selection["categories"][category]
    cutflow_df = cutflow_df.loc[cutflow_index]

    ordered_cols = ["Data", "Total Background"] + [
        col for col in cutflow_df.columns if col not in ["Data", "Total Background"]
    ]
    cutflow_df = cutflow_df[ordered_cols]

    logging.info(
        f'{cutflow_df.applymap(lambda x: f"{x:.3f}" if pd.notnull(x) else "")}\n'
    )
    cutflow_df.to_csv(category_dir / f"cutflow_{category}.csv")


def save_results_report(category: str, category_dir: Path, processed_histograms: dict):
    """generate and save the results summary table for a given category"""
    print_header("Results")
    results_df = get_results_report(processed_histograms, category)
    logging.info(results_df.applymap(lambda x: f"{x:.5f}" if pd.notnull(x) else ""))
    logging.info("\n")
    results_df.to_csv(category_dir / f"results_{category}.csv")


if __name__ == "__main__":
    args = parse_arguments()

    try:
        group_by = json.loads(args.group_by)
    except json.JSONDecodeError:
        group_by = args.group_by

    output_dir = OUTPUT_DIR / args.workflow / args.year
    output_dir.mkdir(parents=True, exist_ok=True)

    clear_output_directory(output_dir, "txt")
    setup_logger(output_dir)

    config_builder = WorkflowConfigBuilder(workflow=args.workflow)
    workflow_config = config_builder.build_workflow_config()
    event_selection = workflow_config.event_selection
    categories = event_selection["categories"]
    processed_histograms = None

    if args.year in ["2022", "2023"]:
        processed_histograms = load_year_histograms(
            args.workflow, args.year, args.output_format
        )

    if args.postprocess:
        logging.info(workflow_config.to_yaml())
        print_header(f"Reading outputs from: {output_dir}")

        output_files = [
            f
            for f in glob.glob(f"{output_dir}/*/*{args.output_format}", recursive=True)
            if not Path(f).stem.startswith("cutflow")
        ]

        grouped_outputs = defaultdict(list)
        for output_file in output_files:
            sample_name = get_sample_name(output_file, args.year)
            grouped_outputs[sample_name].append(output_file)

        process_samples_map = build_process_sample_map(
            grouped_outputs.keys(), args.year
        )

        for sample in grouped_outputs:
            save_process_histograms_by_sample(
                year=args.year,
                output_dir=output_dir,
                sample=sample,
                grouped_outputs=grouped_outputs,
                categories=categories,
            )
            gc.collect()

        for process in process_samples_map:
            save_process_histograms_by_process(
                year=args.year,
                output_dir=output_dir,
                process_samples_map=process_samples_map,
                process=process,
                categories=categories,
            )
            gc.collect()

        processed_histograms = load_processed_histograms(
            year=args.year,
            output_dir=output_dir,
            process_samples_map=process_samples_map,
        )

        for category in categories:
            logging.info(f"category: {category}")
            category_dir = output_dir / category
            save_cutflow_report(
                category, category_dir, event_selection, process_samples_map
            )
            save_results_report(category, category_dir, processed_histograms)

    if args.plot:
        if not args.postprocess and args.year not in ["2022", "2023"]:
            postprocess_file = (
                output_dir / f"{args.year}_processed_histograms.{args.output_format}"
            )
            processed_histograms = load_histogram_file(postprocess_file)
            if processed_histograms is None:
                raise ValueError(
                    f"Postprocess file not found. Please run:\n"
                    f"  'python3 run_postprocess.py -w {args.workflow} -y {args.year} --postprocess'"
                )

        print_header("Plots")
        plotter = CoffeaPlotter(
            workflow=args.workflow,
            processed_histograms=processed_histograms,
            year=args.year,
            output_dir=output_dir,
            group_by=group_by,
        )

        for category in categories:
            logging.info(f"Plotting histograms for category: {category}")
            for variable in workflow_config.histogram_config.variables:
                if plot_variable(variable, group_by, workflow_config.histogram_config):
                    logging.info(variable)
                    plotter.plot_histograms(
                        variable=variable,
                        category=category,
                        yratio_limits=args.yratio_limits,
                        log=args.log,
                        extension=args.extension,
                    )
