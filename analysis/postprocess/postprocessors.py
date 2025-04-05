import logging
from pathlib import Path
from coffea.util import save, load
from analysis.workflows.config import WorkflowConfigBuilder
from analysis.postprocess.root_plotter import ROOTPlotter
from analysis.postprocess.root_postprocessor import ROOTPostprocessor
from analysis.postprocess.utils import (
    print_header,
    setup_logger,
    clear_output_directory,
)


def root_postprocess(
    postprocess: bool,
    plot: bool,
    workflow: str,
    year: str,
    yratio_limits: tuple,
    log: bool,
    extension: str,
):
    # load workflow config
    config_builder = WorkflowConfigBuilder(workflow=workflow, year=year)
    workflow_config = config_builder.build_workflow_config()
    # do postprocessing for each selection category
    for category in workflow_config.event_selection["categories"]:
        output_dir = Path.cwd() / "outputs" / workflow / year / category
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
        clear_output_directory(str(output_dir), "txt")
        setup_logger(str(output_dir))
        if postprocess:
            logging.info(workflow_config.to_yaml())
            clear_output_directory(output_dir, "pkl")
            clear_output_directory(output_dir.parent, "root")
            postprocessor = ROOTPostworkflow(
                workflow=processor,
                year=year,
                category=category,
                output_dir=output_dir,
            )
            postprocessor.run_postprocess()
            processed_histograms = postprocessor.proccesed_histograms
            save(
                processed_histograms,
                f"{output_dir}/{category}_{workflow}_{year}_processed_histograms.coffea",
            )

        if plot:
            if not postprocess:
                postprocess_path = Path(
                    f"{output_dir}/{category}_{workflow}_{year}_processed_histograms.coffea"
                )
                if not postprocess_path.exists():
                    postprocess_cmd = f"python3 run_postprocess.py --workflow {workflow} --year {year} --output_format root --postprocess --plot"
                    raise ValueError(
                        f"Postprocess dict have not been generated. Please run '{postprocess_cmd}'"
                    )
                processed_histograms = load(postprocess_path)
            plotter = ROOTPlotter(
                workflow=workflow,
                year=year,
                processed_histograms=processed_histograms,
                output_dir=output_dir,
            )
            print_header("Plots")
            logging.info(f"plotting histograms for category: {category}")
            for variable in workflow_config.histogram_config.variables:
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
                        yratio_limits=yratio_limits,
                        log=log,
                        extension=extension,
                    )
