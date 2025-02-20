import logging
from pathlib import Path
from coffea.util import save, load
from analysis.configs import ProcessorConfigBuilder
from analysis.postprocess.root_plotter import ROOTPlotter
from analysis.postprocess.root_postprocessor import ROOTPostprocessor
from analysis.postprocess.coffea_plotter import CoffeaPlotter
from analysis.postprocess.coffea_postprocessor import CoffeaPostprocessor
from analysis.postprocess.utils import (
    print_header,
    setup_logger,
    clear_output_directory,
)


def coffea_postprocess(
    postprocess: bool,
    plot: bool,
    processor: str,
    year: str,
    yratio_limits: tuple,
    log_scale: bool,
    extension: str,
):
    # load and save processor config
    config_builder = ProcessorConfigBuilder(processor=processor, year=year)
    processor_config = config_builder.build_processor_config()

    output_dir = Path.cwd() / "outputs" / processor / year
    clear_output_directory(str(output_dir), "txt")
    setup_logger(output_dir)

    if postprocess:
        logging.info(processor_config.to_yaml())
        # process (group and accumulate) outputs
        postprocessor = CoffeaPostprocessor(
            processor=processor,
            year=year,
            output_dir=output_dir,
        )
        processed_histograms = postprocessor.histograms
        save(
            processed_histograms,
            f"{output_dir}/{processor}_{year}_processed_histograms.coffea",
        )

    if plot:
        if not postprocess:
            postprocess_path = Path(
                f"{output_dir}/{processor}_{year}_processed_histograms.coffea"
            )
            if not postprocess_path.exists():
                postprocess_cmd = f"python3 run_postprocess.py --processor {processor} --year {year} --output_format coffea --postprocess --plot"
                raise ValueError(
                    f"Postprocess dict have not been generated. Please run '{postprocess_cmd}'"
                )
            processed_histograms = load(postprocess_path)
        # plot processed histograms
        print_header("Plots")
        plotter = CoffeaPlotter(
            processor=processor,
            processed_histograms=processed_histograms,
            year=year,
            output_dir=output_dir,
        )
        for category in processor_config.event_selection["categories"]:
            logging.info(f"plotting histograms for category: {category}")
            for variable in processor_config.histogram_config.variables:
                logging.info(variable)
                plotter.plot_histograms(
                    variable=variable,
                    category=category,
                    yratio_limits=yratio_limits,
                    log_scale=log_scale,
                    extension=extension,
                )


def root_postprocess(
    postprocess: bool,
    plot: bool,
    processor: str,
    year: str,
    yratio_limits: tuple,
    log_scale: bool,
    extension: str,
):
    # load processor config
    config_builder = ProcessorConfigBuilder(processor=processor, year=year)
    processor_config = config_builder.build_processor_config()
    # do postprocessing for each selection category
    for category in processor_config.event_selection["categories"]:
        output_dir = Path.cwd() / "outputs" / processor / year / category
        if not output_dir.exists():
            output_dir.mkdir(parents=True, exist_ok=True)
        clear_output_directory(str(output_dir), "txt")
        setup_logger(str(output_dir))
        if postprocess:
            logging.info(processor_config.to_yaml())
            clear_output_directory(output_dir, "pkl")
            clear_output_directory(output_dir.parent, "root")
            postprocessor = ROOTPostprocessor(
                processor=processor,
                year=year,
                category=category,
                output_dir=output_dir,
            )
            postprocessor.run_postprocess()
            processed_histograms = postprocessor.proccesed_histograms
            save(
                processed_histograms,
                f"{output_dir}/{category}_{processor}_{year}_processed_histograms.coffea",
            )

        if plot:
            if not postprocess:
                postprocess_path = Path(
                    f"{output_dir}/{category}_{processor}_{year}_processed_histograms.coffea"
                )
                if not postprocess_path.exists():
                    postprocess_cmd = f"python3 run_postprocess.py --processor {processor} --year {year} --output_format root --postprocess --plot"
                    raise ValueError(
                        f"Postprocess dict have not been generated. Please run '{postprocess_cmd}'"
                    )
                processed_histograms = load(postprocess_path)
            plotter = ROOTPlotter(
                processor=processor,
                year=year,
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
                        yratio_limits=yratio_limits,
                        log_scale=log_scale,
                        extension=extension,
                    )
