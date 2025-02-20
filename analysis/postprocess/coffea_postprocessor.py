import yaml
import glob
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from coffea.util import load
from coffea.processor import accumulate
from analysis.configs import ProcessorConfigBuilder
from analysis.postprocess.utils import print_header, df_to_latex


class CoffeaPostprocessor:
    def __init__(
        self,
        processor: str,
        year: str,
        output_dir: str,
    ):
        self.processor = processor
        self.year = year
        self.output_dir = output_dir

        # get datasets configs
        main_dir = Path.cwd()
        fileset_path = Path(f"{main_dir}/analysis/filesets")
        with open(f"{fileset_path}/{year}_nanov12.yaml", "r") as f:
            self.dataset_config = yaml.safe_load(f)
        # get categories
        config_builder = ProcessorConfigBuilder(processor=processor, year=year)
        processor_config = config_builder.build_processor_config()
        self.categories = processor_config.event_selection["categories"]
        # run postprocessor
        self.run_postprocess()

    def run_postprocess(self):
        print_header("grouping outputs by sample")
        self.group_outputs()

        print_header("scaling outputs by sample")
        self.set_lumixsec_weights()
        self.scale_histograms()
        self.scale_cutflow()

        print_header("grouping outputs by process")
        self.histograms = self.group_by_process(self.scaled_histograms)
        logging.info(
            yaml.dump(self.process_samples, sort_keys=False, default_flow_style=False)
        )

        print_header(f"Cutflow")
        for category in self.categories:
            output_path = Path(f"{self.output_dir}/{category}")
            if not output_path.exists():
                output_path.mkdir(parents=True, exist_ok=True)
            logging.info(f"category: {category}")
            processed_cutflow = self.group_by_process(self.scaled_cutflow[category])
            self.cutflow_df = pd.DataFrame(processed_cutflow)
            # add total background events to cutflow
            self.cutflow_df["Total Background"] = self.cutflow_df.drop(
                columns="Data"
            ).sum(axis=1)
            # sort cutflow to show 'Data' and 'Total Background' first
            self.cutflow_df = self.cutflow_df[
                ["Data", "Total Background"]
                + [
                    process
                    for process in self.cutflow_df.columns
                    if process not in ["Data", "Total Background"]
                ]
            ]
            logging.info(
                f'{self.cutflow_df.applymap(lambda x: f"{x:.3f}" if pd.notnull(x) else "")}\n'
            )
            self.cutflow_df.to_csv(f"{output_path}/cutflow_{category}.csv")

        print_header(f"Results")
        for category in self.categories:
            output_path = Path(f"{self.output_dir}/{category}")
            logging.info(f"category: {category}")
            results_df = self.get_results_report(category)
            logging.info(
                results_df.applymap(lambda x: f"{x:.5f}" if pd.notnull(x) else "")
            )
            logging.info("\n")
            results_df.to_csv(f"{output_path}/results_{category}.csv")
            latex_table = df_to_latex(results_df)
            with open(f"{output_path}/results_latex_{category}.txt", "w") as f:
                f.write(latex_table)

    def group_outputs(self):
        """
        group and accumulate output files by sample
        """
        logging.info(f"reading outputs from {self.output_dir}")
        extension = ".coffea"
        output_files = glob.glob(f"{self.output_dir}/*/*{extension}", recursive=True)
        n_output_files = len(output_files)
        assert n_output_files != 0, "No output files found"

        # group output file paths by sample name
        grouped_outputs = {}
        for output_file in output_files:
            sample_name = output_file.split("/")[-1].split(extension)[0]
            if sample_name.rsplit("_")[-1].isdigit():
                sample_name = "_".join(sample_name.rsplit("_")[:-1])
            sample_name = sample_name.replace(f"{self.year}_", "")
            if sample_name in grouped_outputs:
                grouped_outputs[sample_name].append(output_file)
            else:
                grouped_outputs[sample_name] = [output_file]

        logging.info(f"{n_output_files} output files were found:")
        n_grouped_outputs = {}

        # open output dictionaries with layout:
        #      {<sample>_<i-th>: {"histograms": {"pt": Hist(...), ...}, "metadata": {"sumw": x, ...}}})
        # group and accumulate histograms and metadata by <sample>
        self.metadata = {}
        self.histograms = {}
        grouped_metadata = {}
        grouped_histograms = {}
        print_header("Reading and accumulating outputs by sample")
        for sample in grouped_outputs:
            logging.info(f"{sample}...")
            grouped_histograms[sample] = []
            grouped_metadata[sample] = {}
            for fname in grouped_outputs[sample]:
                output = load(fname)
                if output:
                    # group histograms by sample
                    grouped_histograms[sample].append(output["histograms"])
                    # group metadata by sample
                    for meta_key in output["metadata"]:
                        if meta_key in grouped_metadata[sample]:
                            grouped_metadata[sample][meta_key].append(
                                output["metadata"][meta_key]
                            )
                        else:
                            grouped_metadata[sample][meta_key] = [
                                output["metadata"][meta_key]
                            ]
            # accumulate histograms and metadata by sample
            self.histograms[sample] = accumulate(grouped_histograms[sample])
            self.metadata[sample] = {}
            for meta_key in grouped_metadata[sample]:
                self.metadata[sample][meta_key] = accumulate(
                    grouped_metadata[sample][meta_key]
                )

    def set_lumixsec_weights(self):
        print_header("Computing lumi-xsec weights")
        # load luminosities
        with open(f"{Path.cwd()}/analysis/data/luminosity.yaml", "r") as f:
            self.luminosities = yaml.safe_load(f)
        logging.info(f"luminosity [/pb] {self.luminosities[self.year]}")
        # compute lumi-xsec weights
        self.weights = {}
        self.xsecs = {}
        self.sumw = {}
        for sample, metadata in self.metadata.items():
            self.weights[sample] = 1
            self.xsecs[sample] = self.dataset_config[sample]["xsec"]
            self.sumw[sample] = metadata["sumw"]
            if self.dataset_config[sample]["era"] == "MC":
                self.weights[sample] = (
                    self.luminosities[self.year] * self.xsecs[sample]
                ) / self.sumw[sample]
        scale_info = pd.DataFrame(
            {
                "xsec [pb]": self.xsecs,
                "sumw": self.sumw,
                "weight": self.weights,
            }
        )
        logging.info(scale_info.applymap(lambda x: f"{x}" if pd.notnull(x) else ""))

    def scale_histograms(self):
        """scale histograms to lumi-xsec"""
        self.scaled_histograms = {}
        for sample, variables in self.histograms.items():
            # scale histograms
            self.scaled_histograms[sample] = {}
            for variable in variables:
                self.scaled_histograms[sample][variable] = (
                    self.histograms[sample][variable] * self.weights[sample]
                )

    def scale_cutflow(self):
        """scale cutflow to lumi-xsec"""
        self.scaled_cutflow = {}
        for category in self.categories:
            self.scaled_cutflow[category] = {}
            for sample, variables in self.histograms.items():
                self.scaled_cutflow[category][sample] = {}
                if category in self.metadata[sample]:
                    for cut, nevents in self.metadata[sample][category][
                        "cutflow"
                    ].items():
                        self.scaled_cutflow[category][sample][cut] = (
                            nevents * self.weights[sample]
                        )

    def group_by_process(self, to_group):
        """group and accumulate histograms by process"""
        group = {}
        self.process_samples = {}
        for sample in to_group:
            process = self.dataset_config[sample]["process"]
            if process not in group:
                group[process] = [to_group[sample]]
                self.process_samples[process] = [sample]
            else:
                group[process].append(to_group[sample])
                self.process_samples[process].append(sample)

        for process in group:
            group[process] = accumulate(group[process])

        return group

    def get_results_report(self, category):
        nevents = {}
        stat_errors = {}
        for process, samples in self.process_samples.items():
            nevents[process] = 0
            stat_errors[process] = 0
            for sample in samples:
                # compute number of events after selection
                final_nevents = (
                    self.metadata[sample][category]["weighted_final_nevents"]
                    * self.weights[sample]
                )
                nevents[process] += final_nevents
                # compute number of raw initial and final events to compute statistical error
                stat_error = np.sqrt(
                    self.metadata[sample][category]["raw_final_nevents"]
                )
                if self.dataset_config[sample]["era"] == "MC":
                    stat_error /= self.metadata[sample]["raw_initial_nevents"]
                    stat_error *= self.luminosities[self.year] * self.xsecs[sample]
                stat_errors[process] += stat_error**2
            stat_errors[process] = np.sqrt(stat_errors[process])

        # initialize results table with number of events and statistical uncertainty
        results_df = pd.DataFrame({"events": nevents, "stat unc": stat_errors})
        # add background percentage
        bkg_process = [p for p in results_df.index if p != "Data"]
        results_df["percentage"] = (
            results_df.loc[bkg_process, "events"]
            / results_df.loc[bkg_process, "events"].sum()
        ) * 100
        # add total background
        results_df.loc["Total Background", "events"] = np.sum(
            results_df.loc[bkg_process, "events"]
        )
        results_df.loc["Total Background", "stat unc"] = np.sqrt(
            np.sum(results_df.loc[bkg_process, "stat unc"] ** 2)
        )
        # add Data/Bkg ratio and its uncertainty
        data = results_df.loc["Data", "events"]
        data_unc = results_df.loc["Data", "stat unc"]
        bkg = results_df.loc["Total Background", "events"]
        bkg_unc = results_df.loc["Total Background", "stat unc"]
        results_df.loc["Data/Total Background", "events"] = data / bkg
        results_df.loc["Data/Total Background", "stat unc"] = (data / bkg) * np.sqrt(
            (data_unc / data) ** 2 + (bkg_unc / bkg) ** 2
        )
        # sort by percentage
        results_df = results_df.loc[
            bkg_process + ["Total Background", "Data", "Data/Total Background"]
        ]
        results_df = results_df.sort_values(by="percentage", ascending=False)

        # compute systematic uncertainty
        nominal, syst = {}, {}
        for process, hist_dict in self.histograms.items():
            if process == "Data":
                continue
            syst[process] = {}
            # get some helper histogram to extract nominal and variations values
            for histo_key, histo in hist_dict.items():
                axis_names = [
                    axis
                    for axis in histo.axes.name
                    if axis not in ["variation", "category"]
                ]
                helper_histo_key = histo_key
                helper_axis = axis_names[0]
                break
            # get nominal values by process
            nominal[process] = (
                self.histograms[process][helper_histo_key][
                    {"variation": "nominal", "category": category}
                ]
                .project(helper_axis)
                .values()
            )
            # get variations values by process
            variations_keys = []
            for variation in self.histograms[process][helper_histo_key].axes[
                "variation"
            ]:
                if variation == "nominal":
                    continue
                # get variation key
                variation_key = variation.replace("Up", "").replace("Down", "")
                if variation_key not in variations_keys:
                    variations_keys.append(variation_key)
                # get variation hist
                variation_hist = self.histograms[process][helper_histo_key][
                    {"variation": variation, "category": category}
                ].project(helper_axis)
                if variation in syst:
                    syst[process][variation].append(variation_hist)
                else:
                    syst[process][variation] = [variation_hist]
        # accumulate variations
        syst_variations = {}
        for process in syst:
            syst_variations[process] = {}
            for variation in syst[process]:
                syst_variations[process][variation] = accumulate(
                    syst[process][variation]
                )
        # compute up/down variations
        bin_error_up, bin_error_down = {}, {}
        band_up, band_down = {}, {}
        for process in syst_variations:
            bin_error_up[process] = 0
            bin_error_down[process] = 0
            for variation in variations_keys:
                # Up/down variations for a single MC sample
                var_up = syst_variations[process][f"{variation}Up"].values()
                var_down = syst_variations[process][f"{variation}Down"].values()
                # Compute the uncertainties corresponding to the up/down variations
                err_up = var_up - nominal[process]
                err_down = var_down - nominal[process]
                # Compute the flags to check which of the two variations (up and down) are pushing the nominal value up and down
                up_is_up = err_up > 0
                down_is_down = err_down < 0
                # Compute the flag to check if the uncertainty is one-sided, i.e. when both variations are up or down
                is_onesided = up_is_up ^ down_is_down
                # Sum in quadrature of the systematic uncertainties taking into account if the uncertainty is one- or double-sided
                err2_up_twosided = np.where(up_is_up, err_up**2, err_down**2)
                err2_down_twosided = np.where(up_is_up, err_down**2, err_up**2)
                err2_max = np.maximum(err2_up_twosided, err2_down_twosided)
                err2_up_onesided = np.where(is_onesided & up_is_up, err2_max, 0)
                err2_down_onesided = np.where(is_onesided & down_is_down, err2_max, 0)
                err2_up_combined = np.where(
                    is_onesided, err2_up_onesided, err2_up_twosided
                )
                err2_down_combined = np.where(
                    is_onesided, err2_down_onesided, err2_down_twosided
                )
                # Sum in quadrature of the systematic uncertainty corresponding to a MC sample
                err2_up = err2_up_combined
                err2_down = err2_down_combined

            band_up[process] = np.sum(np.sqrt(err2_up))
            band_down[process] = np.sum(np.sqrt(err2_down))

        # add sys uncertainties to results table
        results_df.loc["Total Background", "syst unc up"] = 0
        results_df.loc["Total Background", "syst unc down"] = 0
        for process in syst_variations:
            error_up = band_up[process]
            error_down = band_down[process]
            results_df.loc[process, "syst unc up"] = error_up
            results_df.loc[process, "syst unc down"] = error_down
            results_df.loc["Total Background", "syst unc up"] += error_up**2
            results_df.loc["Total Background", "syst unc down"] += error_down**2

        results_df.loc["Total Background", "syst unc up"] = np.sqrt(
            results_df.loc["Total Background", "syst unc up"]
        )
        results_df.loc["Total Background", "syst unc down"] = np.sqrt(
            results_df.loc["Total Background", "syst unc down"]
        )

        results_df = results_df.loc[
            :, ["events", "percentage", "stat unc", "syst unc up", "syst unc down"]
        ]
        return results_df
