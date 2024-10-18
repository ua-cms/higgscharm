import json
import glob
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from analysis.postprocess.utils import open_output, print_header, accumulate


class Postprocessor:
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
        with open(f"{fileset_path}/{year}_fileset.json", "r") as f:
            self.dataset_config = json.load(f)

        # run postprocessor
        self.run_postprocess()

    def run_postprocess(self):
        print_header("grouping outputs by sample")
        self.group_outputs()

        print_header("scaling outputs by sample")
        self.set_lumixsec_weights()
        self.scale()

        print_header("grouping outputs by process")
        self.histograms = self.group_by_process(self.scaled_histograms)
        logging.info(pd.Series(self.process_samples).to_string(dtype=False))

        print_header(f"Cutflow")
        processed_cutflow = self.group_by_process(self.scaled_cutflow)
        self.cutflow_df = pd.DataFrame(processed_cutflow)
        self.cutflow_df["Total Background"] = self.cutflow_df.drop(columns="Data").sum(
            axis=1
        )
        self.cutflow_df = self.cutflow_df[
            ["Data", "Total Background"]
            + [
                process
                for process in self.cutflow_df.columns
                if process not in ["Data", "Total Background"]
            ]
        ]
        logging.info(self.cutflow_df.map(lambda x: f"{x:.3f}" if pd.notnull(x) else ""))
        self.cutflow_df.to_csv(f"{self.output_dir}/cutflow.csv")

        print_header(f"Results")
        results_df = self.get_results_report()
        logging.info(results_df.map(lambda x: f"{x:.5f}" if pd.notnull(x) else ""))
        results_df.to_csv(f"{self.output_dir}/results.csv")

    def group_outputs(self):
        """
        group and accumulate output files by sample
        """
        logging.info(f"reading outputs from {self.output_dir}")
        extension = ".pkl"
        output_files = glob.glob(f"{self.output_dir}/*{extension}", recursive=True)
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
                output = open_output(fname)
                if output:
                    output_dataset_key = list(output.keys())[0]
                    # group histograms by sample
                    grouped_histograms[sample].append(
                        output[output_dataset_key]["histograms"]
                    )
                    # group metadata by sample
                    for meta_key in output[output_dataset_key]["metadata"]:
                        if meta_key in grouped_metadata[sample]:
                            grouped_metadata[sample][meta_key].append(
                                output[output_dataset_key]["metadata"][meta_key]
                            )
                        else:
                            grouped_metadata[sample][meta_key] = [
                                output[output_dataset_key]["metadata"][meta_key]
                            ]
            # accumulate histograms and metadata by sample
            self.histograms[sample] = accumulate(grouped_histograms[sample])
            self.metadata[sample] = {}
            for meta_key in grouped_metadata[sample]:
                self.metadata[sample][meta_key] = accumulate(
                    grouped_metadata[sample][meta_key]
                )

    def set_lumixsec_weights(self):
        """compute luminosity and xsec-lumi weights"""
        # get integrated luminosity (/pb)
        self.luminosities = {}
        for sample, metadata in self.metadata.items():
            if "lumi" in metadata:
                self.luminosities[sample] = float(metadata["lumi"])
        self.luminosities["Total"] = np.sum(list(self.luminosities.values()))
        logging.info(pd.DataFrame({"luminosity [/pb]": self.luminosities}))
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
                    self.luminosities["Total"] * self.xsecs[sample]
                ) / self.sumw[sample]

    def scale(self):
        """scale histograms and cutflow to lumi-xsec"""
        scale_info = pd.DataFrame(
            {
                "xsec [pb]": self.xsecs,
                "sumw": self.sumw,
                "weight": self.weights,
            }
        )
        logging.info(
            scale_info.drop(
                [data_key for data_key in self.luminosities if data_key != "Total"]
            ).map(lambda x: f"{x:.5f}" if pd.notnull(x) else "")
        )
        self.scaled_histograms = {}
        self.scaled_cutflow = {}
        for sample, features in self.histograms.items():
            # scale histograms
            self.scaled_histograms[sample] = {}
            for feature in features:
                self.scaled_histograms[sample][feature] = (
                    self.histograms[sample][feature] * self.weights[sample]
                )
            # scale cutflow
            self.scaled_cutflow[sample] = {}
            for cut, nevents in self.metadata[sample]["cutflow"].items():
                self.scaled_cutflow[sample][cut] = nevents * self.weights[sample]

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

    def get_results_report(self):
        nevents = {}
        stat_errors = {}
        for process, samples in self.process_samples.items():
            nevents[process] = 0
            stat_errors[process] = 0
            for sample in samples:
                # compute number of events after selection
                final_nevents = (
                    self.metadata[sample]["weighted_final_nevents"]
                    * self.weights[sample]
                )
                nevents[process] += final_nevents
                # compute number of raw initial and final events to compute statistical error
                stat_error = np.sqrt(self.metadata[sample]["raw_final_nevents"])
                if self.dataset_config[sample]["era"] == "MC":
                    stat_error /= self.metadata[sample]["raw_initial_nevents"]
                    stat_error *= self.luminosities["Total"] * self.xsecs[sample]
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
        results_df.loc["Data/Background", "events"] = data / bkg
        results_df.loc["Data/Background", "stat unc"] = (data / bkg) * np.sqrt(
            (data_unc / data) ** 2 + (bkg_unc / bkg) ** 2
        )
        # sort by percentage
        results_df = results_df.loc[
            bkg_process + ["Total Background", "Data", "Data/Background"]
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
                axis_names = [axis for axis in histo.axes.name if axis != "variation"]
                helper_histo = histo_key
                helper_axis = axis_names[0]
                break
            # get nominal values by process
            nominal[process] = (
                self.histograms[process][helper_histo][{"variation": "nominal"}]
                .project(helper_axis)
                .values()
            )
            # get variations values by process
            for variation in self.histograms[process][helper_histo].axes["variation"]:
                if variation == "nominal":
                    continue
                variation_hist = self.histograms[process][helper_histo][
                    {"variation": variation}
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
            for variation in syst_variations[process]:
                syst_values = syst_variations[process][variation].values()
                # add up variation
                max_syst_values = np.max(
                    np.stack([nominal[process], syst_values]), axis=0
                )
                max_syst_values = np.abs(max_syst_values - nominal[process])
                bin_error_up[process] += max_syst_values**2
                # add down variation
                min_syst_values = np.min(
                    np.stack([nominal[process], syst_values]), axis=0
                )
                min_syst_values = np.abs(min_syst_values - nominal[process])
                bin_error_down[process] += min_syst_values**2

            band_up[process] = np.sum(np.sqrt(np.sqrt(bin_error_up[process]) ** 2))
            band_down[process] = np.sum(np.sqrt(np.sqrt(bin_error_down[process]) ** 2))

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
