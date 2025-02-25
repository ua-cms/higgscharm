import os
import sys
import yaml
import uproot
import logging
import subprocess
import numpy as np
import pandas as pd
from glob import glob
from pathlib import Path
from coffea.processor import accumulate
from analysis.configs import ProcessorConfigBuilder
from analysis.postprocess.utils import open_output, print_header, df_to_latex


class ROOTPostprocessor:
    def __init__(
        self,
        processor: str,
        year: str,
        category: str,
        output_dir: Path,
    ):
        self.processor = processor
        self.year = year
        self.category = category
        self.output_dir = output_dir
        self.indir = output_dir.parent

        # get datasets configs
        main_dir = Path.cwd()
        fileset_path = Path(f"{main_dir}/analysis/filesets")
        with open(f"{fileset_path}/{year}_nanov12.yaml", "r") as f:
            self.dataset_config = yaml.safe_load(f)

        # load luminosities
        with open(f"{Path.cwd()}/analysis/postprocess/luminosity.yaml", "r") as f:
            self.luminosities = yaml.safe_load(f)

        config_builder = ProcessorConfigBuilder(processor=processor, year=year)
        processor_config = config_builder.build_processor_config()
        self.histogram_config = processor_config.histogram_config

    def run_postprocess(self):
        self.merge_metadata()
        self.get_weight()
        self.merge_histograms()
        self.scale_histograms()
        self.group_histograms()

        print_header(f"Cutflow")
        logging.info(f"category: {self.category}")
        self.scale_cutflow()
        self.group_cutflow()
        self.sample_cutflow_df = pd.DataFrame(self.scaled_cutflow)
        logging.info("\nCutflow by sample")
        logging.info(
            f'{self.sample_cutflow_df.applymap(lambda x: f"{x:.3f}" if pd.notnull(x) else "")}\n'
        )
        self.cutflow_df = pd.DataFrame(self.processed_cutflow)
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
        logging.info("\nCutflow by process")
        logging.info(
            f'{self.cutflow_df.applymap(lambda x: f"{x:.3f}" if pd.notnull(x) else "")}\n'
        )
        self.cutflow_df.to_csv(f"{self.output_dir}/cutflow_{self.category}.csv")

        print_header(f"Results")
        logging.info("\nEvents by sample:\n")
        logging.info(
            pd.DataFrame({"events": self.nevents}).applymap(
                lambda x: f"{x}" if pd.notnull(x) else ""
            )
        )
        results_df = self.get_results_report(self.category)
        logging.info("\nEvents by process:\n")
        logging.info(results_df.applymap(lambda x: f"{x:.5f}" if pd.notnull(x) else ""))
        logging.info("\n")
        results_df.to_csv(f"{self.output_dir}/results.csv")
        latex_table = df_to_latex(results_df)
        with open(f"{self.output_dir}/results_latex.txt", "w") as f:
            f.write(latex_table)

    def merge_histograms(self):
        samples = os.listdir(self.indir)
        with open(f"{self.indir}/group_by_sample.sh", "w") as outfile:
            outfile.write("#!/bin/bash\n")
            for sample in samples:
                roots = glob(f"{self.indir}/{sample}/{sample}_*.root")
                if len(roots) == 0:
                    continue
                outfile.write(
                    f"hadd -f0 -O {self.indir}/{sample}.root {self.indir}/{sample}/*.root\n"
                )
        print_header("Merging histograms by sample")
        subprocess.run(["bash", f"{self.indir}/group_by_sample.sh"])

    def merge_metadata(self):
        """
        group and accumulate output files by sample
        """
        print_header("Processing metadata")
        metadata_dir = self.output_dir.parent
        logging.info(f"Reading metadata from {metadata_dir}")
        output_files = glob(f"{metadata_dir}/*/*.pkl", recursive=True)
        n_output_files = len(output_files)
        assert n_output_files != 0, "No output files found"

        # group output file paths by sample name
        grouped_outputs = {}
        for output_file in output_files:
            sample_name = output_file.split("/")[-2]
            if sample_name in grouped_outputs:
                grouped_outputs[sample_name].append(output_file)
            else:
                grouped_outputs[sample_name] = [output_file]

        logging.info(f"{n_output_files} output files were found:")
        n_grouped_outputs = {}

        self.metadata = {}
        grouped_metadata = {}
        logging.info("merging metadata by sample")
        for sample in grouped_outputs:
            logging.info(f"{sample}...")
            grouped_metadata[sample] = {}
            for fname in grouped_outputs[sample]:
                output = open_output(fname)
                for k, v in output["base"].items():
                    output[k] = v
                for meta_key in output:
                    if meta_key in grouped_metadata[sample]:
                        grouped_metadata[sample][meta_key].append(output[meta_key])
                    else:
                        grouped_metadata[sample][meta_key] = [output[meta_key]]
            self.metadata[sample] = {}
            for meta_key in grouped_metadata[sample]:
                self.metadata[sample][meta_key] = accumulate(
                    grouped_metadata[sample][meta_key]
                )

    def get_weight(self):
        print_header("Computing lumi-xsec weights")
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
        print_header("Scaling histograms to lumi-xsec")
        self.scaled_histograms = {}
        self.processes = {}
        root_files = glob(f"{self.indir}/*.root")
        self.nevents = {}
        for root_file in root_files:
            sample = root_file.split("/")[-1].replace(".root", "")
            logging.info(f"{sample}...")
            self.nevents[sample] = (
                self.weights[sample] * self.metadata[sample]["weighted_final_nevents"]
            )
            self.scaled_histograms[sample] = {}
            with uproot.open(root_file) as f:
                for hist_key in f:
                    if self.category not in hist_key:
                        continue
                    variable = hist_key.replace(";1", "").replace(
                        f"{self.category}_", ""
                    )
                    try:
                        self.scaled_histograms[sample][variable] = (
                            f[hist_key].to_hist() * self.weights[sample]
                        )
                    except ValueError:
                        logging.info(f"could not process {hist_key} histogram")

    def group_histograms(self):
        group = {}
        self.process_samples = {}
        for sample in self.scaled_histograms:
            if not self.nevents[sample] > 0:
                continue
            process = self.dataset_config[sample]["process"]
            if process not in group:
                group[process] = [self.scaled_histograms[sample]]
                self.process_samples[process] = [sample]
            else:
                group[process].append(self.scaled_histograms[sample])
                self.process_samples[process].append(sample)
        for process in group:
            group[process] = accumulate(group[process])
        self.proccesed_histograms = group

    def scale_cutflow(self):
        """scale cutflow to lumi-xsec"""
        self.scaled_cutflow = {}
        for sample in self.metadata:
            self.scaled_cutflow[sample] = {}
            for cut, nevents in self.metadata[sample]["cutflow"].items():
                self.scaled_cutflow[sample][cut] = nevents * self.weights[sample]

    def group_cutflow(self):
        group = {}
        for sample in self.scaled_cutflow:
            process = self.dataset_config[sample]["process"]
            if process not in group:
                group[process] = [self.scaled_cutflow[sample]]
            else:
                group[process].append(self.scaled_cutflow[sample])
        for process in group:
            group[process] = accumulate(group[process])
        self.processed_cutflow = group

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

        # get systematic variations keys
        variations_keys = []
        for variable in self.histogram_config.variables:
            if "met" in variable:
                continue
            for process, histogram_dict in self.proccesed_histograms.items():
                if process == "Data":
                    continue
                for hist_key in histogram_dict:
                    if "nominal" in hist_key:
                        continue
                    if hist_key.startswith(variable):
                        variations_keys.append(hist_key.replace(f"{variable}_", ""))
                break
            break
        variations_keys = [
            v.replace("Up", "").replace("Down", "") for v in variations_keys
        ]
        variations_keys = list(set(variations_keys))
        # compute systematic uncertainty
        nominal, syst = {}, {}
        for process, hist_dict in self.proccesed_histograms.items():
            if process == "Data":
                continue
            syst[process] = {}
            # get some helper variable to extract nominal and variations values
            for variable in self.histogram_config.variables:
                helper_var = variable
                break
            # get nominal values by process
            nominal[process] = self.proccesed_histograms[process][
                f"{helper_var}_nominal"
            ].values()
            # get variations values by process
            for variation in variations_keys:
                for shift in ["Up", "Down"]:
                    variation_key = f"{variation}{shift}"
                    variation_hist = self.proccesed_histograms[process][
                        f"{helper_var}_{variation_key}"
                    ]
                    if variation_key in syst:
                        syst[process][variation_key].append(variation_hist)
                    else:
                        syst[process][variation_key] = [variation_hist]
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
