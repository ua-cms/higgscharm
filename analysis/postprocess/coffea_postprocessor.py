import yaml
import glob
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from coffea.util import load, save
from coffea.processor import accumulate
from analysis.postprocess.utils import print_header, get_variations_keys


def save_process_histograms(
    process: str,
    year: str,
    output_dir: str,
    process_samples_map: dict,
    categories,
):
    print_header(f"Processing {process} outputs")

    # load dataset config
    fileset_file = Path.cwd() / "analysis" / "filesets" / f"{year}_nanov12.yaml"
    with open(fileset_file, "r") as f:
        dataset_config = yaml.safe_load(f)

    # group and accumulate output files by sample
    extension = ".coffea"
    output_files = []
    for sample in process_samples_map[process]:
        output_files += glob.glob(
            f"{output_dir}/*/{sample}*{extension}", recursive=True
        )
    n_output_files = len(output_files)
    # assert n_output_files != 0, "No output files found"
    if n_output_files == 0:
        return

    logging.info(f"{n_output_files} output files were found for process {process}")

    logging.info("grouping output file paths by sample name")
    grouped_outputs = {}
    for output_file in output_files:
        sample_name = output_file.split("/")[-1].split(extension)[0]
        if sample_name.rsplit("_")[-1].isdigit():
            sample_name = "_".join(sample_name.rsplit("_")[:-1])
        sample_name = sample_name.replace(f"{year}_", "")
        if sample_name in grouped_outputs:
            grouped_outputs[sample_name].append(output_file)
        else:
            grouped_outputs[sample_name] = [output_file]

    logging.info("Reading and accumulating outputs by sample")
    n_grouped_outputs = {}
    # open output dictionaries with layout:
    #      {<sample>_<i-th>: {"histograms": {"pt": Hist(...), ...}, "metadata": {"sumw": x, ...}}})
    # group and accumulate histograms and metadata by <sample>
    metadata = {}
    histograms = {}
    grouped_metadata = {}
    grouped_histograms = {}
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
        histograms[sample] = accumulate(grouped_histograms[sample])
        metadata[sample] = {}
        for meta_key in grouped_metadata[sample]:
            metadata[sample][meta_key] = accumulate(grouped_metadata[sample][meta_key])

    logging.info("Computing lumi-xsec weights")
    lumi_file = Path.cwd() / "analysis" / "postprocess" / "luminosity.yaml"
    with open(lumi_file, "r") as f:
        luminosities = yaml.safe_load(f)
    logging.info(f"luminosity [/pb] {luminosities[year]}")

    # compute lumi-xsec weights
    weights = {}
    xsecs = {}
    sumw = {}
    for sample, md in metadata.items():
        weights[sample] = 1
        xsecs[sample] = dataset_config[sample]["xsec"]
        sumw[sample] = md["sumw"]
        if dataset_config[sample]["era"] == "MC":
            weights[sample] = (luminosities[year] * xsecs[sample]) / sumw[sample]
    scale_info = pd.DataFrame(
        {
            "xsec [pb]": xsecs,
            "sumw": sumw,
            "weight": weights,
        }
    )
    logging.info(scale_info.applymap(lambda x: f"{x}" if pd.notnull(x) else ""))

    logging.info("scaling histograms to lumi-xsec")
    scaled_histograms = {}
    for sample, variables in histograms.items():
        # scale histograms
        scaled_histograms[sample] = {}
        for variable in variables:
            scaled_histograms[sample][variable] = (
                histograms[sample][variable] * weights[sample]
            )

    logging.info("grouping and accumulating histograms by process")
    hist_to_accumulate = []
    for sample in scaled_histograms:
        hist_to_accumulate.append(scaled_histograms[sample])

    logging.info(f"saving {process} histograms\n")
    output_histograms = {process: accumulate(hist_to_accumulate)}
    save(output_histograms, f"{output_dir}/{process}.coffea")

    logging.info("""scaling cutflow to lumi-xsec""")
    scaled_cutflow = {}
    for category in categories:
        category_dir = Path(f"{output_dir}/{category}")
        if not category_dir.exists():
            category_dir.mkdir(parents=True, exist_ok=True)
        scaled_cutflow[category] = {}
        cut_to_accumulate = []
        for sample in metadata:
            scaled_cutflow[category][sample] = {}
            if category in metadata[sample]:
                for cut, nevents in metadata[sample][category]["cutflow"].items():
                    scaled_cutflow[category][sample][cut] = nevents * weights[sample]
            cut_to_accumulate.append(scaled_cutflow[category][sample])
        processed_cutflow = {process: accumulate(cut_to_accumulate)}
        cutflow_df = pd.DataFrame(processed_cutflow)
        logging.info(f"saving {process} cutflow for category {category}\n")
        cutflow_file = Path(f"{output_dir}/{category}/cutflow_{category}_{process}.csv")
        cutflow_df.to_csv(cutflow_file)
    return process


def load_processed_histograms(
    year: str,
    output_dir: str,
    process_samples_map: dict,
):
    processed_histograms = {}
    for process in process_samples_map:
        print(process)
        processed_histograms.update(load(f"{output_dir}/{process}.coffea"))

    save(processed_histograms, f"{output_dir}/{year}_processed_histograms.coffea")
    return processed_histograms


def get_cutflow(processed_histograms, category):

    processed_cutflow = self.group_by_process(self.scaled_cutflow[category])
    self.cutflow_df = pd.DataFrame(processed_cutflow)
    # add total background events to cutflow
    self.cutflow_df["Total Background"] = self.cutflow_df.drop(columns="Data").sum(
        axis=1
    )
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


def get_results_report(processed_histograms, category):
    nominal = {}
    for process in processed_histograms:
        for kin in processed_histograms[process]:
            aux_hist = processed_histograms[process][kin]
            for aux_var in aux_hist.axes.name:
                nominal[process] = aux_hist[{"variation": "nominal"}].project(aux_var)
                break
            break

    variations = {}
    mcstat_err = {}
    bin_error_up = {}
    bin_error_down = {}
    for process in processed_histograms:
        if process == "Data":
            continue
        mcstat_err[process] = {}
        bin_error_up[process] = {}
        bin_error_down[process] = {}
        nom = nominal[process].values()
        mcstat_err2 = nominal[process].variances()
        mcstat_err[process] = np.sum(np.sqrt(mcstat_err2))
        err2_up = mcstat_err2
        err2_down = mcstat_err2
        for variation in get_variations_keys(processed_histograms):
            if f"{variation}Up" not in aux_hist.axes["variation"]:
                continue
            var_up = aux_hist[{"variation": f"{variation}Up"}].project(aux_var).values()
            var_down = (
                aux_hist[{"variation": f"{variation}Down"}].project(aux_var).values()
            )
            # Compute the uncertainties corresponding to the up/down variations
            err_up = var_up - nom
            err_down = var_down - nom
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
            err2_up_combined = np.where(is_onesided, err2_up_onesided, err2_up_twosided)
            err2_down_combined = np.where(
                is_onesided, err2_down_onesided, err2_down_twosided
            )
            # Sum in quadrature of the systematic uncertainty corresponding to a MC sample
            err2_up += err2_up_combined
            err2_down += err2_down_combined

        bin_error_up[process] = np.sum(np.sqrt(err2_up))
        bin_error_down[process] = np.sum(np.sqrt(err2_down))

    mcs = []
    results = {}
    for process in nominal:
        results[process] = {}

        results[process]["events"] = np.sum(nominal[process].values())
        if process == "Data":
            results[process]["stat err"] = np.sqrt(np.sum(nominal[process].values()))
        else:
            mcs.append(process)
            results[process]["stat err"] = mcstat_err[process]
            results[process]["syst err"] = (
                bin_error_up[process] + bin_error_down[process]
            ) / 2
    df = pd.DataFrame(results)
    df["Total background"] = df.loc[["events"], mcs].sum(axis=1)
    df.loc["stat err", "Total background"] = np.sqrt(
        np.sum(df.loc["stat err", mcs] ** 2)
    )
    df.loc["syst err", "Total background"] = np.sqrt(
        np.sum(df.loc["syst err", mcs] ** 2)
    )
    df = df.T
    df.loc["Data/Total background"] = (
        df.loc["Data", ["events"]] / df.loc["Total background", ["events"]]
    )
    return df
