import os
import yaml
import pickle
import logging
import numpy as np
import mplhep as hep
import matplotlib.pyplot as plt
from pathlib import Path
from matplotlib import ticker
from matplotlib.lines import Line2D
from coffea.processor import accumulate
from hist.intervals import poisson_interval
from matplotlib.offsetbox import AnchoredText
from analysis.filesets.utils import get_workflow_key_process_map, get_process_era_map
from analysis.histograms import VariableAxis, IntegerAxis
from analysis.workflows.config import WorkflowConfigBuilder
from analysis.postprocess.utils import (
    setup_logger,
    divide_by_binwidth,
    get_variations_keys,
)

np.seterr(invalid="ignore")
np.seterr(divide="ignore")


class CoffeaPlotter:
    def __init__(
        self,
        workflow: str,
        year: str,
        processed_histograms: dict,
        output_dir: str,
        group_by: str,
        pass_axis: str,
    ):
        self.workflow = workflow
        self.year = year
        self.processed_histograms = processed_histograms
        self.output_dir = output_dir
        self.group_by = group_by
        self.pass_axis = pass_axis

        # get histogram config
        config_builder = WorkflowConfigBuilder(workflow=workflow)
        workflow_config = config_builder.build_workflow_config()
        self.histogram_config = workflow_config.histogram_config
        self.datasets = workflow_config.datasets

        # load luminosities and plotting style params
        postprocess_dir = Path.cwd() / "analysis" / "postprocess"
        style_file = postprocess_dir / "style.yaml"
        luminosity_file = postprocess_dir / "luminosity.yaml"
        color_map_file = postprocess_dir / "process_color_map.yaml"

        with open(style_file, "r") as f:
            self.style = yaml.safe_load(f)
        with open(f"{Path.cwd()}/analysis/postprocess/luminosity.yaml", "r") as f:
            self.luminosities = yaml.safe_load(f)

        # set processes -> era map
        self.process_era_map = get_process_era_map(year)

        # set processes -> color map
        with open(color_map_file, "r") as f:
            color_map = yaml.safe_load(f)
        self.color_map = {p: c for p, c in color_map.items()}

    def get_histogram(
        self,
        variable,
        variation,
        category,
        histogram,
        other_category=None,
    ):
        """returns histogram by processes/variable/category"""
        # get variable histogram for nominal variation and category
        selector = {"variation": variation}
        if "category" in histogram.axes.name:
            selector["category"] = category
        if self.pass_axis:
            selector[self.pass_axis] = True
        if other_category is not None:
            selector[self.group_by["name"]] = other_category
        histogram = histogram[selector].project(variable)
        return histogram

    def get_variations(
        self,
        variable,
        category,
        variation,
        histogram,
    ):
        """returns variation histogram by processes/variable/category"""
        # get variable histogram for nominal variation and category
        selectorup = {"variation": f"{variation}Up"}
        selectordown = {"variation": f"{variation}Down"}
        if self.pass_axis:
            selectorup[self.pass_axis] = True
            selectordown[self.pass_axis] = True
        if "category" in histogram.axes.name:
            selectorup["category"] = category
            selectordown["category"] = category
        histogram_up = histogram[selectorup].project(variable)
        histogram_down = histogram[selectordown].project(variable)
        # if axis type is variable divide by bin width
        return histogram_up, histogram_down

    def collect_histograms_for_plotting(self, variable, category):
        histogram_info = {}
        if "mc" in self.datasets:
            histogram_info["mc"] = {"nominal": {}, "variations": {}}
        if "signal" in self.datasets:
            histogram_info["signal"] = {"nominal": {}}
        if self.group_by != "process":
            histogram_info["categories"] = {}

        for process, histogram_dict in self.processed_histograms.items():
            if variable in histogram_dict:
                aux_histogram = histogram_dict[variable]
            else:
                for key in histogram_dict:
                    if variable in histogram_dict[key].axes.name:
                        aux_histogram = histogram_dict[key]
                        break

            if process == "Data":
                histogram_info["data"] = self.get_histogram(
                    variable=variable,
                    category=category,
                    variation="nominal",
                    histogram=aux_histogram,
                )
            else:
                key = self.process_era_map[process]
                if self.group_by != "process":
                    cat_axis = aux_histogram.axes[self.group_by["name"]]
                    self.category_map = {cat_axis.index(cat): cat for cat in cat_axis}
                    group_by_categories = [cat_axis.index(cat) for cat in cat_axis]
                    for cat in group_by_categories:
                        if cat not in histogram_info["categories"]:
                            histogram_info["categories"][cat] = {}
                        histogram_info["categories"][cat][process] = self.get_histogram(
                            variable=variable,
                            category=category,
                            variation="nominal",
                            other_category=cat,
                            histogram=aux_histogram,
                        )
                else:
                    histogram_info[key]["nominal"][process] = self.get_histogram(
                        variable=variable,
                        category=category,
                        variation="nominal",
                        histogram=aux_histogram,
                    )

                # save variations histograms
                if key == "mc":
                    for variation in get_variations_keys(self.processed_histograms):
                        var_cats = [v for v in aux_histogram.axes["variation"]]
                        if not f"{variation}Up" in var_cats:
                            continue
                        up, down = self.get_variations(
                            variable=variable,
                            category=category,
                            variation=variation,
                            histogram=aux_histogram,
                        )
                        if f"{variation}Up" in histogram_info[key]["variations"]:
                            histogram_info[key]["variations"][f"{variation}Up"] += up
                            histogram_info[key]["variations"][f"{variation}Down"] += up
                        else:
                            histogram_info[key]["variations"][f"{variation}Up"] = up
                            histogram_info[key]["variations"][f"{variation}Down"] = down

        return histogram_info

    def plot_uncert_band(self, histogram_info, ax):
        # initialize up/down errors with statisticall error
        mcstat_err2 = self.nominal_variances
        err2_up = mcstat_err2
        err2_down = mcstat_err2

        for variation in get_variations_keys(self.processed_histograms):
            # Up/down variations for a single MC sample
            var_up = histogram_info["mc"]["variations"][f"{variation}Up"].values()
            var_down = histogram_info["mc"]["variations"][f"{variation}Down"].values()
            # Compute the uncertainties corresponding to the up/down variations
            err_up = var_up - self.nominal_values
            err_down = var_down - self.nominal_values
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

        self.band_up = self.nominal_values + np.sqrt(err2_up)
        self.band_down = self.nominal_values - np.sqrt(err2_down)
        # plot stat + syst uncertainty band
        ax.bar(
            x=self.centers,
            height=self.band_up - self.band_down,
            width=self.widths,
            bottom=self.band_down,
            **self.style["uncert_band_kwargs"],
        )

    def plot_ratio(self, rax):
        # compute Data/MC ratio
        num = self.data_values
        den = self.nominal_values
        ratio = num / den
        # only the uncertainty of num (DATA) propagated
        num_variances = self.data_variances
        ratio_variance = num_variances * np.power(den, -2)
        ratio_uncert = np.abs(poisson_interval(ratio, ratio_variance) - ratio)
        # plot ratio and x-y errors
        xerr = self.edges[1:] - self.edges[:-1]
        rax.errorbar(
            self.centers,
            ratio,
            xerr=xerr / 2,
            yerr=ratio_uncert,
            **self.style["ratio_error_kwargs"],
        )
        # plot ratio uncertainty band
        ratio_up = np.concatenate([[0], self.band_up / den])
        ratio_down = np.concatenate([[0], self.band_down / den])
        ratio_up[np.isnan(ratio_up)] = 1.0
        ratio_down[np.isnan(ratio_down)] = 1.0
        ratio_uncertainty_band = rax.fill_between(
            self.edges,
            ratio_up,
            ratio_down,
            step="pre",
            **self.style["uncert_band_kwargs"],
        )
        # plot horizontal reference line at 1
        xmin, xmax = rax.get_xlim()
        rax.hlines(1, xmin, xmax, color="k", linestyle=":")

    def add_text(self, variable, category, ax):
        text_map = {
            "ztoee": rf"$ Z \rightarrow ee$ events",
            "ztomumu": rf"$ Z \rightarrow \mu \mu$ events",
        }
        if self.workflow.startswith("zplusl_"):
            zplusl_method = {
                "zplusl_os": "(OS)",
                "zplusl_ss": "(SS)",
            }
            zplusl_text = {
                "electron": rf"$Z+e$ {zplusl_method[self.workflow]} events",
                "muon": rf"$Z+\mu$ {zplusl_method[self.workflow]} events",
            }
            if self.pass_axis:
                zplusl_text = {
                    "electron": f"$Z+e$ {zplusl_method[self.workflow]} events with $e$ passing selection",
                    "muon": f"$Z+\mu$ {zplusl_method[self.workflow]} events with $\mu$ passing selection",
                }
            zplusl_text_map = {
                "zplusl_os": zplusl_text[category],
                "zplusl_ss": zplusl_text[category],
                "zplusl_ss_maximal": zplusl_text[category] + " (maximal)",
                "zplusl_ss_minimal": zplusl_text[category] + " (minimal)",
                "zplusl_ss_intermediate": zplusl_text[category] + " (intermediate)",
            }
            text_map = {**text_map, **zplusl_text_map}

        if self.workflow.startswith("zplusll"):
            zplusll_text_map = {
                "zplusll_os": (
                    "3P+1F control sample"
                    if "1fcr" in variable
                    else "2P+2F control sample"
                ),
                "zplusll_ss": "SS-SF control sample",
            }
            text_map = {**text_map, **zplusll_text_map}

        if self.workflow == "zzto4l":
            zzto4l_identifier = {
                "zz_mass_4e": r"$4e$",
                "zz_mass_4mu": r"$4\mu$",
                "zz_mass_2mu2e": r"$2\mu 2e$",
                "zz_mass_2e2mu": r"$2e2\mu$",
            }
            zzto4l_text = (
                r"$ZZ\rightarrow$"
                + zzto4l_identifier.get(variable, r"$4\ell$")
                + " events"
            )
            zzto4l_text_map = {"zzto4l": zzto4l_text}
            text_map = {**text_map, **zzto4l_text_map}

        if self.workflow == "hplusc":
            hplusc_test = {"hplusc": "H(ZZ)+c-jet events"}
            text_map = text_map = {**text_map, **hplusc_test}

        ax.add_artist(
            AnchoredText(
                text_map.get(self.workflow, f"{self.workflow} events") + "\n",
                loc="upper left",
                frameon=False,
            )
        )

    def add_xylabels(self, variable, category, add_ratio, ax, rax):
        ylabel = "Events"
        if isinstance(self.histogram_config.axes[variable], VariableAxis):
            ylabel += " / GeV"

        xlabel = self.histogram_config.axes[variable].label

        if self.workflow.startswith("zplusl_"):
            if category == "electron":
                xlabel = xlabel.replace(r"\ell", r"e")
            elif category == "muon":
                xlabel = xlabel.replace(r"\ell", r"\mu")

        if add_ratio:
            ax.set(xlabel=None, ylabel=ylabel)
            rax.set(
                xlabel=xlabel,
                ylabel="Data / Pred",
                facecolor="white",
            )
        else:
            ax.set(xlabel=xlabel, ylabel=ylabel)

    def plot_histograms(
        self,
        variable: str,
        category: str,
        yratio_limits: str = None,
        log: bool = False,
        add_ratio: bool = True,
        blind: bool = False,
        extension: str = "png",
    ):
        if blind:
            add_ratio = False

        setup_logger(self.output_dir)

        # set plot params
        hep.style.use(hep.style.CMS)
        plt.rcParams.update(self.style["rcParams"])

        # get mc, data and signal histograms
        histogram_info = self.collect_histograms_for_plotting(variable, category)

        # get nominal MC histograms
        mc_colors, mc_labels = [], []
        if self.group_by == "process":
            if (self.workflow in ["zzto4l", "hplusc"]) and ("zz_mass" in variable):
                mc_labels = ["ggToZZ", "qqToZZ", "H(125)"]
                nominal_mc_hists = [
                    histogram_info["mc"]["nominal"][p] for p in mc_labels
                ]
                mc_colors = [self.color_map[p] for p in mc_labels]
            else:
                nominal_mc_hists = list(histogram_info["mc"]["nominal"].values())
                for process in histogram_info["mc"]["nominal"]:
                    mc_labels.append(process)
                    mc_colors.append(self.color_map[process])
        else:
            nominal_mc_hists = []
            for cat in histogram_info["categories"]:
                nominal_mc_hists.append(
                    accumulate(histogram_info["categories"][cat].values())
                )
                labels_map = {v: k for k, v in self.group_by["label"].items()}
                mc_labels.append(labels_map[self.category_map[cat]])

        mc_histogram = accumulate(nominal_mc_hists)
        self.nominal_values = mc_histogram.values()
        self.nominal_variances = mc_histogram.variances()
        self.edges = mc_histogram.axes.edges[0]
        self.centers = mc_histogram.axes.centers[0]
        self.widths = mc_histogram.axes.widths[0]

        # get variation MC histograms
        variation_histograms = histogram_info["mc"]["variations"]

        # get Data histogram
        if "data" not in self.datasets:
            blind = True
        if not blind:
            data_histogram = histogram_info["data"]
            self.data_values = data_histogram.values()
            self.data_variances = data_histogram.variances()

        # plot stacked MC and Data histograms
        fig, (ax, rax) = plt.subplots(
            nrows=2,
            ncols=1,
            figsize=(9, 10),
            tight_layout=True,
            gridspec_kw={"height_ratios": (4, 1)},
            sharex=True,
        )
        if not add_ratio:
            fig, ax = plt.subplots(
                nrows=1,
                ncols=1,
                figsize=(9, 9),
                tight_layout=True,
            )
        mc_hist_args = {
            "H": nominal_mc_hists,
            "label": mc_labels,
            "flow": "none",
            "ax": ax,
        }
        mc_hist_args.update(self.style["mc_hist_kwargs"])
        if mc_colors:
            mc_hist_args.update({"color": mc_colors})
        if (self.workflow in ["zzto4l", "hplusc"]) and ("zz_mass" in variable):
            mc_hist_args["sort"] = None
        hep.histplot(**mc_hist_args)
        if not blind:
            hep.histplot(
                data_histogram,
                label="Data",
                flow="none",
                ax=ax,
                **self.style["data_hist_kwargs"],
            )
        if "signal" in self.datasets:
            for signal_process, signal_histogram in histogram_info["signal"][
                "nominal"
            ].items():
                hep.histplot(
                    signal_histogram,
                    label=signal_process,
                    color=self.color_map[signal_process],
                    flow="none",
                    ax=ax,
                    **self.style["signal_hist_kwargs"],
                )

        # plot uncertainty band
        self.plot_uncert_band(histogram_info, ax)

        # plot ratio
        if add_ratio:
            self.plot_ratio(rax)

        # set limits
        hist_edges = np.array([[i, j] for i, j in zip(self.edges[:-1], self.edges[1:])])
        xlimits = np.min(hist_edges[self.nominal_values > 0]), np.max(
            hist_edges[self.nominal_values > 0]
        )
        ax.set_xlim(xlimits)
        if add_ratio:
            rax.set_xlim(xlimits)
            rax.set_ylim(yratio_limits)

        # set axes labels
        self.add_xylabels(variable, category, add_ratio, ax, rax)

        # set plot text
        self.add_text(variable, category, ax)

        # set log scale
        if log:
            formatter = ticker.ScalarFormatter()
            formatter.set_scientific(False)
            ax.yaxis.set_major_formatter(formatter)
            ax.set_yscale("log")
            ax.set_ylim(top=np.max(mc_histogram.values()) * 100)
        else:
            ylim = ax.get_ylim()[1]
            ax.set_ylim(0, ylim + 0.2 * ylim)
        ax.legend(
            frameon=True,
            loc="upper right",
            fontsize=13,
        )
        if isinstance(self.histogram_config.axes[variable], IntegerAxis):
            start = self.histogram_config.axes[variable].start
            stop = self.histogram_config.axes[variable].stop
            categories = np.arange(start, stop)
            if len(categories) > 20:
                for i, label in enumerate(rax.get_xticklabels()):
                    if i % 5 != 0:  # Show only every 5th tick
                        label.set_visible(False)
        # add CMS info
        lumi = self.luminosities[self.year] * 1e-3
        energy = "13" if self.year.startswith("201") else "13.6"
        hep.cms.lumitext(
            f"{lumi:.1f} fb$^{{-1}}$ ({self.year}, {energy} TeV)",
            ax=ax,
        )
        hep.cms.text("Preliminary", ax=ax)
        # save histograms
        output_path = Path(f"{self.output_dir}/{category}")
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
        figname = f"{str(output_path)}/{self.workflow}_{category}_{variable}_{self.year}.{extension}"
        if self.pass_axis:
            figname = f"{str(output_path)}/{self.workflow}_{category}_{variable}_{self.pass_axis}_{self.year}.{extension}"
        if self.group_by != "process":
            figname = f"{str(output_path)}/{self.workflow}_{category}_{variable}_{self.year}_groupedby{self.group_by['name']}.{extension}"
        fig.savefig(figname)
        plt.close()

    def _debug_fr_table(self, label, endcap_vals, barrel_vals, edges):
        """Print a (pt x region) table matching the compute_fake_rates debug format."""
        pt_labels = [f"{edges[i]:.0f}-{edges[i+1]:.0f}" for i in range(len(edges) - 1)]
        region_labels = ["endcap", "barrel"]
        col_w = 14
        header = (
            f"{'pT / region':>16s}"
            + "".join(f"{r:>{col_w}s}" for r in region_labels)
            + f"{'SUM':>{col_w}s}"
        )
        print(f"\n  {label}")
        print(f"  {'-' * len(header)}")
        print(f"  {header}")
        print(f"  {'-' * len(header)}")
        arr = np.stack([endcap_vals, barrel_vals], axis=1)
        for i, pt_lab in enumerate(pt_labels):
            row = arr[i, :]
            print(f"  {pt_lab:>16s}" + "".join(f"{v:>{col_w}.2f}" for v in row)
                    + f"{np.sum(row):>{col_w}.2f}")
        print(f"  {'-' * len(header)}")
        col_sums = arr.sum(axis=0)
        print(f"  {'SUM':>16s}" + "".join(f"{v:>{col_w}.2f}" for v in col_sums)
               + f"{arr.sum():>{col_w}.2f}")
        print()

    def plot_fake_rate(self, category, ylim=(None, 0.35), extension="png",debug=True):
        edges = (
            self.processed_histograms["Data"]["probe_lepton"]
            .project("probe_lepton_pt")
            .axes.edges[0]
        )
        xerr = edges[1:] - edges[:-1]
        centers = (
            self.processed_histograms["Data"]["probe_lepton"]
            .project("probe_lepton_pt")
            .axes.centers[0]
        )

        data_barrel = self.processed_histograms["Data"]["probe_lepton"][
            {"variation": "nominal", "category": category, "is_barrel_lepton": True}
        ].project("probe_lepton_pt", "is_passing_lepton")
        data_endcap = self.processed_histograms["Data"]["probe_lepton"][
            {"variation": "nominal", "category": category, "is_barrel_lepton": False}
        ].project("probe_lepton_pt", "is_passing_lepton")
        wz_barrel = self.processed_histograms["WZ"]["probe_lepton"][
            {"variation": "nominal", "category": category, "is_barrel_lepton": True}
        ].project("probe_lepton_pt", "is_passing_lepton")
        wz_endcap = self.processed_histograms["WZ"]["probe_lepton"][
            {"variation": "nominal", "category": category, "is_barrel_lepton": False}
        ].project("probe_lepton_pt", "is_passing_lepton")
        data_wz_barrel = data_barrel + (wz_barrel * -1)
        data_wz_endcap = data_endcap + (wz_endcap * -1)
        
        if debug:
            sep = "=" * 80
            print(f"\n{sep}\n  DEBUG [COFFEA PLOTTER — {category.upper()}]\n{sep}")

            # column 0 = endcap (is_barrel False), column 1 = barrel (is_barrel True)
            data_total = {"endcap": data_endcap[{"is_passing_lepton": sum}].values(),
                "barrel": data_barrel[{"is_passing_lepton": sum}].values()}
            data_pass  = {"endcap": data_endcap[{"is_passing_lepton": True}].values(),
                "barrel": data_barrel[{"is_passing_lepton": True}].values()}
            wz_total   = {"endcap": wz_endcap[{"is_passing_lepton": sum}].values(),
                "barrel": wz_barrel[{"is_passing_lepton": sum}].values()}
            wz_pass    = {"endcap": wz_endcap[{"is_passing_lepton": True}].values(),
                "barrel": wz_barrel[{"is_passing_lepton": True}].values()}
            corr_total = {"endcap": data_wz_endcap[{"is_passing_lepton": sum}].values(),
                "barrel": data_wz_barrel[{"is_passing_lepton": sum}].values()}
            corr_pass  = {"endcap": data_wz_endcap[{"is_passing_lepton": True}].values(),
                "barrel": data_wz_barrel[{"is_passing_lepton": True}].values()}

            self._debug_fr_table("DATA  n_total (unweighted)",    data_total["endcap"], data_total["barrel"], edges)
            self._debug_fr_table("DATA  n_pass  (unweighted)",    data_pass["endcap"],  data_pass["barrel"],  edges)
            self._debug_fr_table("WZ MC n_total (lumi-weighted)", wz_total["endcap"],   wz_total["barrel"],   edges)
            self._debug_fr_table("WZ MC n_pass  (lumi-weighted)", wz_pass["endcap"],    wz_pass["barrel"],    edges)
            self._debug_fr_table("CORR  n_total = DATA - WZ",     corr_total["endcap"], corr_total["barrel"], edges)
            self._debug_fr_table("CORR  n_pass  = DATA - WZ",     corr_pass["endcap"],  corr_pass["barrel"],  edges)

            with np.errstate(divide="ignore", invalid="ignore"):
                fr_data = {r: np.where(data_total[r] > 0, data_pass[r] / data_total[r], np.nan)
                          for r in ("endcap", "barrel")}
                fr_corr = {r: np.where(corr_total[r] > 0, corr_pass[r] / corr_total[r], np.nan)
                        for r in ("endcap", "barrel")}
            self._debug_fr_table("DATA  fake rate", fr_data["endcap"], fr_data["barrel"], edges)
            self._debug_fr_table("CORR  fake rate", fr_corr["endcap"], fr_corr["barrel"], edges)

            print(f"  SCALAR SUMMARY [{category}]")
            print(f"    DATA  total probe leptons    : {sum(v.sum() for v in data_total.values()):.1f}")
            print(f"    DATA  passing probe leptons  : {sum(v.sum() for v in data_pass.values()):.1f}")
            print(f"    WZ MC total (lumi-weighted)  : {sum(v.sum() for v in wz_total.values()):.4f}")
            print(f"    WZ MC passing (lumi-weighted): {sum(v.sum() for v in wz_pass.values()):.4f}")
            print()

        data_barrel_ratio = (
            data_barrel[{"is_passing_lepton": True}].values()
            / data_barrel[{"is_passing_lepton": sum}].values()
        )
        data_endcap_ratio = (
            data_endcap[{"is_passing_lepton": True}].values()
            / data_endcap[{"is_passing_lepton": sum}].values()
        )
        data_wz_barrel_ratio = (
            data_wz_barrel[{"is_passing_lepton": True}].values()
            / data_wz_barrel[{"is_passing_lepton": sum}].values()
        )
        data_wz_endcap_ratio = (
            data_wz_endcap[{"is_passing_lepton": True}].values()
            / data_wz_endcap[{"is_passing_lepton": sum}].values()
        )

        data_barrel_den = data_barrel[{"is_passing_lepton": sum}].values()
        data_barrel_num_variances = data_barrel[{"is_passing_lepton": True}].variances()
        data_barrel_ratio_variance = data_barrel_num_variances * np.power(
            data_barrel_den, -2
        )
        data_barrel_ratio_uncert = np.abs(
            poisson_interval(data_barrel_ratio, data_barrel_ratio_variance)
            - data_barrel_ratio
        )

        data_endcap_den = data_endcap[{"is_passing_lepton": sum}].values()
        data_endcap_num_variances = data_endcap[{"is_passing_lepton": True}].variances()
        data_endcap_ratio_variance = data_endcap_num_variances * np.power(
            data_endcap_den, -2
        )
        data_endcap_ratio_uncert = np.abs(
            poisson_interval(data_endcap_ratio, data_endcap_ratio_variance)
            - data_endcap_ratio
        )

        data_wz_barrel_den = data_wz_barrel[{"is_passing_lepton": sum}].values()
        data_wz_barrel_num_variances = data_wz_barrel[
            {"is_passing_lepton": True}
        ].variances()
        data_wz_barrel_ratio_variance = data_wz_barrel_num_variances * np.power(
            data_wz_barrel_den, -2
        )
        data_wz_barrel_ratio_uncert = np.abs(
            poisson_interval(data_wz_barrel_ratio, data_wz_barrel_ratio_variance)
            - data_wz_barrel_ratio
        )

        data_wz_endcap_den = data_wz_endcap[{"is_passing_lepton": sum}].values()
        data_wz_endcap_num_variances = data_wz_endcap[
            {"is_passing_lepton": True}
        ].variances()
        data_wz_endcap_ratio_variance = data_wz_endcap_num_variances * np.power(
            data_wz_endcap_den, -2
        )
        data_wz_endcap_ratio_uncert = np.abs(
            poisson_interval(data_wz_endcap_ratio, data_wz_endcap_ratio_variance)
            - data_wz_endcap_ratio
        )

        fig, ax = plt.subplots(figsize=(8, 7))
        errorbar_container1 = ax.errorbar(
            centers,
            data_barrel_ratio,
            xerr=xerr / 2,
            yerr=data_barrel_ratio_uncert,
            fmt="bo",
            elinewidth=1,
            linestyle="solid",
            linewidth=0,
            markersize=6,
        )
        errorbar_container2 = ax.errorbar(
            centers,
            data_endcap_ratio,
            xerr=xerr / 2,
            yerr=data_endcap_ratio_uncert,
            fmt="ro",
            elinewidth=1,
            linestyle="solid",
            linewidth=0,
            markersize=6,
        )
        # Customizing error lines after creation (optional for full control)
        for bar in errorbar_container1[2]:
            bar.set_linestyle("-")
            bar.set_color("b")
            bar.set_linewidth(2)
        for bar in errorbar_container2[2]:
            bar.set_linestyle("-")
            bar.set_color("r")
            bar.set_linewidth(2)

        errorbar_container3 = ax.errorbar(
            centers,
            data_wz_barrel_ratio,
            xerr=xerr / 2,
            yerr=data_wz_barrel_ratio_uncert,
            fmt="bo",
            elinewidth=1,
            linestyle="solid",
            linewidth=0,
            markersize=6,
        )
        errorbar_container4 = ax.errorbar(
            centers,
            data_wz_endcap_ratio,
            xerr=xerr / 2,
            yerr=data_wz_endcap_ratio_uncert,
            fmt="ro",
            elinewidth=1,
            linestyle="solid",
            linewidth=0,
            markersize=6,
        )
        # Customizing error lines after creation (optional for full control)
        for bar in errorbar_container3[2]:
            bar.set_linestyle(":")
            bar.set_color("b")
            bar.set_linewidth(2)
        for bar in errorbar_container4[2]:
            bar.set_linestyle(":")
            bar.set_color("r")
            bar.set_linewidth(2)

        legend_elements = [
            Line2D(
                [0],
                [0],
                color="blue",
                linewidth=2,
                linestyle="-",
                label="Data (barrel)",
            ),
            Line2D(
                [0], [0], color="red", linewidth=2, linestyle="-", label="Data (endcap)"
            ),
            Line2D(
                [0],
                [0],
                color="blue",
                linewidth=2,
                linestyle=":",
                label="Data - WZ (barrel)",
            ),
            Line2D(
                [0],
                [0],
                color="red",
                linewidth=2,
                linestyle=":",
                label="Data - WZ (endcap)",
            ),
        ]
        ax.legend(
            handles=legend_elements,
            loc="upper center",
            handlelength=3,
            handleheight=2,
            fontsize=15,
            ncols=2,
        )
        hep.cms.text("Preliminary", ax=ax)
        hep.cms.lumitext(
            f"{self.luminosities[self.year] * 1e-3:.1f} fb$^{{-1}}$ ({self.year}, 13.6 TeV)",
            ax=ax,
        )
        xlabel = "$p_T(e)$ [GeV]" if category == "electron" else "$p_T(\mu)$ [GeV]"
        ax.set(ylim=ylim, xlabel=xlabel)
        ax.set_ylabel("Fake rate")
        output_path = Path(f"{self.output_dir}/{category}")
        if not output_path.exists():
            output_path.mkdir(parents=True, exist_ok=True)
        fig.savefig(
            f"{output_path}/{self.workflow}_{category}_FR_{self.year}.{extension}"
        )

    def _compute_fake_rate_arrays(self, n_pass, n_total, min_events=10):
        """
        Binomial fake rate f = n_pass / n_total with the same validity/clipping
        convention as compute_fake_rates.compute_fake_rate: bins with
        n_total <= min_events (or negative counts, which can occur after MC
        subtraction) get fake_rate=0 and a sentinel fake_rate_err=1.0 so
        downstream consumers (get_fake_rate in estimate_zx_background.py)
        can identify and skip them.
        """
        valid = (n_total > min_events) & (n_pass >= 0) & (n_total >= 0)
        with np.errstate(divide="ignore", invalid="ignore"):
            fr = np.where(valid, n_pass / n_total, 0.0)
            fr = np.clip(fr, 0.0, 1.0)
            fr_err = np.where(
                valid,
                np.sqrt(fr * (1 - fr) / np.where(valid, n_total, 1.0)),
                1.0,
            )
        return fr, fr_err

    def compute_fake_rates(self, categories=("electron", "muon"), min_events=10, debug=False):
        """
        Compute fake rates directly from the histograms this plotter already
        holds, reusing exactly the DATA-minus-WZ barrel/endcap logic used in
        plot_fake_rate (and printed by _debug_fr_table), instead of relying
        on the separate compute_fake_rates.py script.

        Returns a dict keyed by category (e.g. 'electron', 'muon'), each with:
          'fake_rate', 'fake_rate_err', 'n_pass', 'n_total' : shape (n_pt_bins, 2)
              column 0 = endcap, column 1 = barrel
          'pt_edges'  : probe_lepton_pt bin edges
          'eta_edges' : [0.0, boundary, eta_max] where boundary is the EB/EE
              cut used to fill is_barrel_lepton (1.2 for muon, 1.5 for electron)

        This is the format expected by get_fake_rate() in
        estimate_zx_background.py (np.searchsorted over pt_edges/eta_edges).
        """
        eb_ee_boundary = {"muon": 1.2, "electron": 1.5}
        eta_max = 2.5

        fake_rates = {}
        for category in categories:
            pt_edges = (
                self.processed_histograms["Data"]["probe_lepton"]
                .project("probe_lepton_pt")
                .axes.edges[0]
            )

            data_barrel = self.processed_histograms["Data"]["probe_lepton"][
                {"variation": "nominal", "category": category, "is_barrel_lepton": True}
            ].project("probe_lepton_pt", "is_passing_lepton")
            data_endcap = self.processed_histograms["Data"]["probe_lepton"][
                {"variation": "nominal", "category": category, "is_barrel_lepton": False}
            ].project("probe_lepton_pt", "is_passing_lepton")
            wz_barrel = self.processed_histograms["WZ"]["probe_lepton"][
                {"variation": "nominal", "category": category, "is_barrel_lepton": True}
            ].project("probe_lepton_pt", "is_passing_lepton")
            wz_endcap = self.processed_histograms["WZ"]["probe_lepton"][
                {"variation": "nominal", "category": category, "is_barrel_lepton": False}
            ].project("probe_lepton_pt", "is_passing_lepton")

            # DATA-only (uncorrected)
            unc_total_endcap = data_endcap[{"is_passing_lepton": sum}].values()
            unc_pass_endcap = data_endcap[{"is_passing_lepton": True}].values()
            unc_total_barrel = data_barrel[{"is_passing_lepton": sum}].values()
            unc_pass_barrel = data_barrel[{"is_passing_lepton": True}].values()

            # DATA - WZ (corrected)
            data_wz_barrel = data_barrel + (wz_barrel * -1)
            data_wz_endcap = data_endcap + (wz_endcap * -1)
            
            if debug:
                sep = "=" * 80
                print(f"\n{sep}\n  DEBUG [COMPUTE_FAKE_RATES — {category.upper()}]\n{sep}")

                data_total = {"endcap": data_endcap[{"is_passing_lepton": sum}].values(),
                    "barrel": data_barrel[{"is_passing_lepton": sum}].values()}
                data_pass  = {"endcap": data_endcap[{"is_passing_lepton": True}].values(),
                    "barrel": data_barrel[{"is_passing_lepton": True}].values()}
                wz_total   = {"endcap": wz_endcap[{"is_passing_lepton": sum}].values(),
                    "barrel": wz_barrel[{"is_passing_lepton": sum}].values()}
                wz_pass    = {"endcap": wz_endcap[{"is_passing_lepton": True}].values(),
                    "barrel": wz_barrel[{"is_passing_lepton": True}].values()}
                corr_total = {"endcap": data_wz_endcap[{"is_passing_lepton": sum}].values(),
                    "barrel": data_wz_barrel[{"is_passing_lepton": sum}].values()}
                corr_pass  = {"endcap": data_wz_endcap[{"is_passing_lepton": True}].values(),
                    "barrel": data_wz_barrel[{"is_passing_lepton": True}].values()}

                self._debug_fr_table("DATA  n_total (unweighted)",    data_total["endcap"], data_total["barrel"], pt_edges)
                self._debug_fr_table("DATA  n_pass  (unweighted)",    data_pass["endcap"],  data_pass["barrel"],  pt_edges)
                self._debug_fr_table("WZ MC n_total (lumi-weighted)", wz_total["endcap"],   wz_total["barrel"],   pt_edges)
                self._debug_fr_table("WZ MC n_pass  (lumi-weighted)", wz_pass["endcap"],    wz_pass["barrel"],    pt_edges)
                self._debug_fr_table("CORR  n_total = DATA - WZ",     corr_total["endcap"], corr_total["barrel"], pt_edges)
                self._debug_fr_table("CORR  n_pass  = DATA - WZ",     corr_pass["endcap"],  corr_pass["barrel"],  pt_edges)

                with np.errstate(divide="ignore", invalid="ignore"):
                    fr_data = {r: np.where(data_total[r] > 0, data_pass[r] / data_total[r], np.nan)
                              for r in ("endcap", "barrel")}
                    fr_wz = {r: np.where(wz_total[r] > 0, wz_pass[r] / wz_total[r], np.nan)
                              for r in ("endcap", "barrel")}
                    fr_corr = {r: np.where(corr_total[r] > 0, corr_pass[r] / corr_total[r], np.nan)
                            for r in ("endcap", "barrel")}
                self._debug_fr_table("DATA  fake rate", fr_data["endcap"], fr_data["barrel"], pt_edges)
                self._debug_fr_table("WZ MC fake rate", fr_wz["endcap"], fr_wz["barrel"], pt_edges)
                self._debug_fr_table("CORR  fake rate", fr_corr["endcap"], fr_corr["barrel"], pt_edges)

                data_tot_sum = sum(v.sum() for v in data_total.values())
                wz_tot_sum = sum(v.sum() for v in wz_total.values())
                print(f"  SCALAR SUMMARY [{category}]")
                print(f"    DATA  total probe leptons    : {data_tot_sum:.1f}")
                print(f"    DATA  passing probe leptons  : {sum(v.sum() for v in data_pass.values()):.1f}")
                print(f"    WZ MC total (lumi-weighted)  : {wz_tot_sum:.4f}")
                print(f"    WZ MC passing (lumi-weighted): {sum(v.sum() for v in wz_pass.values()):.4f}")
                print(f"    WZ / DATA total ratio        : {wz_tot_sum / max(data_tot_sum, 1):.4f}")
                print()
            corr_total_endcap = data_wz_endcap[{"is_passing_lepton": sum}].values()
            corr_pass_endcap = data_wz_endcap[{"is_passing_lepton": True}].values()
            corr_total_barrel = data_wz_barrel[{"is_passing_lepton": sum}].values()
            corr_pass_barrel = data_wz_barrel[{"is_passing_lepton": True}].values()

            unc_fr_endcap, unc_fr_err_endcap = self._compute_fake_rate_arrays(
                unc_pass_endcap, unc_total_endcap, min_events
            )
            unc_fr_barrel, unc_fr_err_barrel = self._compute_fake_rate_arrays(
                unc_pass_barrel, unc_total_barrel, min_events
            )
            corr_fr_endcap, corr_fr_err_endcap = self._compute_fake_rate_arrays(
                corr_pass_endcap, corr_total_endcap, min_events
            )
            corr_fr_barrel, corr_fr_err_barrel = self._compute_fake_rate_arrays(
                corr_pass_barrel, corr_total_barrel, min_events
            )

            uncorrected = {
                "fake_rate": np.stack([unc_fr_endcap, unc_fr_barrel], axis=1),
                "fake_rate_err": np.stack([unc_fr_err_endcap, unc_fr_err_barrel], axis=1),
                "n_pass": np.stack([unc_pass_endcap, unc_pass_barrel], axis=1),
                "n_total": np.stack([unc_total_endcap, unc_total_barrel], axis=1),
                "pt_edges": pt_edges,
                "eta_edges": np.array([0.0, eb_ee_boundary[category], eta_max]),
            }
            corrected = {
                "fake_rate": np.stack([corr_fr_endcap, corr_fr_barrel], axis=1),
                "fake_rate_err": np.stack([corr_fr_err_endcap, corr_fr_err_barrel], axis=1),
                "n_pass": np.stack([corr_pass_endcap, corr_pass_barrel], axis=1),
                "n_total": np.stack([corr_total_endcap, corr_total_barrel], axis=1),
                "pt_edges": pt_edges,
                "eta_edges": np.array([0.0, eb_ee_boundary[category], eta_max]),
            }

            # safe_corrected: use WZ-corrected rate where stable, else fall back
            # to data-only, same convention as compute_fake_rates.extract_fake_rates
            stable = (corrected["n_total"] > 0) & (corrected["n_pass"] >= 0)
            n_unstable = int(np.sum(~stable))
            if n_unstable > 0:
                print(
                    f"  [{category}] {n_unstable} bins with unstable WZ subtraction "
                    f"-> falling back to uncorrected"
                )
            safe_corrected = {
                "fake_rate": np.where(stable, corrected["fake_rate"], uncorrected["fake_rate"]),
                "fake_rate_err": np.where(
                    stable, corrected["fake_rate_err"], uncorrected["fake_rate_err"]
                ),
                "n_pass": np.where(stable, corrected["n_pass"], uncorrected["n_pass"]),
                "n_total": np.where(stable, corrected["n_total"], uncorrected["n_total"]),
                "pt_edges": pt_edges,
                "eta_edges": np.array([0.0, eb_ee_boundary[category], eta_max]),
            }

            fake_rates[category] = {
                "uncorrected": uncorrected,
                "corrected": corrected,
                "safe_corrected": safe_corrected,
                # top-level keys mirror safe_corrected for backwards-compat
                # with get_fake_rate() in estimate_zx_background.py
                **safe_corrected,
            }

            fr_valid = safe_corrected["fake_rate"][safe_corrected["fake_rate"] > 0]
            if fr_valid.size:
                print(
                    f"  [{category}] safe_corrected: mean f={np.mean(fr_valid):.3f}, "
                    f"max f={np.max(safe_corrected['fake_rate']):.3f}"
                )

        return fake_rates

    def save_fake_rates(self, output_path=None, categories=("electron", "muon"), min_events=10, debug=True):
        """
        Compute and pickle fake rates for this plotter's year, in the same
        schema expected by get_fake_rate() in estimate_zx_background.py
        (dict keyed by 'electron'/'muon', each with 'fake_rate',
        'fake_rate_err', 'n_pass', 'n_total', 'pt_edges', 'eta_edges').

        Use this instead of compute_fake_rates.py — it reuses the exact
        barrel/endcap DATA-minus-WZ logic already validated in
        plot_fake_rate/_debug_fr_table on this same set of histograms,
        rather than a separately-loaded and independently-summed set of
        coffea files.

        Args:
            output_path: where to write the .pkl. Defaults to
                {self.output_dir}/fake_rates/{self.year}/fake_rates.pkl
        Returns:
            The fake_rates dict that was saved.
        """
        fake_rates = self.compute_fake_rates(categories=categories, min_events=min_events, debug=debug)

        if output_path is None:
            out_dir = Path(self.output_dir) / "fake_rates" / self.year
            out_dir.mkdir(parents=True, exist_ok=True)
            output_path = out_dir / "fake_rates.pkl"
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "wb") as f:
            pickle.dump(fake_rates, f)
        print(f"Saved fake rates to: {output_path}")
        return fake_rates