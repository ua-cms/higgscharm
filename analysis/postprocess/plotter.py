import logging
import numpy as np
import mplhep as hep
import matplotlib.pyplot as plt
from matplotlib import ticker
from analysis.utils import paths
from coffea.processor import accumulate
from hist.intervals import poisson_interval, ratio_uncertainty
from analysis.postprocess.utils import setup_logger


np.seterr(invalid="ignore")
np.seterr(divide="ignore")


hep.style.use(hep.style.CMS)
plt.rcParams.update(
    {
        "font.size": 20,
        "axes.titlesize": 30,
        "axes.labelsize": 20,
        "xtick.labelsize": 15,
        "ytick.labelsize": 15,
        "lines.markersize": 30,
        "legend.fontsize": 15,
        "xtick.minor.width": 1,
        "xtick.minor.size": 4,
        "xtick.major.width": 1,
        "xtick.major.size": 6,
        "ytick.minor.width": 1,
        "ytick.minor.size": 4,
        "ytick.major.width": 1,
        "ytick.major.size": 6,
    }
)
mc_hist_kwargs = {
    "histtype": "fill",
    "stack": True,
    "sort": "yield",
    "linewidth": 0.7,
    "edgecolor": "k",
}
data_hist_kwargs = {
    "histtype": "errorbar",
    "color": "k",
    "linestyle": "none",
    "marker": ".",
    "markersize": 13,
    "elinewidth": 1,
    "yerr": True,
    "xerr": True,
    "linestyle": "none",
    "marker": ".",
}


class Plotter:

    def __init__(
        self,
        processor: str,
        processed_histograms: dict,
        year: str,
        lumi: int,
        cat_axis: tuple,
        log_scale: bool = False,
        output_dir: str = None,
    ):
        self.processor = processor
        self.processed_histograms = processed_histograms
        self.year = year
        self.lumi = lumi
        self.cat_axis = cat_axis
        self.output_dir = output_dir

    def get_feature_hists(self, feature: str) -> dict:
        """
        get nominal and variations histograms

        # https://cms-analysis.docs.cern.ch/guidelines/plotting/colors/
        colors = [
            "#3f90da",
            "#ffa90e",
            "#bd1f01",
            "#94a4a2",
            "#832db6",
            "#a96b59",
            "#e76300",
            "#b9ac70",
            "#717581",
            "#92dadd",
        ]
        """
        color_map = {
            "DY+Jets": "#3f90da",
            "tt": "#94a4a2",
            "Single Top": "#bd1f01",
            "Diboson": "#ffa90e",
        }
        feature_hists = {
            "mc": {
                "nominal": {"histograms": [], "labels": [], "colors": []},
                "variations": {},
            },
        }
        for process, histogram_dicts in self.processed_histograms.items():
            if histogram_dicts is None:
                continue
            if feature in histogram_dicts:
                histogram = histogram_dicts[feature]
            else:
                for key in histogram_dicts:
                    if feature in histogram_dicts[key].axes.name:
                        if self.cat_axis:
                            histogram = histogram_dicts[key].project(
                                feature, "variation", self.cat_axis[0]
                            )
                        else:
                            histogram = histogram_dicts[key].project(
                                feature, "variation"
                            )
                        break
            if self.cat_axis:
                if process != "Data":
                    for variation in histogram.axes["variation"]:
                        if variation == "nominal":
                            # add nominal histograms, their labels and colors
                            feature_hists["mc"]["nominal"]["histograms"].append(
                                histogram[
                                    {
                                        "variation": "nominal",
                                        self.cat_axis[0]: self.cat_axis[1],
                                    }
                                ]
                            )
                            feature_hists["mc"]["nominal"]["labels"].append(process)
                            feature_hists["mc"]["nominal"]["colors"].append(
                                color_map[process]
                            )
                        else:
                            variation_hist = histogram[
                                {
                                    "variation": variation,
                                    self.cat_axis[0]: self.cat_axis[1],
                                }
                            ]
                            if variation in feature_hists["mc"]["variations"]:
                                feature_hists["mc"]["variations"][variation].append(
                                    variation_hist
                                )
                            else:
                                feature_hists["mc"]["variations"][variation] = [
                                    variation_hist
                                ]
                else:
                    feature_hists["data"] = histogram[
                        {"variation": "nominal", self.cat_axis[0]: self.cat_axis[1]}
                    ]
            else:
                if process != "Data":
                    for variation in histogram.axes["variation"]:
                        if variation == "nominal":
                            # add nominal histograms, their labels and colors
                            feature_hists["mc"]["nominal"]["histograms"].append(
                                histogram[{"variation": "nominal"}]
                            )
                            feature_hists["mc"]["nominal"]["labels"].append(process)
                            feature_hists["mc"]["nominal"]["colors"].append(
                                color_map[process]
                            )
                        else:
                            variation_hist = histogram[{"variation": variation}]
                            if variation in feature_hists["mc"]["variations"]:
                                feature_hists["mc"]["variations"][variation].append(
                                    variation_hist
                                )
                            else:
                                feature_hists["mc"]["variations"][variation] = [
                                    variation_hist
                                ]
                else:
                    feature_hists["data"] = histogram[{"variation": "nominal"}]

        # accumulate variations histograms
        for variation, hist_list in feature_hists["mc"]["variations"].items():
            feature_hists["mc"]["variations"][variation] = accumulate(hist_list)
        return feature_hists

    def plot_feature_hist(
        self,
        feature: str,
        feature_label: str,
        yratio_limits: str = None,
        savefig: bool = True,
    ):
        setup_logger(self.output_dir)
        # compute nominal (MC and Data) and variation (MC) histograms
        feature_hists = self.get_feature_hists(feature)
        # MC nominal
        nominal_mc_hists = feature_hists["mc"]["nominal"]["histograms"]
        mc_histogram = accumulate(nominal_mc_hists)
        mc_histogram_values = mc_histogram.values()
        mc_histogram_variances = mc_histogram.variances()
        mc_histogram_edges = mc_histogram.axes.edges[0]
        mc_histogram_centers = mc_histogram.axes.centers[0]
        mc_histogram_widths = mc_histogram.axes.widths[0]
        # MC variations
        variations_mc_hists = feature_hists["mc"]["variations"]
        # Data
        data_histogram = feature_hists["data"]
        data_histogram_values = data_histogram.values()

        # plot stacked MC and Data histograms
        fig, (ax, rax) = plt.subplots(
            nrows=2,
            ncols=1,
            figsize=(9, 10),
            tight_layout=True,
            gridspec_kw={"height_ratios": (4, 1)},
            sharex=True,
        )
        hep.histplot(
            data_histogram, label="Data", flow="none", ax=ax, **data_hist_kwargs
        )
        hep.histplot(
            nominal_mc_hists,
            label=feature_hists["mc"]["nominal"]["labels"],
            color=feature_hists["mc"]["nominal"]["colors"],
            flow="none",
            ax=ax,
            **mc_hist_kwargs,
        )

        # get up and down stat uncertainty per bin
        nom_stat_down, nom_stat_up = poisson_interval(
            values=mc_histogram_values, variances=mc_histogram_variances
        )
        # initialize up and down errors per bin
        bin_error_up = np.abs(nom_stat_up - mc_histogram_values) ** 2
        bin_error_down = np.abs(nom_stat_down - mc_histogram_values) ** 2
        # add variation errors to bin errors
        for variation, variation_hist in variations_mc_hists.items():
            variation_values = variation_hist.values()
            if "Up" in variation:
                # add up variation
                max_values = np.max(
                    np.stack([mc_histogram_values, variation_values]), axis=0
                )
                up_variation_values = np.abs(max_values - mc_histogram_values)
                bin_error_up += up_variation_values**2
            else:
                # add down variation
                min_values = np.min(
                    np.stack([mc_histogram_values, variation_values]), axis=0
                )
                down_variation_values = np.abs(min_values - mc_histogram_values)
                bin_error_down += down_variation_values**2
        band_up = mc_histogram_values + np.sqrt(bin_error_up)
        band_down = mc_histogram_values - np.sqrt(bin_error_down)

        # plot stat + syst uncertainty band
        ax.bar(
            x=mc_histogram_centers,
            height=band_up - band_down,
            width=mc_histogram_widths,
            bottom=band_down,
            color="lightgray",
            alpha=0.6,
            label="Stat + Syst unc.",
            hatch="/" * 3,
            edgecolor="black",
            linewidth=0,
        )

        # compute Data/MC ratio
        ratio = data_histogram_values / mc_histogram_values
        # plot ratio x error bar
        xerr = mc_histogram_edges[1:] - mc_histogram_edges[:-1]
        rax.errorbar(
            x=mc_histogram_centers,
            y=ratio,
            xerr=xerr / 2,
            fmt=f"ko",
            markersize=6,
        )
        # plot ratio y error bar
        try:
            ratio_error_down, ratio_error_up = ratio_uncertainty(
                num=data_histogram_values,
                denom=mc_histogram_values,
                uncertainty_type="poisson-ratio",
            )
            rax.vlines(
                mc_histogram_centers,
                ratio + ratio_error_down,
                ratio - ratio_error_down,
                color="k",
            )
        except ValueError:
            logging.info(f"(no poisson-ratio error for {feature})")

        # plot ratio uncertaity band
        ratio_up = np.concatenate([[0], band_up / mc_histogram_values])
        ratio_down = np.concatenate([[0], band_down / mc_histogram_values])
        ratio_uncertainty_band = rax.fill_between(
            mc_histogram_edges,
            ratio_up,
            ratio_down,
            step="pre",
            color="lightgray",
            hatch="////",
            alpha=0.6,
            edgecolor="k",
            linewidth=0,
        )

        # plot horizontal reference lines
        xmin, xmax = rax.get_xlim()
        rax.hlines(1, xmin, xmax, color="k", linestyle=":")
        """
        ymajorticks = rax.get_yticks()
        ymajorticks_mp = (ymajorticks[1:] + ymajorticks[:-1]) / 2
        yticks = np.concatenate([ymajorticks, ymajorticks_mp])
        for ytick in yticks:
            rax.hlines(ytick, xmin, xmax, color="k", linestyle=":", alpha=0.3)
        """
        # set limits
        hist_edges = np.array(
            [[i, j] for i, j in zip(mc_histogram_edges[:-1], mc_histogram_edges[1:])]
        )
        xlimits = np.min(hist_edges[mc_histogram_values > 0]), np.max(
            hist_edges[mc_histogram_values > 0]
        )
        ax.set_xlim(xlimits)
        rax.set_xlim(xlimits)
        if yratio_limits is None:
            try:
                up_limit = np.nanmax(ratio_error_up)
                down_limit = np.nanmin(ratio_error_down)
                scale = 1.1
                yup = scale * up_limit
                ydown = down_limit - scale * (1 - down_limit)

                up_distance = up_limit - 1
                down_distance = down_limit - 1
                if abs(up_distance) > 2 * abs(down_distance):
                    ydown = 1 - up_distance
                if yup < 0:
                    yup = 1 + scale * max(down_distance, up_distance)
                up_distance = abs(1 - yup)
                down_distance = abs(1 - ydown)
                max_distance = max(down_distance, up_distance)
                yratio_limits = 1 - max_distance, 1 + max_distance
            except:
                yratio_limits = (0, 2)
        rax.set_ylim(yratio_limits)

        # set legend layout
        if ("eta" in feature) or ("phi" in feature):
            ax.legend(loc="lower left", frameon=True)
        else:
            ax.legend()

        # set axes labels
        ax.set(xlabel=None, ylabel="Events")
        formatter = ticker.ScalarFormatter()
        formatter.set_scientific(False)
        ax.yaxis.set_major_formatter(formatter)
        rax.set(xlabel=feature_label, ylabel="Data / Pred", facecolor="white")

        # set log scale
        if log_scale:
            ax.set_yscale("log")

        # add CMS info
        hep.cms.lumitext(
            f"{self.lumi * 1e-3:.1f} fb$^{{-1}}$ ({self.year}, 13.6 TeV)",
            ax=ax,
        )
        hep.cms.text("Preliminary", ax=ax)

        # save histograms
        if savefig:
            fname = f"{self.output_dir}/{self.processor}_{feature}"
            if self.cat_axis:
                fname += f"_{self.cat_axis[0]}_{self.cat_axis[1]}"
            fig.savefig(f"{fname}_{self.year}.png")
        plt.close()
