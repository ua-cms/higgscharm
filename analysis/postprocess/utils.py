import os
import glob
import hist
import pickle
import logging
import numpy as np
import pandas as pd


def setup_logger(output_dir):
    """Set up the logger to log to a file in the specified output directory."""
    output_file_path = os.path.join(output_dir, "output.txt")
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.FileHandler(output_file_path), logging.StreamHandler()],
    )


def open_output(fname: str) -> dict:
    with open(fname, "rb") as f:
        output = pickle.load(f)
    return output


def print_header(text):
    logging.info("-" * 90)
    logging.info(text)
    logging.info("-" * 90)


def divide_by_binwidth(histogram):
    bin_width = histogram.axes.edges[0][1:] - histogram.axes.edges[0][:-1]
    return histogram / bin_width


def clear_output_directory(output_dir, ext):
    """Delete all result files in the output directory with extension 'ext'"""
    files = glob.glob(os.path.join(output_dir, f"*.{ext}"))
    for file in files:
        os.remove(file)


def df_to_latex(df, table_title="Events"):
    output = rf"""\begin{{table}}[h!]
\centering
\begin{{tabular}}{{@{{}} l c @{{}}}}
\hline
 & \textbf{{{table_title}}} \\
\hline
"""

    total_background_value = None
    data_value = None

    for label, row in df.iterrows():
        events = row["events"]
        stat_err = row["stat err"] if pd.notna(row["stat err"]) else None
        syst_err = row["syst err"] if pd.notna(row["syst err"]) else None

        events_f = f"{float(events):.2f}"
        stat_err_f = f"{float(stat_err):.2f}" if stat_err is not None else "nan"
        syst_err_f = f"{float(syst_err):.2f}" if syst_err is not None else "nan"

        if label not in ["Data", "Total background", "Data/Total background"]:
            output += f"{label} & ${events_f} \\pm {stat_err_f} \\, (\\text{{stat}}) \\pm {syst_err_f} \\, (\\text{{syst}})$\\\\\n"

    output += r"\hline" + "\n"

    # Total background
    bg = df.loc["Total background"]
    total_background_value = bg["events"]
    stat_err_bg = bg["stat err"]
    syst_err_bg = bg["syst err"]

    output += f"Total Background & ${float(total_background_value):.2f} \\pm {float(stat_err_bg):.2f} \\, (\\text{{stat}}) \\pm {float(syst_err_bg):.2f} \\, (\\text{{syst}})$ \\\\ \n"

    # Data
    data_value = df.loc["Data"]["events"]
    output += f"Data & ${float(data_value):.0f}$ \\\\ \n"

    output += r"\hline" + "\n"

    # Data/Total Background
    if total_background_value and data_value:
        ratio = data_value / total_background_value
        output += f"Data/Total Background & ${ratio:.2f}$ \\\\ \n"

    output += r"""\hline
\end{tabular}
\end{table}"""
    return output


def combine_event_tables(df1, df2):
    assert all(df1.index == df2.index), "Los índices de los DataFrames no coinciden."
    combined = pd.DataFrame(index=df1.index)
    combined["events"] = df1["events"] + df2["events"]
    combined["stat err"] = np.sqrt(df1["stat err"] ** 2 + df2["stat err"] ** 2)
    combined["syst err"] = np.sqrt(df1["syst err"] ** 2 + df2["syst err"] ** 2)
    data = combined.loc["Data", "events"]
    total_bkg = combined.loc["Total background", "events"]
    combined.loc["Data/Total background", ["events", "stat err", "syst err"]] = [
        data / total_bkg,
        np.nan,
        np.nan,
    ]
    return combined


def combine_cutflows(df1, df2):
    if not df1.index.equals(df2.index):
        raise ValueError("Los índices (etiquetas de cortes) no coinciden.")
    combined = df1.add(df2, fill_value=0)
    return combined


def format_cutflow_with_efficiency(events_df, eff_df):
    combined = pd.DataFrame(index=events_df.index, columns=events_df.columns)
    for col in events_df.columns:
        for idx in events_df.index:
            val = events_df.loc[idx, col]
            eff = eff_df.loc[idx, col]
            combined.loc[idx, col] = f"{val} ({eff:.2f}%)"
    return combined


def get_variations_keys(processed_histograms):
    variations = {}
    for process, histogram_dict in processed_histograms.items():
        if process == "Data":
            continue
        for feature in histogram_dict:
            helper_histogram = histogram_dict[feature]
            variations = [
                var for var in helper_histogram.axes["variation"] if var != "nominal"
            ]
            break
        break
    variations = list(
        set([var.replace("Up", "").replace("Down", "") for var in variations])
    )
    return variations
