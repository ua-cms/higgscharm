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


def df_to_latex(df):
    # Initialize LaTeX table output
    output = """
\\begin{table}[h!]
\\centering
\\begin{tabular}{@{} l c @{}}
\\hline
 & \\textbf{Events} \\\\
\\hline
"""

    # Initialize variables to hold important values for later calculations
    purity_value = df.percentage.max()  # To store the purity value
    total_background_value = None  # To store the total background events
    data_value = None  # To store the data value

    # First, process the samples and save the purity value from the first sample
    for label, row in df.iterrows():
        # Extract values from the dataframe row
        events = row["events"]
        stat_unc = row["stat unc"] if pd.notna(row["stat unc"]) else None
        syst_unc_up = row["syst unc up"] if pd.notna(row["syst unc up"]) else None
        syst_unc_down = row["syst unc down"] if pd.notna(row["syst unc down"]) else None
        percentage = row["percentage"] if pd.notna(row["percentage"]) else None

        # Ensure events are treated as float for formatting
        events_f = f"{float(events):.2f}"
        stat_unc_f = f"{float(stat_unc):.2f}" if stat_unc is not None else "nan"

        # Calculate syst uncertainty as the mean of up and down values
        if syst_unc_up is not None and syst_unc_down is not None:
            syst_unc_mean = (syst_unc_up + syst_unc_down) / 2
            syst_unc_f = f"{float(syst_unc_mean):.2f}"
        else:
            syst_unc_f = "nan"

        # Add sample rows to LaTeX output
        if label not in ["Data", "Total Background", "Data/Total Background"]:
            output += f"{label} & ${events_f} \\pm {stat_unc_f} \\,(\\text{{stat}}) \\pm {syst_unc_f} \\,(\\text{{syst}})$\\\\\n"

    # Add line after sample rows before Total Background and Data
    output += "\\hline\n"

    # Now handle Total Background row
    total_background_row = df.loc["Total Background"]
    total_background_value = total_background_row["events"]
    stat_unc_total = total_background_row["stat unc"]
    syst_unc_up_total = total_background_row["syst unc up"]
    syst_unc_down_total = total_background_row["syst unc down"]

    # Calculate the uncertainties and format the output for Total Background
    events_f_total = f"{float(total_background_value):.2f}"
    stat_unc_f_total = (
        f"{float(stat_unc_total):.2f}" if pd.notna(stat_unc_total) else "nan"
    )
    syst_unc_f_total = (
        f"{(syst_unc_up_total + syst_unc_down_total) / 2:.2f}"
        if pd.notna(syst_unc_up_total) and pd.notna(syst_unc_down_total)
        else "nan"
    )

    output += f"Total Background & ${events_f_total} \\pm {stat_unc_f_total} \\,(\\text{{stat}}) \\pm {syst_unc_f_total} \\,(\\text{{syst}})$ \\\\ \n"

    # Now handle Data row
    data_value = df.loc["Data"]["events"]
    output += f"Data & ${float(data_value):.0f}$ \\\\ \n"

    # Add line after Data
    output += "\\hline\n"

    # Handle Purity row
    if purity_value is not None:
        output += f"Purity & {purity_value:.2f} \\% \\\\ \n"

    # Add line before Data/Background
    output += "\\hline\n"

    # Handle Data/Total Background ratio
    if total_background_value is not None and data_value is not None:
        ratio = data_value / total_background_value
        output += f"Data/Total Background & ${ratio:.2f}$ \\\\ \n"

    # End the table with the LaTeX footer
    output += "\\hline\n"
    output += "\\end{tabular}\n"
    output += "\\end{table}"

    # Return the generated LaTeX table
    return output
