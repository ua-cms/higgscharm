import pickle
import numpy as np


def open_output(fname: str) -> dict:
    with open(fname, "rb") as f:
        output = pickle.load(f)
    return output


def print_header(text):
    print("-" * 40)
    print(text)
    print("-" * 40)


def accumulate(to_accumulate: list):
    if isinstance(to_accumulate[0], dict):
        accumulated_values = {}
        for element in to_accumulate:
            for feature in element:
                if feature not in accumulated_values:
                    accumulated_values[feature] = element[feature]
                else:
                    accumulated_values[feature] += element[feature]
        return accumulated_values
    else:
        return np.sum(to_accumulate)
