import hist
import pickle
import numpy as np

def open_output(fname: str) -> dict:
    with open(fname, "rb") as f:
        output = pickle.load(f)
    return output

def print_header(text, lenght=90):
    print("-" * lenght)
    print(text)
    print("-" * lenght)

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
    elif isinstance(to_accumulate[0], hist.Hist):
        if len(to_accumulate) == 1:
            return to_accumulate[0]
        else:
            accumulated_values = to_accumulate[0]
            for element in to_accumulate[1:]:
                accumulated_values += element
            return accumulated_values
    else:
        return np.sum(to_accumulate)