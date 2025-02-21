from analysis.utils.path_handler import Paths


def make_output_directory(args: dict) -> str:
    """builds output directories. Returns output path"""
    paths = Paths(eos=args["eos"])
    path_args = {}
    for arg in ["processor", "year", "dataset"]:
        if arg in args:
            path_args[arg] = args[arg]
        else:
            path_args[arg] = None
    processor_output_path = paths.processor_path(**path_args)
    return str(processor_output_path)
