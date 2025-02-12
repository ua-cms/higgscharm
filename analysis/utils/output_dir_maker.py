from analysis.utils.path_handler import Paths

def make_output_directory(args: dict) -> str:
    """builds output directories. Returns output path"""
    paths = Paths(eos=args["eos"])
    processor_output_path = paths.processor_path(
        processor=args["processor"],
        year=args["year"],
        dataset=args["dataset"]
    )
    return str(processor_output_path)