from analysis.utils.path_handler import Paths


def make_output_directory(args) -> str:
    """builds output directories. Returns output path"""
    paths = Paths(eos=args.eos)
    path_args = {}
    for arg in ["workflow", "year", "dataset"]:
        if arg in args:
            path_args[arg] = vars(args)[arg]
        else:
            path_args[arg] = None
    workflow_output_path = paths.workflow_path(**path_args)
    return str(workflow_output_path)
