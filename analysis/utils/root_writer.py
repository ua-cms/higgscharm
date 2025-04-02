import pickle
from analysis.workflows import WorkflowConfigBuilder


def write_root(out, save_path, args):
    import uproot
    # save metadata
    with open(f"{save_path}.pkl", "wb") as handle:
        pickle.dump(out["metadata"], handle, protocol=pickle.HIGHEST_PROTOCOL)
    # save 1D histograms
    config_builder = WorkflowConfigBuilder(workflow=args.workflow, year=args.year)
    workflow_config = config_builder.build_workflow_config()
    categories = workflow_config.event_selection["categories"]
    with uproot.recreate(f"{save_path}.root") as f:
        for category in categories:
            for histogram in out["histograms"].values():
                category_histogram = histogram[{"category": category}]
                variables = [
                    v for v in category_histogram.axes.name if v != "variation"
                ]
                for variable in variables:
                    for syst_var in category_histogram.axes["variation"]:
                        variation_histogram = category_histogram[
                            {"variation": syst_var}
                        ]
                        f[f"{category}_{variable}_{syst_var}"] = (
                            variation_histogram.project(variable)
                        )