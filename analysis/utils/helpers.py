import awkward as ak


def add_cutflow(events, output, selection_manager, workflow_config):
    """Add cutflow to output metadata"""
    sumw = ak.sum(events.genWeight) if hasattr(events, "genWeight") else len(events)
    for category, category_cuts in workflow_config.event_selection[
        "categories"
    ].items():
        output["metadata"].update({category: {"cutflow": {"initial": sumw}}})
        selections = []
        for cut_name in category_cuts:
            selections.append(cut_name)
            current_selection = selection_manager.all(*selections)
            if ak.sum(current_selection) != 0:
                sumw_cutflow = (
                    ak.sum(events.genWeight[current_selection])
                    if hasattr(events, "genWeight")
                    else len(events[current_selection])
                )
                output["metadata"][category]["cutflow"][cut_name] = sumw_cutflow
            else:
                output["metadata"][category]["cutflow"][cut_name] = 0


def update(events, collections):
    """Return a shallow copy of events array with some collections swapped out"""
    out = events
    for name, value in collections.items():
        out = ak.with_field(out, value, name)
    return out
