class HistogramConfig:
    """
    config class for a Histogram

    Attributes:
    -----------
        axes:
                dictionary with axis info like {"type": <type>, **args}
                <type> is a hist.axis type: Regular, IntCategory, StrCategory, Variable

                Example:
                    axes = {
                        "jet_pt": {
                            "type": "Regular",
                            "bins": 30,
                            "start": 30,
                            "stop": 200,
                            "label": r"Jet $p_T$ [GeV]"
                        },
                        "jet_eta": {
                            "type": "Regular",
                            "bins": 50,
                            "start": -2.5,
                            "stop": 2.5,
                            "label": "Jet $\eta$",
                        },
                        "njets": {
                            "type": "IntCategory",
                            "categories": np.arange(0, 16),
                            "label": "$N_{jets}$"
                        }
                    }
        layout:
            if "individual", when building the histogram, each axis will be an individual histogram:
                {'jet_pt': hist.Hist(jet_pt_axis), 'jet_eta': hist.Hist(jet_eta_axis), 'njets': hist.Hist(njets__axis)}
            if a dict like {'jet': ['jet_pt', 'jet_eta'], 'njets': ['njets']}, when building the histogram, each key will contain its values as axes:
                {'jet': hist.Hist(jet_pt_axis, jet_eta_axis), 'njets': hist.Hist(njets_axis)}
        add_syst_axis:
            if True histograms will include a StrCategory axis for systematics
        add_weight:
            if True hist.storage.Weight() will be added to the histograms

    """

    def __init__(
        self,
        axes: dict,
        layout: dict,
        add_syst_axis: bool,
        add_weight: bool,
        add_cat_axis = None,
    ):
        self.axes = axes
        self.layout = layout
        self.add_syst_axis = add_syst_axis
        self.add_weight = add_weight
        self.add_cat_axis = add_cat_axis

    def to_dict(self):
        """Convert HistogramConfig to a dictionary."""
        return {
            "add_syst_axis": self.add_syst_axis,
            "add_weight": self.add_weight,
            "add_cat_axis": self.add_cat_axis,
            "axes": self.axes,
            "layout": self.layout,
        }
