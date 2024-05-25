class HistogramConfig:
    """
    config class for a Histogram
     
    Attributes:
    -----------
        individual: 
            if True each axis will be an individual histogram, otherwise all axes will be in the same histogram
        add_syst_axis:
            if True histograms will include a StrCategory axis for systematics
        add_weight:
            if True hist.storage.Weight() will be added to the histograms
        names:
            it provided, it will create a histogram for each name
        axes:
            dictionary with axis info with {"type": <type>, **args} structure. 
            <type> is a hist.axis type: Regular, IntCategory, StrCategory, Variable
            
            Example:
                axes = {
                    "cjet_pt": {
                        "type": "Regular", 
                        "bins": 30, 
                        "start": 30, 
                        "stop": 200, 
                        "label": r"Jet $p_T$ [GeV]"
                    },
                    "njets": {
                        "type": "IntCategory", 
                        "categories": np.arange(0, 16),
                        "label": "$N_{jets}$"
                    }
                }
    """
    def __init__(
        self, 
        individual: bool,
        add_syst_axis: bool,
        add_weight: bool,
        names: list[str],
        axes: dict,
    ):
        self.individual = individual
        self.add_syst_axis = add_syst_axis
        self.add_weight = add_weight
        self.names = names
        self.axes = axes