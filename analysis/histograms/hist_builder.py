import hist

class HistBuilder:
    def __init__(self, histogram_config):
        self.histogram_config = histogram_config
        self.axis_opt = {
            "StrCategory": hist.axis.StrCategory,
            "IntCategory": hist.axis.IntCategory,
            "Regular": hist.axis.Regular,
            "Variable": hist.axis.Variable,
        }
        self.syst_axis = self.build_axis(
            {"variation": {"type": "StrCategory", "categories": [], "growth": True}}
        )
        if self.histogram_config.add_cat_axis:
            self.cat_axis = self.build_axis(self.histogram_config.add_cat_axis)
            

    def build_axis(self, axis_config: dict):
        """build a hist axis object from an axis config"""
        axis_args = {}
        for name in axis_config:
            axis_args["name"] = name
            hist_type = axis_config[name]["type"]
            for arg_name, arg_value in axis_config[name].items():
                if arg_name in ["type", "expression"]:
                    continue
                axis_args[arg_name] = arg_value
        hist_args = {k: v for k, v in axis_args.items()}
        axis = self.axis_opt[hist_type](**hist_args)
        return axis

    def build_individual_histogram(self):
        histograms = {}
        for name, args in self.histogram_config.axes.items():
            axes = [self.build_axis({name: args})]
            if self.histogram_config.add_syst_axis:
                axes.append(self.syst_axis)
            if self.histogram_config.add_cat_axis:
                axes.append(self.cat_axis)
            if self.histogram_config.add_weight:
                axes.append(hist.storage.Weight())
            
            histograms[name] = hist.Hist(*axes)
        return histograms
    
    def build_stacked_histogram(self, axes_names):
        axes = []
        for name, args in self.histogram_config.axes.items():
            if name in axes_names:
                axes.append(self.build_axis({name: args}))
        if self.histogram_config.add_syst_axis:
            axes.append(self.syst_axis)
        if self.histogram_config.add_cat_axis:
            axes.append(self.cat_axis)
        if self.histogram_config.add_weight:
            axes.append(hist.storage.Weight())
        histograms = hist.Hist(*axes)
        return histograms

    def build_histogram(self):
        if self.histogram_config.layout == "individual":
            histograms = self.build_individual_histogram()
        else:
            histograms = {}
            for hist_name, axes_names in self.histogram_config.layout.items():
                histograms[hist_name] = self.build_stacked_histogram(axes_names)

        return histograms