import yaml
import importlib.resources
from analysis.histograms import HistogramConfig
from analysis.configs.processor_config import ProcessorConfig


class ProcessorConfigBuilder:

    def __init__(self, processor: str, year: str):
        with importlib.resources.open_text(
            f"analysis.configs.{processor}", f"{processor}.yaml"
        ) as file:
            self.config = yaml.safe_load(file)

    def build_processor_config(self):
        return ProcessorConfig(
            object_selection=self.parse_object_selection(),
            event_selection=self.parse_event_selection(),
            corrections_config=self.parse_corrections_config(),
            histogram_config=self.parse_histogram_config(),
        )

    def parse_object_selection(self):
        object_selection = {}
        for object_name in self.config["object_selection"]:
            object_selection[object_name] = {
                "field": self.config["object_selection"][object_name]["field"]
            }
            if "cuts" in self.config["object_selection"][object_name]:
                object_selection[object_name]["cuts"] = self.config["object_selection"][
                    object_name
                ]["cuts"]
            if "add_cut" in self.config["object_selection"][object_name]:
                cuts_to_add = self.config["object_selection"][object_name]["add_cut"]
                object_selection[object_name]["add_cut"] = {}
                for cut_name, cuts in cuts_to_add.items():
                    object_selection[object_name]["add_cut"][cut_name] = cuts
        return object_selection

    def parse_event_selection(self):
        event_selection = {}
        for cut_name, cut in self.config["event_selection"].items():
            event_selection[cut_name] = cut
        return event_selection

    def parse_histogram_config(self):
        hist_config = HistogramConfig(**self.config["histogram_config"])
        hist_config.categories = list(self.parse_event_selection()["categories"].keys())
        return hist_config

    def parse_corrections_config(self):
        corrections = {}
        corrections["objects"] = self.config["corrections"]["objects"]
        corrections["event_weights"] = {}
        for name, vals in self.config["corrections"]["event_weights"].items():
            if isinstance(vals, bool):
                corrections["event_weights"][name] = vals
            elif isinstance(vals, list):
                corrections["event_weights"][name] = {}
                for val in vals:
                    for corr, wp in val.items():
                        corrections["event_weights"][name][corr] = wp
        return corrections
