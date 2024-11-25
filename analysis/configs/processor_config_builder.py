import yaml
import importlib.resources
from analysis.histograms import HistogramConfig
from analysis.configs.processor_config import ProcessorConfig


class ProcessorConfigBuilder:
    
    def __init__(self, processor: str, year: str):
        with importlib.resources.open_text(f"analysis.configs.{processor}", f"{year}.yaml") as file:
            self.config = yaml.safe_load(file)
            
    def build_processor_config(self):
        return ProcessorConfig(
            goldenjson=self.config["goldenjson"],
            lumidata=self.config["lumidata"],
            hlt_paths=self.config["hlt_paths"],
            object_selection=self.parse_object_selection(),
            event_selection=self.parse_event_selection(),
            histogram_config=self.parse_histogram_config()
        )
            
    def parse_object_selection(self):
        object_selection = {}
        for object_name in self.config['object_selection']:
            object_selection[object_name] = {"field": self.config['object_selection'][object_name]["field"]}
            if self.config['object_selection'][object_name]["cuts"]:
                object_selection[object_name]["cuts"] = {}
                for cut_name, cut in self.config['object_selection'][object_name]["cuts"].items():
                    object_selection[object_name]["cuts"][cut_name] = cut
        return object_selection
    
    def parse_event_selection(self):
        event_selection = {}
        for cut_name, cut in self.config['event_selection'].items():
            event_selection[cut_name] = cut
        return event_selection
    
    def parse_histogram_config(self):
        return HistogramConfig(**self.config["histogram_config"])