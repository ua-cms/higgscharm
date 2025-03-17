import yaml


class ProcessorConfig:
    """
    Attributes:
    -----------
        object_selection:
        event_selection:
        corrections_config:
        histogram_config:
    """

    def __init__(
        self,
        object_selection,
        event_selection,
        corrections_config,
        histogram_config,
    ):
        self.object_selection = object_selection
        self.event_selection = event_selection
        self.corrections_config = corrections_config
        self.histogram_config = histogram_config

    def to_dict(self):
        """Convert ProcessorConfig to a dictionary."""
        return {
            "object_selection": self.object_selection,
            "event_selection": self.event_selection,
            "corrections_config": self.corrections_config,
            "histogram_config": self.histogram_config.to_dict(),
        }

    def to_yaml(self):
        """Convert ProcessorConfig to a YAML string."""
        return yaml.dump(self.to_dict(), sort_keys=False, default_flow_style=False)
