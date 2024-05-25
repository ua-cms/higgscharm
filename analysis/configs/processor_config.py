class ProcessorConfig:
    """
    Attributes:
    -----------
        lumimask:
        hlt_paths:
        selection:
    """
    def __init__(
        self, lumimask: str, hlt_paths: list[str], selection: dict
    ):
        self.lumimask = lumimask
        self.hlt_paths = hlt_paths
        self.selection = selection