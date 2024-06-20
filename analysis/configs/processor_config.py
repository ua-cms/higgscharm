class ProcessorConfig:
    """
    Attributes:
    -----------
        lumimask:
        hlt_paths:
        selection:
    """
    def __init__(
        self, lumimask: str, lumidata: str, hlt_paths: list[str], selection: dict
    ):
        self.lumimask = lumimask
        self.lumidata = lumidata
        self.hlt_paths = hlt_paths
        self.selection = selection