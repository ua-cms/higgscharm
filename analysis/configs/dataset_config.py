from analysis.configs.config import Config

class DatasetConfig(Config):
    """
    Container for information about a dataset.

    Attributes:
        name: The name of the dataset.
        path: The path to the dataset.
        key: The key of the TTree in the ROOT file.
        year: The year of the dataset
        is_mc: Is the dataset MC or not
        xsec: The cross section of the dataset
        filenames: Filenames of the ROOT files.
    """

    def __init__(
        self,
        name: str,
        path: str,
        key: str,
        year: str,
        is_mc: bool,
        xsec: float,
        filenames: tuple[str],
    ) -> None:
        if path[-1] != "/":
            raise ValueError(f"Dataset path has to end with '/'. Got: {path}")

        super().__init__(name=name)

        self.path = path
        self.key = key
        self.filenames = filenames
        self.is_mc = is_mc
        self.xsec = xsec