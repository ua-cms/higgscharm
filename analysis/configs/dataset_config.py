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
        partitions: number of partitions when building the dataset
        stepsize: step size to use in the dataset preprocessing
        filenames: Filenames of the ROOT files.
    """

    def __init__(
        self,
        name: str,
        path: str,
        key: str,
        year: str,
        era: str,
        xsec: float,
        partitions: int,
        stepsize: int,
        filenames: tuple,
    ) -> None:
        if path[-1] != "/":
            raise ValueError(f"Dataset path has to end with '/'. Got: {path}")

        super().__init__(name=name)

        self.name = name
        self.path = path
        self.key = key
        self.year = year
        self.era = era
        self.xsec = xsec
        self.partitions = partitions
        self.stepsize = stepsize
        self.filenames = filenames

    def __repr__(self):
        return f"DatasetConfig({self.name}, {self.year}, {self.stepsize})"