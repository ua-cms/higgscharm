class DatasetConfig:
    """
    Container for information about a dataset.

    Attributes:
    -----------
        name: 
            short name of the dataset
        path: 
            path to the dataset
        key: 
            key of the TTree in the ROOT file.
        year: 
            year of the dataset
        is_mc: 
            Is the dataset MC?
        xsec: 
            dataset cross section
        partitions: 
            number of partitions when building the dataset
        stepsize: 
            step size to use in the dataset preprocessing
        filenames: 
            names of the ROOT files
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