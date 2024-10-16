class DatasetConfig:
    """
    Container for information about a dataset.

    Attributes:
    -----------
        name: 
            short name of the dataset
        process: 
            physical process class (used as a key to accumulate datasets in postprocessing)
        path: 
            path to the dataset
        key: 
            key of the TTree in the ROOT file
        year: 
            year of the dataset
        is_mc: 
            Is the dataset MC?
        xsec: 
            dataset cross section
        stepsize: 
            step size to use in the dataset preprocessing
    """
    def __init__(
        self,
        name: str,
        process: str,
        path: str,
        key: str,
        year: str,
        era: str,
        xsec: float,
        stepsize: int,
    ) -> None:
        if path[-1] != "/":
            raise ValueError(f"Dataset path has to end with '/'. Got: {path}")

        self.name = name
        self.process = process
        self.path = path
        self.key = key
        self.year = year
        self.era = era
        self.xsec = xsec
        self.stepsize = stepsize

    def __repr__(self):
        return f"DatasetConfig({self.name}, {self.year}, {self.stepsize})"