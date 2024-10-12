import os
import pathlib


class Paths:

    def __init__(self) -> None:
        # finds the root path as the directory one level upwards of where this file is located
        user = os.environ["USER"]
        self.root_path = pathlib.Path(f"/pnfs/iihe/cms/store/user/{user}")

    def processor_path(
        self,
        processor: str,
        year: str,
        lepton_flavor: str = None,
        tagger: str = None,
        flavor: str = None,
        wp: str = None,
    ) -> pathlib.Path:
        """
        Safely return a path by creating the parent directories to avoid errors when writing to the path.

        Parameters:
        -----------
            path: Path to optionally create and return.
            mkdir: If True, creates the parent directories. If False, it has no effect.

        Returns:
            Input path.
        """
        processor_path = "/".join(
            [
                elem
                for elem in [
                    processor,
                    lepton_flavor,
                    tagger,
                    flavor,
                    wp,
                    year,
                ]
                if elem is not None
            ]
        )
        # make output directory
        output_path = self.root_path / "higgscharm_outputs" / processor_path
        if not output_path.exists():
            output_path.mkdir(parents=True)
        return output_path