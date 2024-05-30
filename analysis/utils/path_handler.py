import pathlib


class Paths:

    def __init__(self) -> None:
        # finds the root path as the directory one level upwards of where this file is located
        self.root_path = pathlib.Path(__file__).resolve().parent.parent

    def processor_path(
        self,
        processor: str,
        tagger: str = None,
        flavor: str = None,
        wp: str = None,
        year: str = None,
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
                    tagger,
                    flavor,
                    wp,
                    year,
                ]
                if elem is not None
            ]
        )
        # make output directory
        output_path = self.root_path / "outputs" / processor_path
        if not output_path.exists():
            output_path.mkdir(parents=True)
        return output_path