import os
import pathlib


class Paths:

    def __init__(self) -> None:
        user = os.environ["USER"]
        # HcZZ (2026-11-18): HCZZ_OUTPUTS (hczz_chain/config.sh) sets the outputs base, so a branch production writes where the chain reads (<outputs>/<workflow>/<era>/<dataset>/); default = upstream location
        self.root_path = pathlib.Path(os.environ.get("HCZZ_OUTPUTS") or f"/eos/user/{user[0]}/{user}/higgscharm_outputs")

    def workflow_path(self, workflow: str, year: str, dataset: str) -> pathlib.Path:
        """
        Safely return a path by creating the parent directories to avoid errors when writing to the path.

        Parameters:
        -----------
            path: Path to optionally create and return.
            mkdir: If True, creates the parent directories. If False, it has no effect.

        Returns:
            Input path.
        """
        workflow_path = "/".join(
            [elem for elem in [workflow, year, dataset] if elem is not None]
        )
        # make output directory
        output_path = self.root_path / workflow_path
        if not output_path.exists():
            output_path.mkdir(parents=True)
        return output_path
