import yaml
from pathlib import Path
from xrootd_sites import xroot_to_site


if __name__ == "__main__":
    data = {
        "black": [],
        "white": sorted(set(xroot_to_site.values()))
    }
    sites_file = Path.cwd() / "analysis" / "filesets" / "sites.yaml"
    with open(sites_file, "w") as f:
        yaml.dump(data, f, default_flow_style=False)