import yaml
import importlib.util

def load_config(config_type: str, config_name: str, year: str = None):
    """
    load dataset, processor or histogram configuration
    """
    path = f"analysis.configs.{config_type}."
    if config_type == "histogram":
        path += config_name
    else:
        path += f"{year}.{config_name}"
        
    loader = importlib.util.find_spec(path)
    if loader is None:
        raise Exception(
            f"No config file found for the selected {config_type} '{config_name}'"
        )

    config_module = importlib.import_module(path)
    config = getattr(config_module, f"{config_type}_config")
    return config

def load_config_params(year: str):
    with open(f'analysis/configs/config_{year}.yaml', 'r') as file:
        config = yaml.safe_load(file)
    return config