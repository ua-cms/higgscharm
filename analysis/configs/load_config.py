import importlib.util

def load_config(config_type: str, config_name: str, year: str):
    """
    load dataset or processor configuration
    """
    path = f"analysis.configs.{config_type}.{year}.{config_name}"
    loader = importlib.util.find_spec(path)

    if loader is None:
        raise Exception(
            f"No config file found for the selected {config_type} '{config_name}'"
        )

    config_module = importlib.import_module(path)
    config = getattr(config_module, f"{config_type}_config")
    return config