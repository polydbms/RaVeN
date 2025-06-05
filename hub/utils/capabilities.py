import yaml

from hub.configuration import PROJECT_ROOT


class Capabilities:
    """
    a wrapper for loading the capabilities.yaml file
    """
    @staticmethod
    def read_capabilities() -> dict:
        with open(PROJECT_ROOT.joinpath("capabilities.yaml"), mode="r") as c:
            try:
                return yaml.safe_load(c)["systems"]
            except yaml.YAMLError as exc:
                print(exc)
                return {}
