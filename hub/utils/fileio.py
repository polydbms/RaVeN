import os
from pathlib import Path

import yaml

from configuration import PROJECT_ROOT
from hub.utils.system import System


class FileIO:

    @staticmethod
    def read_experiments_config():
        with open(PROJECT_ROOT.joinpath("experiements.yaml")) as c:
            try:
                yamlfile = yaml.safe_load(c)
                experiments = yamlfile["experiments"]
                public_key_path = experiments["public_key_path"]
                ssh_connection = experiments["ssh_connection"]
                workload = experiments["workload"]
                data = experiments["data"] if "data" in experiments else None
                host_base_path = experiments["host_base_path"]
                results_folder = experiments["results_folder"]
                resource = {
                    system["name"]: {
                        "system": System(system["name"],
                                         public_key_path,
                                         ssh_connection,
                                         host_base_path),
                        "workload": workload,
                        "raster": data["raster"],
                        "vector": data["vector"],
                        "results_folder": Path(results_folder).expanduser()
                    }
                    for system in experiments["systems"]
                }
                return resource
            except yaml.YAMLError as exc:
                print(exc)
                return {}

    @staticmethod
    def read_capabilities():
        with open(PROJECT_ROOT.joinpath("capabilities.yaml")) as c:
            try:
                return yaml.safe_load(c)["systems"]
            except yaml.YAMLError as exc:
                print(exc)
                return {}
