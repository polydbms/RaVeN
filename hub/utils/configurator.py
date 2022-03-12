import yaml
from hub.evaluation.main import measure_time
from hub.utils.network import NetworkManager


class Configurator:
    def __init__(self, system) -> None:
        self.system = system
        self.network_manager = NetworkManager(self.system)

    def __get_instructions(self):
        with open(f"hub/deployment/configuration/{self.system}.yaml") as c:
            try:
                config = yaml.safe_load(c)["config"]
                scripts = config["scripts"]
                return scripts
            except yaml.YAMLError as exc:
                print(exc)

    @measure_time
    def run_instructions(self, **kwargs):
        instructions = self.__get_instructions()
        instruction_string = f'{"; ".join(instructions)}'
        self.network_manager.run_ssh(instruction_string)
