import os
from time import sleep
import importlib
import yaml
import json
import argparse
from hub.deployment.main import Deployer
from hub.utils.configurator import Configurator
from hub.utils.network import NetworkManager
from hub.utils.preprocess import FileTransporter
from datetime import datetime

CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))


class Setup:
    def __init__(self) -> None:
        self.logger = {}

    @staticmethod
    def __importer(module, class_name):
        """Dynamic Importing"""
        module = importlib.import_module(module)
        return getattr(module, class_name)

    @staticmethod
    def read_experiments_config():
        with open(f"{CURRENT_PATH}/experiements.yaml") as c:
            try:
                experiments = yaml.safe_load(c)["experiments"]
                project_id = experiments["project_id"]
                public_key_path = experiments["public_key_path"]
                machine_size = experiments["machine_size"]
                workload = experiments["workload"]
                data = experiments["data"] if "data" in experiments else None
                resource = {
                    system["name"]: {
                        "system": system["name"],
                        "project_id": project_id,
                        "public_key_path": public_key_path,
                        "workload": workload,
                        "variables[TF_VAR_resource_group_name]": system["name"],
                        "variables[TF_VAR_machine_size]": machine_size,
                        "variables[TF_VAR_extra_port]": system["port"]
                        if "port" in system
                        else 80,
                        "raster": data["raster"],
                        "vector": data["vector"],
                    }
                    for system in experiments["systems"]
                }
                return resource
            except yaml.YAMLError as exc:
                print(exc)
                return {}

    def __run_tasks(self, resource, vector, raster):
        system = resource["system"]
        now = datetime.now()
        with open("out.log", "a") as f:
            f.write(f'{system} {now.strftime("%d/%m/%Y %H:%M:%S")} \n')
            f.write(f"--------------------- Pre-Benchmark ------------------- \n")
        if "raster" in resource:
            raster = resource["raster"]
        if "vector" in resource:
            vector = resource["vector"]
        if not raster:
            raise ValueError(
                "Raster directory path was not provided. Either add it to experiemnts or add to cli --raster option"
            )
        if not vector:
            raise ValueError(
                "Vector directory path was not provided. Either add it to experiemnts or add to cli --vector option"
            )
        try:
            with open(f"{system}.json") as f:
                sys_resource = json.load(f)
                ssh_connection_available = (
                    True if sys_resource["ssh_connection"] else False
                )
        except:
            print("Creating resources")
            ssh_connection_available = False
        if not ssh_connection_available:
            deployer = Deployer(resource)
            print(f"Deploying {system}")
            deployer.deploy(log_time=self.logger)
        network_manager = NetworkManager(system)
        print(f"Configuring {system}")
        configurator = Configurator(system)
        configurator.run_instructions(log_time=self.logger)
        transporter = FileTransporter(network_manager)
        transporter.send_configs(CURRENT_PATH, log_time=self.logger)
        if vector:
            transporter.send_data(vector, log_time=self.logger)
        if raster:
            transporter.send_data(raster, log_time=self.logger)
        # Give execute permission
        network_manager.run_ssh("chmod +x ~/config/*.sh", log_time=self.logger)

        with open("out.log", "a") as f:
            f.write(f"--------------------- Benchmark ------------------- \n")
        with open("out.log", "a") as f:
            f.write(f"Preprocesing data\n")
        print("Preprocesing data")
        network_manager.run_ssh(
            f'~/config/preprocess.sh "--system {system} --vector_path {vector} --raster_path {raster} --output {raster}"',
            log_time=self.logger,
        )
        print("Wait 30s until docker is ready")
        sleep(30)
        with open("out.log", "a") as f:
            f.write(f"Ingesting data\n")
        print("Ingesting data")
        Ingestor = self.__importer(f"hub.ingestion.{system}", "Ingestor")
        ingestor = Ingestor(vector, raster, network_manager)
        ingestor.ingest_raster(log_time=self.logger)
        ingestor.ingest_vector(log_time=self.logger)
        with open("out.log", "a") as f:
            f.write(f"Run query\n")
        print("Run query")
        Executor = self.__importer(f"hub.executor.{system}", "Executor")
        executor = Executor(vector, raster, network_manager)
        executor.run_query(resource["workload"], log_time=self.logger)
        with open("out.log", "a") as f:
            f.write(f"--------------------- Post-Benchmark ------------------- \n")
            f.write(f'Finished {now.strftime("%d/%m/%Y %H:%M:%S")} \n')

    def benchmark(self, system=None, vector=None, raster=None):
        experiments = self.read_experiments_config()
        if system is not None:
            self.__run_tasks(experiments[system], vector, raster)
        else:
            for system in experiments:
                self.__run_tasks(experiments[system], vector, raster)
                self.clean(system)

    def clean(self, system=None):
        experiments = self.read_experiments_config()
        if system is not None:
            deployer = Deployer(experiments[system])
            deployer.clean_up()
        else:
            for system in experiments:
                deployer = Deployer(experiments[system])
                deployer.clean_up()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Use either start or clean.")
    parser.add_argument("--system", help="Specify which system should be benchmarked")
    parser.add_argument("--vector", help="Specify the path to vector dataset")
    parser.add_argument("--raster", help="Specify the path to raster dataset")
    args = parser.parse_args()
    setup = Setup()
    if args.command == "start":
        setup.benchmark(args.system, args.vector, args.raster)
    if args.command == "clean":
        setup.clean(args.system)


if __name__ == "__main__":
    main()
