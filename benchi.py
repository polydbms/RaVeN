import os
from time import sleep
import importlib
import yaml
import json
import argparse
from hub.deployment.main import Deployer
from hub.utils.configurator import Configurator
from hub.utils.datalocation import DataLocation, DataType
from hub.utils.network import NetworkManager
from hub.utils.preprocess import FileTransporter
from hub.evaluation.main import Evaluator
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
                public_key_path = experiments["public_key_path"]
                ssh_connection = experiments["ssh_connection"]
                workload = experiments["workload"]
                data = experiments["data"] if "data" in experiments else None
                results_folder = experiments["results_folder"]
                resource = {
                    system["name"]: {
                        "system": system["name"],
                        "public_key_path": public_key_path,
                        "ssh_connection": ssh_connection,
                        "workload": workload,
                        "raster": data["raster"],
                        "vector": data["vector"],
                        "results_folder": results_folder
                    }
                    for system in experiments["systems"]
                }
                return resource
            except yaml.YAMLError as exc:
                print(exc)
                return {}

    def __run_tasks(self, resource, vector, raster, repeat):
        system = resource["system"]
        now = datetime.now()
        with open("out.log", "a") as f:
            f.write(f'{system} {now.strftime("%d/%m/%Y %H:%M:%S")} \n')
            f.write(f"--------------------- Pre-Benchmark ------------------- \n")
        if "raster" in resource:
            raster = DataLocation(resource["raster"], data_type=DataType.RASTER)
        if "vector" in resource:
            vector = DataLocation(resource["vector"], data_type=DataType.VECTOR)
        if not raster:
            raise ValueError(
                "Raster directory path was not provided. Either add it to experiments or add to cli --raster option"
            )
        if not vector:
            raise ValueError(
                "Vector directory path was not provided. Either add it to experiments or add to cli --vector option"
            )
        # try:
        #     with open(f"{system}.json") as f:
        #         sys_resource = json.load(f)
        #         ssh_connection_available = (
        #             True if sys_resource["ssh_connection"] else False
        #         )
        # except:
        #     print("Creating resources")
        #     exit(1)
        network_manager = NetworkManager(system)
        transporter = FileTransporter(network_manager)
        transporter.send_configs(CURRENT_PATH, log_time=self.logger)
        if vector:
            print(vector)
            transporter.send_data(vector, log_time=self.logger)
        if raster:
            print(raster)
            transporter.send_data(raster, log_time=self.logger)
        # Give execute permission
        network_manager.run_ssh("chmod +x ~/config/**/*.sh", log_time=self.logger)

        with open("out.log", "a") as f:
            f.write(f"--------------------- Benchmark ------------------- \n")
        with open("out.log", "a") as f:
            f.write(f"Preprocesing data\n")
        print("Preprocesing data")
        network_manager.run_ssh(
            f'~/config/{system}/preprocess.sh "--system {system} --vector_path {vector.docker_dir} --raster_path {raster.docker_dir} --output {raster.docker_dir}"',
            log_time=self.logger,
        )
        print("Wait 30s until docker is ready")
        sleep(30)
        with open("out.log", "a") as f:
            f.write(f"Ingesting data\n")
        print("Ingesting data")
        print(vector, raster)
        Ingestor = self.__importer(f"hub.ingestion.{system}", "Ingestor")
        ingestor = Ingestor(vector, raster, network_manager)
        ingestor.ingest_raster(log_time=self.logger)
        ingestor.ingest_vector(log_time=self.logger)
        with open("out.log", "a") as f:
            f.write(f"Run query\n")
        print("Run query")
        Executor = self.__importer(f"hub.executor.{system}", "Executor")
        executor = Executor(vector, raster, network_manager, resource["results_folder"])
        if repeat:
            for i in range(repeat):
                executor.run_query(resource["workload"], log_time=self.logger)
        else:
            executor.run_query(resource["workload"], log_time=self.logger)
        with open("out.log", "a") as f:
            f.write(f"--------------------- Post-Benchmark ------------------- \n")
            f.write(f'Finished {now.strftime("%d/%m/%Y %H:%M:%S")} \n')

    def benchmark(self, system=None, vector=None, raster=None, repeat=None):
        experiments = self.read_experiments_config()
        if system is not None:
            self.__run_tasks(experiments[system], vector, raster, repeat)
        else:
            for system in experiments:
                self.__run_tasks(experiments[system], vector, raster, repeat)
                self.clean(system)

    def evaluate(self):
        systems_list = list(self.read_experiments_config().keys())
        evaluator = Evaluator(systems_list)
        evaluator.get_accuracy()

    def clean(self, system=None):
        experiments = self.read_experiments_config()
        if system is not None:
            deployer = Deployer(experiments[system])
            deployer.clean_up() #TODO replace
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
    parser.add_argument(
        "--repeat", help="Specify number of iterations an experiment will be repeated"
    )
    args = parser.parse_args()
    setup = Setup()
    if args.command == "start":
        setup.benchmark(args.system, args.vector, args.raster, args.repeat)
        setup.evaluate()
    if args.command == "clean":
        setup.clean(args.system)


if __name__ == "__main__":
    main()
