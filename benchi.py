import argparse
import importlib
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import List, Any

from configuration import PROJECT_ROOT
from hub.deployment.main import Deployer
from hub.evaluation.main import Evaluator
from hub.utils.datalocation import DataLocation, DataType
from hub.utils.fileio import FileIO
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager


class Setup:
    def __init__(self) -> None:
        self.logger = {}

    @staticmethod
    def __importer(module, class_name):
        """Dynamic Importing"""
        module = importlib.import_module(module)
        return getattr(module, class_name)

    def __run_tasks(self, resource, vector, raster, repeat) -> list[Path]:
        system = resource["system"]
        print(system)
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

        network_manager = NetworkManager(system)
        transporter = FileTransporter(network_manager)
        transporter.send_configs(log_time=self.logger)
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

        result_files: list[Path] = []
        if repeat:
            for i in range(repeat):
                result_files.append(executor.run_query(resource["workload"], log_time=self.logger))
        else:
            result_files.append(executor.run_query(resource["workload"], log_time=self.logger))
        with open("out.log", "a") as f:
            f.write(f"--------------------- Post-Benchmark ------------------- \n")
            f.write(f'Finished {now.strftime("%d/%m/%Y %H:%M:%S")} \n')

        return result_files

    def benchmark(self, system=None, vector=None, raster=None, repeat=None) -> list[Path]:
        experiments = FileIO.read_experiments_config()

        result_files = []
        if system is not None:
            result_files.extend(self.__run_tasks(experiments[system], vector, raster, repeat))
        else:
            for system in experiments:
                result_files.extend(self.__run_tasks(experiments[system], vector, raster, repeat))
                # self.clean(system) # TODO replace

        return result_files

    def evaluate(self, result_files: list[Path]):
        config = FileIO.read_experiments_config()
        systems_list = list(config.keys())
        print(result_files)
        evaluator = Evaluator(systems_list, result_files, config[systems_list[0]]["results_folder"])
        evaluator.get_accuracy()

    def clean(self, system=None):
        experiments = FileIO.read_experiments_config()

        # if system is not None:
        #     network_manager = NetworkManager(system)
        #     deployer = clea(experiments[system])
        #     deployer.clean_up()  # TODO replace
        # else:
        for system in experiments:
            network_manager = NetworkManager(system)
            network_manager.run_ssh("docker stop $(docker ps -q)")
            network_manager.run_ssh("docker rm $(docker ps -aq)")
            network_manager.run_ssh("docker volume rm $(docker volume ls -q)")


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
        result_files = setup.benchmark(args.system, args.vector, args.raster, args.repeat)
        setup.evaluate(result_files)
    if args.command == "clean":
        setup.clean(args.system)


if __name__ == "__main__":
    main()
