import argparse
import importlib
from datetime import datetime
from pathlib import Path
from time import sleep

from hub.benchmarkrun.measurementslocation import MeasurementsLocation
from hub.evaluation.main import Evaluator
from hub.benchmarkrun.benchmark_run import BenchmarkRun
from hub.utils.fileio import FileIO
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager
from hub.utils.system import System


class Setup:
    def __init__(self) -> None:
        self.logger = {}

    @staticmethod
    def __importer(module, class_name):
        """Dynamic Importing"""
        module = importlib.import_module(module)
        return getattr(module, class_name)

    def __run_tasks(self, run: BenchmarkRun) -> list[Path]:
        system: System = run.benchmark_params.system
        print(system)

        now = datetime.now()
        with open("out.log", "a") as f:
            f.write(f'{system} {now.strftime("%d/%m/%Y %H:%M:%S")} \n')
            f.write(f"--------------------- Pre-Benchmark ------------------- \n")

        network_manager = NetworkManager(run.host_params, run.benchmark_params.system.name, run.measurements_loc)
        transporter = FileTransporter(network_manager)

        transporter.send_configs(log_time=self.logger)
        print(run.vector)
        transporter.send_data(run.vector, log_time=self.logger)

        print(run.raster)
        transporter.send_data(run.raster, log_time=self.logger)
        # Give execute permission
        network_manager.run_ssh(f"chmod +x {run.host_params.host_base_path.joinpath('config/**/*.sh')}",
                                log_time=self.logger)

        with open("out.log", "a") as f:
            f.write(f"--------------------- Benchmark ------------------- \n")
        with open("out.log", "a") as f:
            f.write(f"Preprocessing data\n")
        print("Preprocessing data")

        network_manager.start_measure_docker("preprocess", prerecord=False)
        command = run.host_params.host_base_path.joinpath(f'config/{system}/preprocess.sh')

        network_manager.run_ssh(
            f'{command} "--system {system} '
            f'--vector_path {run.vector.docker_dir} '
            f'--vector_target_suffix {run.benchmark_params.vector_target_format.value} '
            f'--vector_output_folder {run.vector.docker_dir_preprocessed} '
            f'--vector_target_crs {run.benchmark_params.vector_target_crs.to_epsg()} '
            f'--vectorization_type {run.benchmark_params.vectorize_type.value} '
            f'--raster_path {run.raster.docker_dir} '
            f'--raster_target_suffix {run.benchmark_params.raster_target_format.value} '
            f'--raster_output_folder {run.raster.docker_dir_preprocessed} '
            f'--raster_target_crs {run.benchmark_params.raster_target_crs.to_epsg()} '
            f'"',
            log_time=self.logger,
        )
        run.raster.set_preprocessed()
        run.vector.set_preprocessed()
        network_manager.stop_measure_docker()
        print("Wait 15s until docker is ready")
        sleep(15)

        with open("out.log", "a") as f:
            f.write(f"Ingesting data\n")
        print("Ingesting data")

        network_manager.start_measure_docker("ingestion")
        Ingestor = self.__importer(f"hub.ingestion.{system}", "Ingestor")
        ingestor = Ingestor(run.vector, run.raster, network_manager, run.benchmark_params)
        ingestor.ingest_raster(log_time=self.logger)
        ingestor.ingest_vector(log_time=self.logger)
        network_manager.stop_measure_docker()

        with open("out.log", "a") as f:
            f.write(f"Run query\n")
        print("Run query")

        network_manager.start_measure_docker("execution")
        Executor = self.__importer(f"hub.executor.{system}", "Executor")
        executor = Executor(run.vector, run.raster, network_manager)

        result_files: list[Path] = []
        if run.benchmark_params.iterations:
            for i in range(run.benchmark_params.iterations):
                result_files.append(executor.run_query(run.workload, log_time=self.logger))
        else:
            result_files.append(executor.run_query(run.workload, log_time=self.logger))

        with open("out.log", "a") as f:
            f.write(f"--------------------- Post-Benchmark ------------------- \n")
            f.write(f'Finished {now.strftime("%d/%m/%Y %H:%M:%S")} \n')

        network_manager.stop_measure_docker()

        return result_files

    def benchmark(self, experiment_file_name, system=None, post_cleanup=True) -> list[Path]:
        runs = FileIO.read_experiments_config(experiment_file_name, system)
        # print([str(r.benchmark_params) for r in runs])

        result_files = []
        if system:
            run = next(r for r in runs if r.benchmark_params.system.name == system)
            print(str(run))
            result_files.extend(self.__run_tasks(run))
            if post_cleanup:
                self.clean(experiment_file_name)
        else:
            for r in runs:
                result_files.extend(self.__run_tasks(r))
                self.clean(experiment_file_name)

        return result_files

    def evaluate(self, experiment_file_name, result_files: list[Path]):
        systems_list = FileIO.get_systems(experiment_file_name)
        host_params = FileIO.get_host_params(experiment_file_name)
        print(result_files)
        evaluator = Evaluator(systems_list, result_files, host_params)
        evaluator.get_accuracy()

    def clean(self, experiment_file_name: str):
        host_params = FileIO.get_host_params(experiment_file_name)

        network_manager = NetworkManager(host_params, "cleanup", None)
        network_manager.run_ssh("""kill $(ps aux | grep "docker stats" | awk {\\'print $2\\'} )""")
        network_manager.run_ssh("docker stop $(docker ps -q)")
        network_manager.run_ssh("docker rm $(docker ps -aq)")
        network_manager.run_ssh("docker volume rm $(docker volume ls -q)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Use either start or clean.")
    parser.add_argument("--system", help="Specify which system should be benchmarked")
    parser.add_argument("--vector", help="Specify the path to vector dataset")
    parser.add_argument("--raster", help="Specify the path to raster dataset")
    parser.add_argument("--experiment", help="Specify the path to the experiment definition file", required=True)
    parser.add_argument("--repeat", help="Specify number of iterations an experiment will be repeated")
    parser.add_argument("--postcleanup",
                        help="Whether to run a cleanup after running the benchmark. Only works together with '--system <system>'",
                        action=argparse.BooleanOptionalAction,
                        default=True)
    args = parser.parse_args()
    print(args)
    setup = Setup()
    if args.command == "start":
        result_files = setup.benchmark(args.experiment, args.system, args.postcleanup)

        if len(result_files) > 1:
            setup.evaluate(args.experiment, result_files)
    if args.command == "clean":
        setup.clean(args.experiment)


if __name__ == "__main__":
    main()
