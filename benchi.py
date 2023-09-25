import argparse
import importlib
from datetime import datetime
from pathlib import Path
from time import sleep

from hub.benchmarkrun.benchmark_run import BenchmarkRun
from hub.enums.stage import Stage
from hub.evaluation.main import Evaluator
from hub.utils.fileio import FileIO
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager
from hub.utils.system import System


class Setup:
    """
    the main utility containing all benchi-related routines
    """
    def __init__(self) -> None:
        self.logger = {}

    @staticmethod
    def __importer(module, class_name):
        """Dynamic Importing"""
        module = importlib.import_module(module)
        return getattr(module, class_name)

    def __run_tasks(self, run: BenchmarkRun, iteration=1) -> list[Path]:
        """
        performs a single benchmark run. the following stages are executed:

        1. setup including sending config and datasets to the host
        2. preprocess stage
        3. ingestion stage
        4. execution stage, as often as warm starts are specified + 1 cold start
        5. teardown
        6. transferring results to the database
        :param run: the benchmark run
        :param iteration: the iteration of the run
        :return:
        """
        system: System = run.benchmark_params.system
        print(system)

        """
        Setup
        """

        run_cursor = run.host_params.controller_db_connection.initialize_benchmark_run(run.benchmark_params, iteration)

        now = datetime.now()
        with open("out.log", "a") as f:
            f.write(f'{system} {now.strftime("%d/%m/%Y %H:%M:%S")} \n')
            f.write(f"--------------------- Pre-Benchmark ------------------- \n")

        network_manager = NetworkManager(run.host_params, run.benchmark_params.system.name, run.measurements_loc,
                                         run_cursor, run.query_timeout)
        transporter = FileTransporter(network_manager)

        """
        transfer configs and datasets to the host
        """

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

        """
        Preprocess stage
        """

        network_manager.start_measure_docker("preprocess", prerecord=False)
        command = run.host_params.host_base_path.joinpath(f'config/{system}/preprocess.sh')

        vector_target_crs = run.benchmark_params.vector_target_crs.to_epsg() \
            if run.benchmark_params.align_crs_at_stage == Stage.PREPROCESS \
            else run.vector.get_crs().to_epsg()
        raster_target_crs = run.benchmark_params.raster_target_crs.to_epsg() \
            if run.benchmark_params.align_crs_at_stage == Stage.PREPROCESS \
            else run.raster.get_crs().to_epsg()

        network_manager.run_ssh(
            f'{command} "--system {system} '
            f'--vector_path {run.vector.docker_dir} '
            f'--vector_target_suffix {run.benchmark_params.vector_target_format.value} '
            f'--vector_output_folder {run.vector.docker_dir_preprocessed} '
            f'--vector_target_crs {vector_target_crs} '
            f'--vectorization_type {run.benchmark_params.vectorize_type.value} '
            f'--raster_path {run.raster.docker_dir} '
            f'--raster_target_suffix {run.benchmark_params.raster_target_format.value} '
            f'--raster_output_folder {run.raster.docker_dir_preprocessed} '
            f'--raster_target_crs {raster_target_crs} '
            f'"',
            log_time=self.logger,
        )
        run.raster.set_preprocessed()
        run.vector.set_preprocessed()
        network_manager.stop_measure_docker()
        print("Wait 5s until docker is ready")
        sleep(5)

        """
        Ingestion stage
        """

        with open("out.log", "a") as f:
            f.write(f"Ingesting data\n")
        print("Ingesting data")

        network_manager.start_measure_docker("ingestion")
        Ingestor = self.__importer(f"hub.ingestion.{system}", "Ingestor")
        ingestor = Ingestor(run.vector, run.raster, network_manager, run.benchmark_params, run.workload)
        ingestor.ingest_raster(log_time=self.logger)
        ingestor.ingest_vector(log_time=self.logger)
        network_manager.stop_measure_docker()

        """
        Execution stage
        """

        with open("out.log", "a") as f:
            f.write(f"Run query\n")
        print("Run query")

        network_manager.init_timings_sync_marker(run.benchmark_params.system.name)
        network_manager.start_measure_docker("execution")
        Executor = self.__importer(f"hub.executor.{system}", "Executor")
        executor = Executor(run.vector, run.raster, network_manager, run.benchmark_params)

        result_files: list[Path] = []

        # cold start
        print(f"running cold start for parameters {run.benchmark_params}")
        network_manager.add_meta_marker_start(0)
        result_files.append(executor.run_query(run.workload, warm_start_no=0, log_time=self.logger))
        network_manager.add_meta_marker_end()

        # i warm starts
        for i in range(1, run.warm_starts + 1):
            sleep(10)
            print(f"running warm start {i} out of {run.warm_starts} for parameters {run.benchmark_params}")
            network_manager.add_meta_marker_start(i)
            result_files.append(executor.run_query(run.workload, warm_start_no=i, log_time=self.logger))
            network_manager.add_meta_marker_end()

        """
        Cleanup
        """

        with open("out.log", "a") as f:
            f.write(f"--------------------- Post-Benchmark ------------------- \n")
            f.write(f'Finished {now.strftime("%d/%m/%Y %H:%M:%S")} \n')

        network_manager.stop_measure_docker()

        transporter.get_measurements(run.measurements_loc)

        executor.post_run_cleanup()

        """
        transfer results to database
        """

        run_cursor.add_resource_utilization(
            [run.measurements_loc.controller_measurements_folder.joinpath(f"{e.value}.csv") for e in list(Stage)]
        )

        result_files_emptiness_info = list(map(run_cursor.add_results_file, result_files))
        result_files_not_empty = list(filter(lambda r: r[1], result_files_emptiness_info))

        return list(map(lambda resfile: resfile[0], result_files_not_empty))

    def benchmark(self, experiment_file_name: str, config_file: str, system=None, post_cleanup=True,
                  single_run=True) -> list[Path]:
        """
        anchor function that starts a benchmark set
        :param experiment_file_name: the path to the experiment definintion
        :param config_file: the location of the config file
        :param system: the system-under-test
        :param post_cleanup: whether to perform a cleanup after the run. only evaluated if a single run is performed
        :param single_run: whether to perform only a single run (the first in the lsit of experiments). Intended for debugging purposes
        :return: a list of paths containing references to the results
        """
        runs, iterations = FileIO.read_experiments_config(experiment_file_name, config_file,
                                                          system)  # todo use iterations
        print(f"running {len(runs)} experiments")
        print([str(r.benchmark_params) for r in runs])

        runs[0].host_params.controller_db_connection.initialize_benchmark_set(Path(experiment_file_name).parts[-1])

        result_files = []
        if system:
            if single_run:
                run = next(r for r in runs if r.benchmark_params.system.name == system)
                print(str(run))
                result_files.extend(self.__run_tasks(run))
                if post_cleanup:
                    self.clean(config_file)
            else:
                for r in runs:
                    result_files.extend(self.__run_tasks(r))
                    self.clean(config_file)

        else:
            for r in runs:
                result_files.extend(self.__run_tasks(r))
                self.clean(config_file)

        return result_files

    def evaluate(self, config_filename: str, result_files: list[Path], base_run_str="", evalfolder=""):
        """
        start the evaluator
        :param config_filename: the config for which results shall be evaluated
        :param result_files: the list of result files
        :param base_run_str: the parameter combination the evaluation shall be based on
        :param evalfolder: the location where results of the evaluation shall be stored
        :return:
        """
        host_params = FileIO.get_host_params(config_filename)
        print(result_files)
        evaluator = Evaluator(result_files, host_params, evalfolder)
        evaluator.get_accuracy(base_run_str)

    def clean(self, config_filename: str):
        """
        the cleanup routine
        :param config_filename: the config for which the cleanup shall be performed
        :return:
        """
        host_params = FileIO.get_host_params(config_filename)

        network_manager = NetworkManager(host_params, "cleanup", None, None)
        file_transporter = FileTransporter(network_manager)

        file_transporter.send_file(Path("hub/teardown.sh"), Path("config/teardown.sh"))
        network_manager.run_ssh(f"chmod 755 {host_params.host_base_path.joinpath('config/teardown.sh')}")
        network_manager.run_ssh(f"bash {host_params.host_base_path.joinpath('config/teardown.sh')}")

        # network_manager.run_ssh("""kill $(ps aux | grep "docker stats" | awk {\\'print $2\\'} )""")
        # network_manager.run_ssh("docker stop $(docker ps -q)")
        # network_manager.run_ssh("docker rm $(docker ps -aq)")
        # network_manager.run_ssh("docker volume rm $(docker volume ls -q)")


def main():
    """
    the main method containing the CLI
    :return:
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("command", help="Use either start, clean, or eval.")
    parser.add_argument("--system", help="Specify which system should be benchmarked")
    parser.add_argument("--config",
                        help="Specify the path to the controller config file",
                        required=True)
    parser.add_argument("--experiment",
                        help="Specify the path to the experiment definition file",
                        required=True,
                        action="append")
    parser.add_argument("--postcleanup",
                        help="Whether to run a cleanup after running the benchmark. Only works together with '--system <system>'",
                        action=argparse.BooleanOptionalAction,
                        default=True)
    parser.add_argument("--singlerun",
                        help="Whether to run only one the first experiment. Only works together with '--system <system>'",
                        action=argparse.BooleanOptionalAction,
                        default=True)
    parser.add_argument("--eval",
                        help="Whether to run the evaluation",
                        action=argparse.BooleanOptionalAction,
                        default=True)
    parser.add_argument("--resultsfile",
                        help="specify files for running the eval",
                        nargs="+")
    parser.add_argument("--evalbase",
                        help="specify which system should act as the baseline in eval mode. If none is specified, the lexicographically first string is chosen.")
    parser.add_argument("--evalfolder",
                        help="specify the name of the output eval folder.")

    args = parser.parse_args()
    print(args)
    setup = Setup()
    if args.command == "start":
        for experiment_file_name in args.experiment:
            result_files = setup.benchmark(experiment_file_name, args.config, args.system, args.postcleanup,
                                           args.singlerun)

            print(" ".join(list(map(lambda p: str(p), result_files))))
            # if len(result_files) > 1 and args.eval:
            #     setup.evaluate(args.config, result_files, args.evalbase)
    if args.command == "clean":
        setup.clean(args.config)
    if args.command == "eval":
        setup.evaluate(args.config, list(map(lambda f: Path(f), args.resultsfile)), args.evalbase, args.evalfolder)


if __name__ == "__main__":
    main()
