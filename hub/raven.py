import base64
import importlib
import json
import logging
from datetime import datetime
from pathlib import Path
from time import sleep

import jinja2
import yaml

from hub.configuration import PROJECT_ROOT
from hub.benchmarkrun.benchmark_run import BenchmarkRun
from hub.enums.stage import Stage
from hub.evaluation.main import Evaluator
from hub.utils.fileio import FileIO
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager
from hub.utils.system import System
from hub.utils.interfaces import ExecutorInterface
from hub.zsresultsdb.submit_data import DuckDBRunCursor
from hub.utils.query import extent_to_geom


class Setup:
    """
    the main utility containing all benchi-related routines
    """

    def __init__(self, progress_listener=None) -> None:
        self.logger = {}
        self._progress = 0
        self._max_progress = 0
        self._progress_listener = progress_listener

    @property
    def progress(self) -> int:
        return self._progress

    def increase_progress(self):
        self._progress += 1
        if self._progress_listener:
            self._progress_listener(self._progress, self._max_progress)

    @property
    def max_progress(self) -> int:
        return self._max_progress

    def init_progress(self, runs, iterations, warm_starts) -> None:
        print(f"runs: {runs}, warm_starts: {warm_starts}, iterations: {iterations}")
        self._max_progress = runs * iterations * (7 + warm_starts)
        self._progress = 0

    @staticmethod
    def __importer(module, class_name):
        """Dynamic Importing"""
        module = importlib.import_module(module)
        return getattr(module, class_name)

    def run_tasks(self, run: BenchmarkRun, iteration=1) -> list[Path]:
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

        network_manager, run_cursor, transporter = self.setup_host(iteration, run, system)

        self.do_preprocess(network_manager, run, system)

        self.do_ingestion(network_manager, run, system)

        executor, result_files = self.do_execution(network_manager, run, system)

        result_files_not_empty = self.do_teardown(executor, result_files, run, run_cursor, transporter)

        return list(map(lambda resfile: resfile[0], result_files_not_empty))

    def do_teardown(self, executor: ExecutorInterface,
                    result_files: list[Path],
                    run: BenchmarkRun,
                    run_cursor: DuckDBRunCursor,
                    transporter: FileTransporter) -> list[tuple[Path, bool]]:
        """
            Cleanup
            """
        transporter.get_measurements(run.measurements_loc)
        executor.post_run_cleanup()
        self.increase_progress()
        """
            transfer results to database
            """
        run_cursor.add_resource_utilization(
            [run.measurements_loc.controller_measurements_folder.joinpath(f"{e.value}.csv") for e in list(Stage)]
        )
        result_files_emptiness_info = list(map(run_cursor.add_results_file, result_files))
        result_files_not_empty = list(filter(lambda r: r[1], result_files_emptiness_info))
        return result_files_not_empty

    def do_execution(self,
                     network_manager: NetworkManager,
                     run: BenchmarkRun,
                     system: System) -> tuple[ExecutorInterface, list[Path]]:
        """
            Execution stage
            """
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
        self.increase_progress()
        # i warm starts
        for i in range(1, run.warm_starts + 1):
            sleep(10)
            print(f"running warm start {i} out of {run.warm_starts} for parameters {run.benchmark_params}")
            network_manager.add_meta_marker_start(i)
            result_files.append(executor.run_query(run.workload, warm_start_no=i, log_time=self.logger))
            network_manager.add_meta_marker_end()

            self.increase_progress()


        network_manager.stop_measure_docker()

        return executor, result_files

    def do_ingestion(self,
                     network_manager: NetworkManager,
                     run: BenchmarkRun,
                     system: System) -> None:
        """
            Ingestion stage
            """
        network_manager.start_measure_docker("ingestion")
        Ingestor = self.__importer(f"hub.ingestion.{system}", "Ingestor")
        ingestor = Ingestor(run.vector, run.raster, network_manager, run.benchmark_params, run.workload)
        ingestor.ingest_raster(log_time=self.logger)
        self.increase_progress()
        ingestor.ingest_vector(log_time=self.logger)
        self.increase_progress()
        network_manager.stop_measure_docker()

    def do_preprocess(self,
                      network_manager: NetworkManager,
                      run: BenchmarkRun,
                      system: System) -> None:
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
        vector_filter = run.workload.get("condition", {}).get("vector",
                                                              []) if run.benchmark_params.vector_filter_at_stage == Stage.PREPROCESS else []
        vector_filter_str = base64.b64encode(json.dumps(vector_filter).encode('utf-8')).decode('utf-8')
        # extent = extent_to_geom(run.workload.get("extent", {}), run.benchmark_params, run.vector.get_crs()) \
        #     if run.benchmark_params.vector_filter_at_stage == Stage.PREPROCESS \
        #     else None
        # extent_str = extent.wkt \
        #     if extent \
        #     else ""
        bbox = run.workload.get("extent", {}).get("bbox", {}) if run.benchmark_params.vector_filter_at_stage == Stage.PREPROCESS else {}
        bbox_str = f"{bbox['xmin']} {bbox['ymin']} {bbox['xmax']} {bbox['ymax']}" if bbox else ""


        parameters = f'--system {system} ' \
                     f'--vector_path {run.vector.docker_dir} ' \
                     f'--vector_target_suffix {run.benchmark_params.vector_target_format.value} ' \
                     f'--vector_output_folder {run.vector.docker_dir_preprocessed} ' \
                     f'--vector_target_crs {vector_target_crs} ' \
                     f'--vectorization_type {run.benchmark_params.vectorize_type.value} ' \
                     f'--vector_simplify {run.benchmark_params.vector_simplify} ' \
                     f'--raster_path {run.raster.docker_dir} ' \
                     f'--raster_target_suffix {run.benchmark_params.raster_target_format.value} ' \
                     f'--raster_output_folder {run.raster.docker_dir_preprocessed} ' \
                     f'--raster_target_crs {raster_target_crs} ' \
                     f'--raster_resolution {run.benchmark_params.raster_resolution} ' \
                     f'--{"" if run.benchmark_params.raster_clip else "no-"}raster_clip ' \
                     f'{"--vector_filter " + vector_filter_str if run.benchmark_params.vector_filter_at_stage == Stage.PREPROCESS else ""} ' \
                     f'''{f'--bbox {bbox_str} --bbox_srs {bbox["srid"]}' if bbox else ""}''' \
                     f''
                     # f'''{'--extent "' + extent_str + '"' if extent else ""} ''' \

        print(f"running {command} {parameters}")
        "create a string that encodess parameters to a base64 string"
        network_manager.run_ssh(f"{command} {base64.b64encode(parameters.encode('utf-8')).decode('utf-8')}",
                                log_time=self.logger,
                                )
        run.raster.set_preprocessed()
        run.vector.set_preprocessed()
        network_manager.stop_measure_docker()
        # print("Wait 5s until docker is ready")
        # sleep(5)
        self.increase_progress()

    def setup_host(self,
                   iteration: int,
                   run: BenchmarkRun,
                   system: System) -> tuple[NetworkManager, DuckDBRunCursor, FileTransporter]:
        """
            Setup
            """
        run_cursor = run.host_params.controller_db_connection.initialize_benchmark_run(run.benchmark_params, iteration)
        network_manager = NetworkManager(run.host_params, run.benchmark_params.system.name, run.measurements_loc,
                                         run_cursor, run.query_timeout)
        transporter = FileTransporter(network_manager)
        self.increase_progress()
        """
            resolve compose Template and set resource limitations
            """
        template_loader = jinja2.FileSystemLoader(searchpath=PROJECT_ROOT.joinpath(f"deployment/files/{system.name}/"))
        template_env = jinja2.Environment(loader=template_loader)
        template_file = "docker-compose.yml.j2"
        template = template_env.get_template(template_file)
        rendered = template.render()
        rendered_yaml = yaml.safe_load(rendered)
        rendered_yaml["services"][system.name] = rendered_yaml["services"][system.name] | run.resource_limits
        PROJECT_ROOT.joinpath(f"deployment/files/{system.name}/docker-compose.yml").write_text(yaml.dump(rendered_yaml))
        """
            transfer configs and datasets to the host
            """
        transporter.send_configs(log_time=self.logger)
        # print(run.vector)
        transporter.send_data(run.vector, log_time=self.logger)
        print(run.raster)
        transporter.send_data(run.raster, log_time=self.logger)
        # Give execute permission
        network_manager.run_ssh(f"chmod +x {run.host_params.host_base_path.joinpath('config/**/*.sh')}",
                                log_time=self.logger)
        self.increase_progress()
        return network_manager, run_cursor, transporter

    def benchmark(self, experiment_file_name: str, config_file: str, system=None, post_cleanup=True,
                  single_run=True) -> tuple[list[Path], Path, list[str]]:
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

        runs[0].host_params.controller_db_connection.initialize_benchmark_set(Path(experiment_file_name).parts[-1], runs[0].resource_limits)

        result_files = []
        if system:
            if single_run:
                run = next(r for r in runs if r.benchmark_params.system.name == system)
                print(str(run))
                result_files.extend(self.run_tasks(run))
                if post_cleanup:
                    self.clean(config_file)
            else:
                for run in list(filter(lambda r: r.benchmark_params.system.name == system, runs)):
                    result_files.extend(self.run_tasks(run))
                    self.clean(config_file)

        else:
            for run in runs:
                result_files.extend(self.run_tasks(run))
                self.clean(config_file)

        return result_files, runs[0].vector.controller_file, runs[0].workload.get("get", {}).get("vector", [])

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

        file_transporter.send_file(PROJECT_ROOT.joinpath("teardown.sh"), Path("config/teardown.sh"))
        network_manager.run_ssh(f"chmod 755 {host_params.host_base_path.joinpath('config/teardown.sh')}")
        network_manager.run_ssh(f"bash {host_params.host_base_path.joinpath('config/teardown.sh')}")

        # network_manager.run_ssh("""kill $(ps aux | grep "docker stats" | awk {\\'print $2\\'} )""")
        # network_manager.run_ssh("docker stop $(docker ps -q)")
        # network_manager.run_ssh("docker rm $(docker ps -aq)")
        # network_manager.run_ssh("docker volume rm $(docker volume ls -q)")
