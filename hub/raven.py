import base64
import copy
import importlib
import json
import subprocess
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
from hub.utils.capabilities import Capabilities
from hub.utils.network import BasicNetworkManager
from hub.optimizer.optimizer import Optimizer
from hub.zsresultsdb.init_duckdb import InitializeDuckDB


class Setup:
    """
    the main utility containing all benchi-related routines
    """

    def __init__(self, progress_listener=None) -> None:
        self.logger = {}
        self._progress = 0
        self._max_progress = 0
        self._progress_listener = progress_listener
        self.workers_nm: list[BasicNetworkManager] = []
        self.workers_ft: list[FileTransporter] = []

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

    def run_tasks(self, run: BenchmarkRun, iteration=1, stop_at_preprocess=False) -> list[Path]:
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

        if stop_at_preprocess:
            print("Stopping at preprocess")
            transporter.get_measurements(run.measurements_loc)

            return []

        self.do_ingestion(network_manager, run, system)

        executor, result_files = self.do_execution(network_manager, run, system)

        result_files_not_empty = self.do_teardown(executor, result_files, run, run_cursor, transporter)

        return list(map(lambda resfile: resfile[0], result_files_not_empty))

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
        capabilities = Capabilities.read_capabilities()
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
        main_yaml = copy.deepcopy(rendered_yaml)

        worker_name = f"{system.name}_worker"

        if system.name in capabilities["distributed"] and run.benchmark_params.parallel_machines > 1 and \
                system.name in capabilities["spark_based"]:
            del main_yaml["services"][worker_name]
        else:
            main_yaml = {"services": {system.name: main_yaml["services"][system.name]}}
            if "volumes" in rendered_yaml:
                main_yaml["volumes"] = copy.deepcopy(rendered_yaml["volumes"])

            if "depends_on" in main_yaml["services"][system.name]:
                del main_yaml["services"][system.name]["depends_on"]
            if "networks" in main_yaml["services"][system.name]:
                del main_yaml["services"][system.name]["networks"]

        PROJECT_ROOT.joinpath(f"deployment/files/{system.name}/docker-compose.yml").write_text(yaml.dump(main_yaml))

        if system.name in capabilities["distributed"]:
            env = rendered_yaml["services"][system.name].get("environment", {})

            if system.name in capabilities["spark_based"]:
                master_url = ""


                with run.host_params.ssh_config_path.expanduser().open("r") as ssh_config_file:
                    while line := ssh_config_file.readline():
                        if line.lower().startswith("Host ".lower()) and run.host_params.ssh_connection in line:
                            while line := ssh_config_file.readline():
                                if line.lower().strip().startswith("HostName ".lower()):
                                    master_url = line.strip().split(" ")[1].strip()
                                    break
                            break

                if not master_url:
                    raise ValueError(f"Could not find master URL for {run.host_params.ssh_connection} in ssh config")

                master_ip = subprocess.run(
                    f'nslookup {master_url} | grep -oP "(?<=Address: ).*"',
                    shell=True,
                    capture_output=True,
                ).stdout.decode('utf-8').strip()

                network_manager.master_url = master_ip

                if isinstance(env, list):
                    env = {e.split('=')[0]: e.split('=')[1] for e in env}

                env["SPARK_MODE"] = "worker"
                # env["SPARK_MASTER_URL"] = f"spark://{master_ip}:7077"
                env["SPARK_MASTER_URL"] = f"spark://beast:7077"

                if env["GRANT_SUDO"]:
                    env["GRANT_SUDO"] = "true" if env["GRANT_SUDO"] else "false"

                rendered_yaml["services"][worker_name]["environment"] = env

                worker_yaml = {"services": {worker_name: rendered_yaml["services"][worker_name].copy()}, "networks": rendered_yaml["networks"]}
                worker_yaml["services"][worker_name]["deploy"] = rendered_yaml["services"][worker_name].get("deploy", {"replicas": run.benchmark_params.parallel_machines})

                del worker_yaml["services"][worker_name]["depends_on"]

                PROJECT_ROOT.joinpath(f"deployment/files/{system.name}/docker-compose.worker.yml").write_text(yaml.dump(worker_yaml))


            else:
                raise NotImplementedError(f"System {system.name} is not supported for distributed execution")

            system_name = run.benchmark_params.system.name
            # self.workers_nm = []
            # self.workers_ft = []

            if system_name in capabilities["distributed"] and run.benchmark_params.parallel_machines > 1:
                if len(run.host_params.workers) < run.benchmark_params.parallel_machines:
                    raise ValueError(
                        f"Not enough workers available for {run.benchmark_params.parallel_machines} parallel machines. "
                        f"Only {len(run.host_params.workers)} workers available."
                    )

                # print(f"initializing {run.benchmark_params.parallel_machines} workers for {system_name}")
                # network_manager.run_ssh
                # for i in range(run.benchmark_params.parallel_machines):
                #     worker = run.host_params.workers[i]
                #     worker_nm = BasicNetworkManager(worker["host"], run.host_params, system_name)
                #     worker_ft = FileTransporter(worker_nm)
                #
                #     self.workers_nm.append(worker_nm)
                #     self.workers_ft.append(worker_ft)


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

        # self.initialize_workers(run)

        self.increase_progress()
        return network_manager, run_cursor, transporter


    # def initialize_workers(self, run: BenchmarkRun):
    #     """
    #     initializes the worker nodes
    #     :param run: the benchmark run containing the parameters
    #     """
    #
    #     for worker_nm, worker_ft in zip(self.workers_nm, self.workers_ft):
    #         worker_nm.run_remote_mkdir(worker_nm.host_params.host_base_path.joinpath("config"))
    #         worker_ft.send_configs(log_time=self.logger)


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

        vector_filter = []
        bbox = {}
        if run.benchmark_params.vector_filter_at_stage == Stage.PREPROCESS and run.workload.get("condition", {}).get("vector", {}):
            vector_filter = run.workload.get("condition", {}).get("vector", [])
            del run.workload["condition"]["vector"]

        if run.benchmark_params.vector_filter_at_stage == Stage.PREPROCESS and run.workload.get("extent", {}):
            bbox = run.workload.get("extent", {}).get("bbox", {})
            del run.workload["extent"]

        # extent = extent_to_geom(run.workload.get("extent", {}), run.benchmark_params, run.vector.get_crs()) \
        #     if run.benchmark_params.vector_filter_at_stage == Stage.PREPROCESS \
        #     else None
        # extent_str = extent.wkt \
        #     if extent \
        #     else ""

        vector_filter_str = base64.b64encode(json.dumps(vector_filter).encode('utf-8')).decode('utf-8')
        bbox_str = f"{bbox['xmin']} {bbox['ymin']} {bbox['xmax']} {bbox['ymax']}" if bbox else ""

        parameters = f'--system {system} ' \
                     f'--vector_path {run.vector.docker_dir} ' \
                     f'--vector_source_suffix {run.vector.suffix.value} ' \
                     f'--vector_target_suffix {run.benchmark_params.vector_target_format.value} ' \
                     f'--vector_output_folder {run.vector.docker_dir_preprocessed} ' \
                     f'--vector_target_crs {vector_target_crs} ' \
                     f'--vectorization_type {run.benchmark_params.vectorize_type.value} ' \
                     f'--vector_simplify {run.benchmark_params.vector_simplify} ' \
                     f'--raster_path {run.raster.docker_dir} ' \
                     f'--raster_source_suffix {run.raster.suffix.value} ' \
                     f'--raster_target_suffix {run.benchmark_params.raster_target_format.value} ' \
                     f'--raster_output_folder {run.raster.docker_dir_preprocessed} ' \
                     f'--raster_target_crs {raster_target_crs} ' \
                     f'--raster_resolution {run.benchmark_params.raster_resolution} ' \
                     f'--{"" if run.benchmark_params.raster_clip else "no-"}raster_clip ' \
                     f'{"--vector_filter " + vector_filter_str if run.benchmark_params.vector_filter_at_stage == Stage.PREPROCESS else ""} ' \
                     f'''{f'--bbox {bbox_str} --bbox_srs {bbox["srid"]}' if bbox else ""} ''' \
                     f'--{"" if run.benchmark_params.raster_singlefile else "no-"}raster_singlefile ' \
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

        self.launch_workers(system, network_manager)

        self.increase_progress()


    def launch_workers(self,
                       system_i: System, network_manager: NetworkManager) -> None:

        network_manager.run_ssh(
            f"docker stack deploy --compose-file {network_manager.host_params.host_base_path.joinpath('config', system_i.name, 'docker-compose.worker.yml')} {system_i.name}",
            log_time=self.logger)
        # for worker_nm in self.workers_nm:
        #         worker_nm.run_ssh(f"docker compose -f {worker_nm.host_params.host_base_path.joinpath(worker_nm.host_params.host_base_path, 'config', system.name, 'docker-compose.worker.yml')} up -d",
        #                           log_time=self.logger)


    def do_ingestion(self,
                     network_manager: NetworkManager,
                     run: BenchmarkRun,
                     system: System) -> None:
        """
            Ingestion stage
            """
        # sleep(5)

        network_manager.start_measure_docker("ingestion")
        Ingestor = self.__importer(f"hub.ingestion.{system}", "Ingestor")
        ingestor = Ingestor(run.vector, run.raster, network_manager, run.benchmark_params, run.workload)
        ingestor.ingest_raster(log_time=self.logger)
        network_manager.host_params.controller_params.controller_db_connection.register_file(run.raster, run.benchmark_params, run.workload)
        self.increase_progress()

        ingestor.ingest_vector(log_time=self.logger)
        network_manager.host_params.controller_params.controller_db_connection.register_file(run.vector, run.benchmark_params, run.workload)
        self.increase_progress()

        network_manager.stop_measure_docker()

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
            sleep(8)
            print(f"running warm start {i} out of {run.warm_starts} for parameters {run.benchmark_params}")
            network_manager.add_meta_marker_start(i)
            result_files.append(executor.run_query(run.workload, warm_start_no=i, log_time=self.logger))
            network_manager.add_meta_marker_end()

            self.increase_progress()


        network_manager.stop_measure_docker()

        return executor, result_files

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
        run.host_params.controller_params.controller_db_connection.delete_file_by_uuid(run.raster.uuid)
        run.host_params.controller_params.controller_db_connection.delete_file_by_uuid(run.vector.uuid)

        self.stop_workers(run.benchmark_params.system.name, transporter.network_manager)
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

    def stop_workers(self, system_i: str, network_manager: BasicNetworkManager) -> None:
        network_manager.run_ssh(f"docker stack rm {system_i}", log_time=self.logger)
        # for worker_nm in self.workers_nm:
        #     worker_nm.run_ssh(f"docker compose -f {worker_nm.host_params.host_base_path.joinpath(worker_nm.host_params.host_base_path, 'config', system, 'docker-compose.worker.yml')} down",
        #                       log_time=self.logger)

    def optimize(self, experiment_file_name: str, config_file: str, post_cleanup=True) -> None:
        workload, raster_location, vector_location, host_cofig, controller_config = FileIO.read_experiment_essentials(
            experiment_file_name, config_file)
        capabilities = Capabilities.read_capabilities()

        params = Optimizer.create_run_config(workload, raster_location, vector_location)

        FileIO.adjust_by_capabilities(params, capabilities, vector_location, raster_location)
        vector_location.adjust_target_files(params)
        raster_location.adjust_target_files(params)

        params.validate(capabilities)

        vector_location.impose_limitations(params)
        raster_location.impose_limitations(params)

        exp_file = Path("OPTIMIZED." + experiment_file_name).parts[-1]

        run = BenchmarkRun(raster=raster_location,
                           vector=vector_location,
                           workload=workload,
                           benchmark_params=params,
                           controller_params=controller_config,
                           experiment_name_file=exp_file,
                           warm_starts=0,
                           query_timeout=3600,
                           resource_limits={}
                           )

        run.set_host(host_cofig)
        InitializeDuckDB(controller_config.controller_db_connection, [run], exp_file)

        run.host_params.controller_db_connection.initialize_benchmark_set(exp_file, {})

        print(str(run))

        result = self.run_tasks(run)

        print(result)




    def benchmark(self, experiment_file_name: str, config_file: str, system=None, post_cleanup=True,
                  single_run=True, stop_at_preprocess=False) -> tuple[list[Path], Path, list[str]]:
        """
        anchor function that starts a benchmark set
        :param experiment_file_name: the path to the experiment definintion
        :param config_file: the location of the config file
        :param system: the system-under-test
        :param post_cleanup: whether to perform a cleanup after the run. only evaluated if a single run is performed
        :param single_run: whether to perform only a single run (the first in the lsit of experiments). Intended for debugging purposes
        :param stop_at_preprocess: whether to stop the benchmark after preprocessing
        :return: a list of paths containing references to the results
        """
        runs, iterations = FileIO.read_experiments_config(experiment_file_name, config_file,
                                                          system)  # todo use iterations

        if not runs or iterations < 1:
            print("No runs found, aborting")
            return [], None, []

        print(f"running {len(runs)} experiments")
        print([str(r.benchmark_params) for r in runs])

        runs[0].host_params.controller_db_connection.initialize_benchmark_set(Path(experiment_file_name).parts[-1], runs[0].resource_limits)

        result_files = []
        if system:
            if single_run:
                run = next(r for r in runs if r.benchmark_params.system.name == system)
                print(str(run))
                result_files.extend(self.run_tasks(run, stop_at_preprocess=stop_at_preprocess))
                if post_cleanup:
                    self.clean(config_file)
            else:
                for run in list(filter(lambda r: r.benchmark_params.system.name == system, runs)):
                    result_files.extend(self.run_tasks(run, stop_at_preprocess=stop_at_preprocess))
                    self.clean(config_file)

        else:
            for run in runs:
                result_files.extend(self.run_tasks(run, stop_at_preprocess=stop_at_preprocess))
                self.clean(config_file)

        return result_files, runs[0].vector.controller_file[0], runs[0].workload.get("get", {}).get("vector", [])

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
        host_params, _ = FileIO.get_host_params(config_filename)

        network_manager = NetworkManager(host_params, "cleanup", None, None)
        file_transporter = FileTransporter(network_manager)

        file_transporter.send_file(PROJECT_ROOT.joinpath("teardown.sh"), Path("config/teardown.sh"))
        network_manager.run_ssh(f"chmod 755 {host_params.host_base_path.joinpath('config/teardown.sh')}")
        network_manager.run_ssh(f"bash {host_params.host_base_path.joinpath('config/teardown.sh')} {host_params.host_base_path}")

        # network_manager.run_ssh("""kill $(ps aux | grep "docker stats" | awk {\\'print $2\\'} )""")
        # network_manager.run_ssh("docker stop $(docker ps -q)")
        # network_manager.run_ssh("docker rm $(docker ps -aq)")
        # network_manager.run_ssh("docker volume rm $(docker volume ls -q)")
