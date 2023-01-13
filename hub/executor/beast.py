from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager


class Executor:
    def __init__(self, vector_path: DataLocation,
                 raster_path: DataLocation,
                 network_manager: NetworkManager,
                 benchmark_params: BenchmarkParameters) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.transporter = FileTransporter(network_manager)
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.vector = vector_path
        self.raster = raster_path
        self.benchmark_params = benchmark_params

    @measure_time
    def run_query(self, workload, warm_start_no: int, **kwargs):
        self.network_manager.run_ssh(str(self.host_base_path.joinpath("config/beast/execute.sh")), **kwargs)

        result_path = self.network_manager.host_params.controller_result_folder.joinpath(
            f"results_{self.network_manager.measurements_loc.file_prepend}.{'cold' if warm_start_no == 0 else f'warm-{warm_start_no}'}.csv")
        result_file = self.host_base_path.joinpath("data/results/results_beast.csv")
        self.transporter.get_file(
            result_file,
            result_path,
            **kwargs,
        )

        self.network_manager.run_remote_rm_file(result_file)

        return result_path

    def post_run_cleanup(self):
        pass
