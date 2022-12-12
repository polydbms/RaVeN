from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters
from hub.benchmarkrun.measurementslocation import MeasurementsLocation
from hub.duckdb.submit_data import DuckDBConnector, DuckDBRunCursor
from hub.utils.datalocation import DataLocation


class BenchmarkRun:
    raster: DataLocation
    vector: DataLocation
    workload: dict
    benchmark_params: BenchmarkParameters
    host_params: HostParameters
    warm_starts: int
    measurements_loc: MeasurementsLocation

    def __init__(self, raster: DataLocation, vector: DataLocation, workload: dict,
                 host_params: HostParameters, benchmark_params: BenchmarkParameters, experiment_name_file: str,
                 warm_starts: int):
        self.raster = raster
        self.vector = vector
        self.workload = workload
        self.host_params = host_params
        self.benchmark_params = benchmark_params
        self.experiment_name_file = experiment_name_file
        self.warm_starts = warm_starts

        self.measurements_loc = MeasurementsLocation(self.host_params, self.benchmark_params)

    def __str__(self):
        return ", ".join([f"[{p}]" for p in [str(self.raster),
                                             str(self.vector),
                                             str(self.workload),
                                             str(self.host_params),
                                             str(self.benchmark_params),
                                             str(self.warm_starts),
                                             str(self.measurements_loc)]])
