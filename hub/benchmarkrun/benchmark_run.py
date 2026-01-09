import copy

from hub.benchmarkrun.controller_params import ControllerParameters
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters
from hub.benchmarkrun.measurementslocation import MeasurementsLocation
from hub.utils.datalocation import DataLocation


class BenchmarkRun:
    """
    Contains all information necessary for a single benchmark run
    """
    raster: DataLocation
    vector: DataLocation
    workload: dict
    benchmark_params: BenchmarkParameters
    host_params: HostParameters
    warm_starts: int
    measurements_loc: MeasurementsLocation

    def __init__(self,
                 raster: DataLocation,
                 vector: DataLocation,
                 workload: dict,
                 benchmark_params: BenchmarkParameters,
                 controller_params: ControllerParameters,
                 experiment_name_file: str,
                 warm_starts: int,
                 query_timeout: int,
                 resource_limits: dict,
                 ):
        """
        the Init function
        :param raster: all information on the raster data
        :param vector: all information on the vector data
        :param workload: the query described in the Benchi DSL
        :param host_params: the static parameters of the host
        :param benchmark_params: the benchmark-run specific paramteres
        :param experiment_name_file: the name of the benchmark
        :param warm_starts: the amount of warm starts to be done
        :param query_timeout: the query-timeout after which an execution shall be aborted
        """
        self.raster = copy.deepcopy(raster)
        self.vector = copy.deepcopy(vector)
        self.workload = copy.deepcopy(workload)
        self.controller_params = controller_params
        self.benchmark_params = benchmark_params
        self.experiment_name_file = experiment_name_file
        self.warm_starts = warm_starts
        self.query_timeout = query_timeout
        self.resource_limits = resource_limits.copy()


    def set_host(self, host_params: HostParameters):
        """
        define the host when the run will be executed
        """
        self.host_params = host_params
        self.measurements_loc = MeasurementsLocation(self.host_params, self.benchmark_params)

    def __str__(self):
        return ", ".join([f"[{p}]" for p in [str(self.raster),
                                             str(self.vector),
                                             str(self.workload),
                                             str(self.host_params),
                                             str(self.controller_params),
                                             str(self.benchmark_params),
                                             str(self.warm_starts),
                                             str(self.query_timeout),
                                             str(self.resource_limits),
                                             str(self.measurements_loc)]])
