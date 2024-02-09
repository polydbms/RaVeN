from pathlib import Path

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters
from hub.duckdb.submit_data import DuckDBRunCursor


class MeasurementsLocation:
    """
    the location of all measurements
    """

    def __init__(self, host_params: HostParameters, benchmark_params: BenchmarkParameters):
        """
        initializes the measurements location. takes host and benchmark params and creates a measurement folder on the
        controller as a location to store them. also initializes a timings file used as a backup.
        :param host_params: the hsot parameters
        :param benchmark_params: the benchmark run specific parameters
        """
        self._file_prepend = f"{benchmark_params}"

        self._controller_measurements_folder = host_params.controller_result_folder \
            .joinpath(self._file_prepend)
        self._timings_file = self._controller_measurements_folder.joinpath("timings.csv")
        self._run_folder = host_params.run_folder

        self._host_measurements_folder = host_params.host_base_path \
            .joinpath("measurements") \
            .joinpath(self._run_folder) \
            .joinpath(self._file_prepend)

        self._is_accessed = False

    def create_dir_structure(self):
        self._controller_measurements_folder.mkdir(parents=True, exist_ok=True)
        with self._timings_file.open("a") as f:
            f.write("marker,timestamp,event,stage,system,dataset,comment,controller_time")
            f.write("\n")
        self._is_accessed = True

    @property
    def controller_measurements_folder(self) -> Path:
        if not self._is_accessed:
            self.create_dir_structure()
        return self._controller_measurements_folder

    @property
    def host_measurements_folder(self) -> Path:
        if not self._is_accessed:
            self.create_dir_structure()
        return self._host_measurements_folder

    @property
    def timings_file(self) -> Path:
        if not self._is_accessed:
            self.create_dir_structure()
        return self._timings_file

    @property
    def file_prepend(self):
        if not self._is_accessed:
            self.create_dir_structure()
        return self._file_prepend

    def __str__(self):
        return ", ".join([
            str(self._file_prepend),
            str(self._timings_file),
            str(self._controller_measurements_folder),
            str(self._host_measurements_folder)
        ])
