from pathlib import Path

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters


class MeasurementsLocation:
    def __init__(self, host_params: HostParameters, benchmark_params: BenchmarkParameters):
        self._file_prepend = f"{benchmark_params}"

        self._controller_measurements_folder = host_params.controller_result_folder \
            .joinpath(self._file_prepend)
        self._controller_measurements_folder.mkdir(parents=True, exist_ok=True)
        self._timings_file = self._controller_measurements_folder.joinpath("timings.csv")

        self._host_measurements_folder = host_params.host_base_path \
            .joinpath("measurements") \
            .joinpath(self._file_prepend)

        with self._timings_file.open("a") as f:
            f.write("marker,timestamp,event,stage,system,dataset,comment,controller_time")
            f.write("\n")

    @property
    def controller_measurements_folder(self) -> Path:
        return self._controller_measurements_folder

    @property
    def host_measurements_folder(self) -> Path:
        return self._host_measurements_folder

    @property
    def timings_file(self) -> Path:
        return self._timings_file

    @property
    def file_prepend(self):
        return self._file_prepend

    def __str__(self):
        return ", ".join([
            str(self._file_prepend),
            str(self._timings_file),
            str(self._controller_measurements_folder),
            str(self._host_measurements_folder)
        ])

