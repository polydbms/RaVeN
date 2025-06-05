import json
import subprocess
import zipfile
from abc import abstractmethod
from pathlib import Path

import pyproj
from pyproj import CRS

from hub.enums.datatype import DataType
from hub.enums.filetype import FileType
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters
from hub.enums.rasterfiletype import RasterFileType
from hub.enums.vectorfiletype import VectorFileType


class DataLocation:
    """
    class that wraps either a vector or a raster dataset. returns necessary file names depending on the requesting entity
    """
    _host_base_dir: Path
    _files: list[Path]
    _data_type: DataType
    _dir_name: str
    _preprocessed: bool
    _controller_location: Path
    _target_suffix: VectorFileType | RasterFileType
    _suffix: VectorFileType | RasterFileType
    _allowed_endings: list[str]

    def __init__(self,
                 path_str: str,
                 data_type: DataType,
                 host_params: HostParameters,
                 name: str = None) -> None:
        """
        initiializes the data lcoation. if the proveided path is a folder or zip archive, it tries to find the first
        available file. also prepares paths for the host and docker container based on the file
        :param path_str: the path to the dataset
        :param data_type: the class of data stored
        :param host_params: the hsot parameters
        :param name: the name of the dataset
        """
        self._host_params = host_params
        self._host_base_dir = host_params.host_base_path
        self._data_type = data_type
        self._benchmark_params = None

        path = Path(path_str).expanduser()

        if not path.exists():
            raise FileNotFoundError(f"Path {path_str} to input for {data_type} data does not exist")

        if path.is_dir():
            self.type = FileType.FOLDER
            self._controller_location = path

            self._files = self._find_files()
        elif zipfile.is_zipfile(path):
            self.type = FileType.ZIP_ARCHIVE
            raise NotImplementedError("ZIP Files are currently not supported")
        else:
            self.type = FileType.FILE
            self._controller_location = path.parent
            self._files = [Path(path.name)]

        self._fix_files()

        self._dir_name = self._controller_location.stem if name is None else name

        self._host_dir = self._host_base_dir.joinpath("data").joinpath(self._dir_name)
        self._docker_dir = Path("/data").joinpath(Path(self._dir_name))

        self._preprocessed = False

        # if self._data_type == DataType.VECTOR:
        #     self.type = json.loads(
        #         subprocess.check_output(f"ogrinfo -nocount -json -nomd {self.controller_file}", shell=True).decode(
        #             "utf-8"))["layers"][0]["fields"]

    @abstractmethod
    def adjust_target_files(self, benchmark_params: BenchmarkParameters):
        self._benchmark_params = benchmark_params

    def _find_files(self) -> list[Path]:
        """
        find a dataset file within a folder. Normalizes tiff files to a common file ending
        :return:
        """

        if self.type == FileType.FOLDER:
            all_relevant_files = [self._controller_location.glob(e) for e in self._allowed_endings]
            arf_flat = [item for sublist in all_relevant_files for item in sublist]

            if len(arf_flat) == 0:
                raise FileNotFoundError(f"Could not find any relevant file in {self._controller_location}")

            if len(set(f.suffix for f in arf_flat)) > 1:
                raise ValueError(
                    f"Multiple file types found in {self._controller_location}: {set(f.suffix for f in arf_flat)}")

            return [Path(f.name) for f in arf_flat]

        elif self.type == FileType.ZIP_ARCHIVE:
            raise NotImplementedError("ZIP Files are currently not supported")

            # return Path(
            #     [f for f in zipfile.Path(self._controller_location).iterdir() if Path(f.name).match(ending)][0].name)
        else:
            raise FileNotFoundError(f"Could not find file with ending {self._allowed_endings} at/in {self}")

    @property
    def docker_dir(self) -> Path:
        """
        the dataset directory as seen from the docker container
        :return:
        """
        return self._docker_dir if not self._preprocessed else self.docker_dir_preprocessed

    @property
    def docker_file(self) -> list[Path]:
        """
        the dataset file as seen from the docker container
        :return:
        """
        return [self.docker_dir.joinpath(f) for f in self._files]

    @property
    def docker_file_preprocessed(self) -> list[Path]:
        """
        the dataset file as seen from the docker container after the preprocess stage has been completed
        :return:
        """
        return [self.docker_dir_preprocessed.joinpath(f).with_suffix(str(self._target_suffix.value)) for f in self._files]

    @property
    def docker_dir_preprocessed(self) -> Path:
        """
        the dataset directory as seen from the docker container after the preprocess stage has been completed
        :return:
        """
        return self._docker_dir.joinpath(f"preprocessed_{self._benchmark_params}")

    @property
    def host_dir(self) -> Path:
        """
        the dataset directory as seen from the host
        :return:
        """
        return self._host_dir if not self._preprocessed else self.host_dir_preprocessed

    @property
    def host_file(self) -> list[Path]:
        """
        the dataset file as seen from the host
        :return:
        """
        return [self.host_dir.joinpath(f) for f in self._files]

    @property
    def host_file_preprocessed(self) -> list[Path]:
        """
        the dataset directory as seen from the host after the preprocess stage has been completed
        :return:
        """
        return [self.host_dir_preprocessed.joinpath(f).with_suffix(str(self._target_suffix.value)) for f in self._files]

    @property
    def host_dir_preprocessed(self) -> Path:
        """
        the dataset directory as seen from the host after the preprocess stage has been completed
        :return:
        """
        return self._host_dir.joinpath(f"preprocessed_{self._benchmark_params}")

    @property
    def name(self) -> str:
        """
        the name of the dataset
        :return:
        """
        return self._dir_name

    @property
    def controller_location(self) -> Path:
        """
        the dataset directory as available on the controller
        :return:
        """
        return self._controller_location

    @property
    def controller_file(self) -> list[Path]:
        """
        the dataset file as available on the controller
        :return:
        """
        return [self._controller_location.joinpath(f) for f in self._files]

    def set_preprocessed(self):
        """
        switch that returned paths shall be output from the preprocess stage
        :return:
        """
        self._preprocessed = True

    @property
    def preprocessed(self) -> bool:
        """
        whether preprocessed is set
        :return:
        """
        return self._preprocessed

    @property
    def suffix(self) -> VectorFileType | RasterFileType:
        """
        the suffix of the datset file
        :return:
        """
        return self._suffix

    @property
    def target_suffix(self) -> VectorFileType | RasterFileType:
        """
        the target suffix after the preprocess stage
        :return:
        """
        return self._target_suffix

    @target_suffix.setter
    def target_suffix(self, suffix: VectorFileType | RasterFileType):
        """
        set the target suffix of the file
        :param suffix:
        :return:
        """
        self._target_suffix = suffix

    @abstractmethod
    def get_crs(self) -> CRS:
        """
        return the CRS of the dataset
        :return:
        """
        pass

    @abstractmethod
    def _fix_files(self):
        """
        fix the file endings of the dataset
        :return:
        """
        pass

    def get_vector_types(self):
        return

    @abstractmethod
    def impose_limitations(self, benchmark_params: BenchmarkParameters):
        """
        impose limitations on the benchmark parameters based on the capabilities of the system
        :param benchmark_params: The benchmark parameters
        """
        pass

    def is_multifile(self) -> bool:
        """
        check if the dataset is a multi-file dataset
        :return: True if the dataset is a multi-file dataset, False otherwise
        """
        return len(self._files) > 1

    def __str__(self):
        return ",".join(
            [str(self._controller_location),
             str(self._data_type),
             str(self._files),
             self._dir_name,
             str(self._preprocessed)]
        )


class VectorLocation(DataLocation):
    """
    class that wraps a vector dataset. returns necessary file names depending on the requesting entity
    """

    _allowed_endings = ["*.shp"]

    def __init__(self,
                 path_str: str,
                 host_params: HostParameters,
                 name: str = None) -> None:
        """
        initiializes the data lcoation. if the proveided path is a folder or zip archive, it tries to find the first
        available file. also prepares paths for the host and docker container based on the file
        :param path_str: the path to the dataset
        :param host_params: the hsot parameters
        :param name: the name of the dataset
        """
        super().__init__(path_str, DataType.VECTOR, host_params, name)

        self._suffix = VectorFileType.get_by_value(self._files[0].suffix)

    def get_crs(self) -> CRS:
        """
        return the CRS of the dataset
        :return:
        """
        crs = json.loads(
            subprocess.check_output(f'ogrinfo -json -nocount -nomd {self.controller_file[0]}', shell=True)
            .decode('utf-8'))["layers"][0]["geometryFields"][0]["coordinateSystem"]["projjson"]["id"]["code"]
        return pyproj.CRS.from_epsg(crs)

    def _fix_files(self):
        """
        fix the file endings of the dataset
        :return:
        """
        pass

    def adjust_target_files(self, benchmark_params: BenchmarkParameters):
        """
        adjust the target files based on the benchmark parameters
        :param benchmark_params: the benchmark parameters
        :return:
        """
        super().adjust_target_files(benchmark_params)

        if self._benchmark_params.vector_target_format is None:
            self._target_suffix = self._suffix
        else:
            self._target_suffix = self._benchmark_params.vector_target_format

    @property
    def docker_wkt(self) -> Path:
        """
        the dataset file as a csv containing well-known text as seen form the docker container
        :return:
        """
        return self.docker_file_preprocessed[0].with_suffix(".csv")

    @property
    def host_wkt(self) -> Path:
        """
        the dataset file as a csv containing well-known text as seen form the host
        :return:
        """
        return self.host_file_preprocessed[0].with_suffix(".csv")

    @property
    def controller_wkt(self) -> Path:
        """
        the dataset file as a csv containing well-known text as seen form the host
        :return:
        """
        return self.controller_location.with_suffix(".csv")

    def impose_limitations(self, benchmark_params: BenchmarkParameters):
        if len(self._files) > 1:
            raise ValueError(f"Multiple files found in {self._controller_location}: {self._files}")



class RasterLocation(DataLocation):
    """
    class that wraps a raster dataset. returns necessary file names depending on the requesting entity
    """

    _allowed_endings = ["*.tif", "*.tiff", "*.geotiff", "*.jp2"]

    def __init__(self,
                 path_str: str,
                 host_params: HostParameters,
                 name: str = None) -> None:
        """
        initiializes the data lcoation. if the proveided path is a folder or zip archive, it tries to find the first
        available file. also prepares paths for the host and docker container based on the file
        :param path_str: the path to the dataset
        :param host_params: the hsot parameters
        :param name: the name of the dataset
        """
        super().__init__(path_str, DataType.RASTER, host_params, name)

        self._suffix = RasterFileType.get_by_value(self._files[0].suffix)

    def get_crs(self) -> CRS:
        """
        return the CRS of the dataset
        """
        crs = json.loads(
            subprocess.check_output(f'gdalinfo -json {self.controller_file[0]}', shell=True)
            .decode('utf-8'))["stac"]["proj:epsg"]

        return pyproj.CRS.from_epsg(crs)

    def _fix_files(self):
        """
        fix the file endings of the dataset
        :return:
        """
        for file in self._files:
            if file.suffix in {".tif", ".tiff", ".geotiff"}.difference({str(RasterFileType.TIFF.value)}):
                new_f = file.with_suffix(str(RasterFileType.TIFF.value))
                file.rename(new_f)

    def adjust_target_files(self, benchmark_params: BenchmarkParameters):
        """
        adjust the target files based on the benchmark parameters
        :param benchmark_params: the benchmark parameters
        :return:
        """
        super().adjust_target_files(benchmark_params)

        if self._benchmark_params.raster_target_format is None:
            self._target_suffix = self._suffix
        else:
            self._target_suffix = self._benchmark_params.raster_target_format

    def impose_limitations(self, benchmark_params: BenchmarkParameters):
        if len(self._files) > 1 and benchmark_params.raster_clip:
            raise ValueError(f"Multiple raster files not supported for clipping: {self._files}")
