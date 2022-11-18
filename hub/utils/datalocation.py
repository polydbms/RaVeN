import zipfile
from enum import Enum
from pathlib import Path

import geopandas.io.file
from pyproj import CRS
from rioxarray import rioxarray

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters
from hub.enums.rasterfiletype import RasterFileType
from hub.enums.vectorfiletype import VectorFileType


class DataType(Enum):
    RASTER = 1
    VECTOR = 2


class FileType(Enum):
    FILE = 1
    FOLDER = 2
    ZIP_ARCHIVE = 3


class DataLocation:
    # _system: System
    _host_base_dir: Path
    _file: Path
    _data_type: DataType
    _name: str
    _preprocessed: bool
    _controller_location: Path
    _target_suffix: VectorFileType | RasterFileType

    def __init__(self,
                 path: str,
                 data_type: DataType,
                 host_params: HostParameters,
                 benchmark_params: BenchmarkParameters,
                 name: str = None) -> None:
        self._controller_location = Path(path).expanduser()
        self._benchmark_params = benchmark_params
        self._host_params = host_params
        self._host_base_dir = host_params.host_base_path

        if not self._controller_location.exists():
            raise FileNotFoundError(f"Path {path} to input for {data_type} data does not exist")

        if self._controller_location.is_dir():
            self.type = FileType.FOLDER
        elif zipfile.is_zipfile(self._controller_location):
            self.type = FileType.ZIP_ARCHIVE
        else:
            self.type = FileType.FILE

        self._name = self._controller_location.stem if name is None else name

        self._data_type = data_type
        self._file = self._find_file(["*.shp"] if self._data_type == DataType.VECTOR else ["*.tif", "*.tiff", "*.jp2"])

        self._host_dir = self._host_base_dir.joinpath("data").joinpath(self._name)
        self._docker_dir = Path("/data").joinpath(Path(self._name))

        self._preprocessed = False

        match self._data_type:
            case DataType.VECTOR:
                self._suffix = VectorFileType.get_by_value(self._file.suffix)
            case DataType.RASTER:
                self._suffix = RasterFileType.get_by_value(self._file.suffix)

        match self._data_type:
            case DataType.RASTER:
                self._target_suffix = self._suffix \
                    if self._benchmark_params.raster_target_format is None \
                    else self._benchmark_params.raster_target_format
            case DataType.VECTOR:
                self._target_suffix = self._suffix \
                    if self._benchmark_params.vector_target_format is None \
                    else self._benchmark_params.vector_target_format

    def _find_file(self, ending: [str]) -> Path:
        if self.type == FileType.FILE:
            raise NotImplementedError("Single Files are currently not supported")
        elif self.type == FileType.FOLDER:
            for e in ending:
                files = [f for f in self._controller_location.glob(e)]
                if len(files) > 0:
                    f = files[0]
                    match f.suffix:
                        case ".tif":
                            f.rename(f.with_suffix(".tiff"))

                    return Path(files[0].name)
        elif self.type == FileType.ZIP_ARCHIVE:
            raise NotImplementedError("Files inside ZIP are not getting renamed right now")
            # return Path(
            #     [f for f in zipfile.Path(self._controller_location).iterdir() if Path(f.name).match(ending)][0].name)
        else:
            raise FileNotFoundError(f"Could not find file with ending {ending} at/in {self}")

    @property
    def docker_dir(self) -> Path:
        return self._docker_dir if not self._preprocessed else self.docker_dir_preprocessed

    @property
    def docker_file(self) -> Path:
        return self.docker_dir.joinpath(self._file)

    @property
    def docker_file_preprocessed(self) -> Path:
        return self.docker_dir_preprocessed.joinpath(self._file).with_suffix(self._target_suffix.value)

    @property
    def docker_wkt(self) -> Path:
        return self.docker_file_preprocessed.with_suffix(".json")

    @property
    def docker_dir_preprocessed(self) -> Path:
        return self._docker_dir.joinpath(f"preprocessed_{self._benchmark_params}")

    @property
    def host_dir(self) -> Path:
        return self._host_dir if not self._preprocessed else self.host_dir_preprocessed

    @property
    def host_file(self) -> Path:
        return self.host_dir.joinpath(self._file)

    @property
    def host_file_preprocessed(self) -> Path:
        return self.host_dir_preprocessed.joinpath(self._file).with_suffix(self._target_suffix.value)

    @property
    def host_wkt(self) -> Path:
        return self.host_file_preprocessed.with_suffix(".json")

    @property
    def host_dir_preprocessed(self) -> Path:
        return self._host_dir.joinpath(f"preprocessed_{self._benchmark_params}")

    @property
    def name(self) -> str:
        return self._name

    @property
    def controller_location(self) -> Path:
        return self._controller_location

    @property
    def controller_file(self) -> Path:
        return self._controller_location.joinpath(self._file)

    @property
    def controller_wkt(self) -> Path:
        return self.controller_location.with_suffix(".json")

    def set_preprocessed(self):
        self._preprocessed = True

    @property
    def preprocessed(self) -> bool:
        return self._preprocessed

    @property
    def suffix(self) -> VectorFileType | RasterFileType:
        return self._suffix

    @property
    def target_suffix(self) -> VectorFileType | RasterFileType:
        return self._target_suffix

    @target_suffix.setter
    def target_suffix(self, suffix: VectorFileType | RasterFileType):
        self._target_suffix = suffix

    def get_crs(self) -> CRS:
        match self._data_type:
            case DataType.VECTOR:
                return geopandas.read_file(self.controller_file, rows=1).crs
            case DataType.RASTER:
                rio_epsg = rioxarray.open_rasterio(str(self.controller_file), masked=True).squeeze().rio.crs.to_epsg()

                return CRS.from_epsg(rio_epsg)

    def __str__(self):
        return ",".join(
            [str(self._controller_location),
             str(self._data_type),
             str(self._file),
             self._name,
             str(self._preprocessed)]
        )
