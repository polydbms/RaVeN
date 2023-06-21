import zipfile
from pathlib import Path

import geopandas.io.file
from pyproj import CRS
from rioxarray import rioxarray

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters
from hub.enums.datatype import DataType
from hub.enums.filetype import FileType
from hub.enums.rasterfiletype import RasterFileType
from hub.enums.vectorfiletype import VectorFileType


class DataLocation:
    """
    class that wraps either a vector or a raster dataset. returns necessary file names depending on the requesting entity
    """
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
        """
        initiializes the data lcoation. if the proveided path is a folder or zip archive, it tries to find the first
        available file. also prepares paths for the host and docker container based on the file
        :param path: the path to the dataset
        :param data_type: the class of data stored
        :param host_params: the hsot parameters
        :param benchmark_params: the benchmark-specific parameters
        :param name: the name of the dataset
        """
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
        self._file = self._find_file(
            ["*.shp"] if self._data_type == DataType.VECTOR else ["*.tif", "*.tiff", "*.geotiff", "*.jp2"])

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
        """
        find a dataset file within a folder. Normalizes tiff files to a common file ending
        :param ending: the file endings possible to be used by benchi
        :return:
        """
        if self.type == FileType.FILE:
            raise NotImplementedError("Single Files are currently not supported")
        elif self.type == FileType.FOLDER:
            for e in ending:
                files = [f for f in self._controller_location.glob(e)]
                if len(files) > 0:
                    f = files[0]
                    if f.suffix in {".tif", ".tiff", ".geotiff"}.difference({str(RasterFileType.TIFF.value)}):
                        new_f = f.with_suffix(str(RasterFileType.TIFF.value))
                        f.rename(new_f)
                        return Path(new_f.name)

                    return Path(files[0].name)
        elif self.type == FileType.ZIP_ARCHIVE:
            raise NotImplementedError("Files inside ZIP are not getting renamed right now")
            # return Path(
            #     [f for f in zipfile.Path(self._controller_location).iterdir() if Path(f.name).match(ending)][0].name)
        else:
            raise FileNotFoundError(f"Could not find file with ending {ending} at/in {self}")

    @property
    def docker_dir(self) -> Path:
        """
        the dataset directory as seen from the docker container
        :return:
        """
        return self._docker_dir if not self._preprocessed else self.docker_dir_preprocessed

    @property
    def docker_file(self) -> Path:
        """
        the dataset file as seen from the docker container
        :return:
        """
        return self.docker_dir.joinpath(self._file)

    @property
    def docker_file_preprocessed(self) -> Path:
        """
        the dataset file as seen from the docker container after the preprocess stage has been completed
        :return:
        """
        return self.docker_dir_preprocessed.joinpath(self._file).with_suffix(self._target_suffix.value)

    @property
    def docker_wkt(self) -> Path:
        """
        the dataset file as a json containing well-known text as seen form the docker container
        :return:
        """
        return self.docker_file_preprocessed.with_suffix(".json")

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
    def host_file(self) -> Path:
        """
        the dataset file as seen from the host
        :return:
        """
        return self.host_dir.joinpath(self._file)

    @property
    def host_file_preprocessed(self) -> Path:
        """
        the dataset directory as seen from the host after the preprocess stage has been completed
        :return:
        """
        return self.host_dir_preprocessed.joinpath(self._file).with_suffix(self._target_suffix.value)

    @property
    def host_wkt(self) -> Path:
        """
        the dataset file as a json containing well-known text as seen form the host
        :return:
        """
        return self.host_file_preprocessed.with_suffix(".json")

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
        return self._name

    @property
    def controller_location(self) -> Path:
        """
        the dataset directory as available on the controller
        :return:
        """
        return self._controller_location

    @property
    def controller_file(self) -> Path:
        """
        the dataset file as available on the controller
        :return:
        """
        return self._controller_location.joinpath(self._file)

    @property
    def controller_wkt(self) -> Path:
        """
        the dataset file as a json containing well-known text as seen form the host
        :return:
        """
        return self.controller_location.with_suffix(".json")

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

    def get_crs(self) -> CRS:
        """
        return the CRS of the dataset
        :return:
        """
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
