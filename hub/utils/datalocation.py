import json
import random
import string
import subprocess
import uuid
import zipfile
from abc import abstractmethod
from pathlib import Path

import pandas as pd
import pyproj
import shapely
from duckdb.duckdb import DuckDBPyConnection
from numpy.array_api import result_type
from pandas import DataFrame
from pyproj import CRS

from shapely import box
from shapely.geometry.polygon import Polygon

from hub.enums.stage import Stage
from hub.executor.sqlbased import SQLBased
from hub.enums.datatype import DataType
from hub.enums.filetype import FileType
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters
from hub.enums.rasterfiletype import RasterFileType
from hub.enums.vectorfiletype import VectorFileType
from hub.utils.system import System
from hub.utils.network import BasicNetworkManager

gdal_docker_image = "ghcr.io/osgeo/gdal:alpine-small-latest"

class DataLocation:
    """
    class that wraps either a vector or a raster dataset. returns necessary file names depending on the requesting entity
    """
    _host_base: Path
    _files: list[Path]
    _data_type: DataType
    _dir_name: str
    _preprocessed: bool
    _controller_base: Path
    _target_suffix: VectorFileType | RasterFileType
    _suffix: VectorFileType | RasterFileType
    _allowed_endings: list[str]
    _metadata: list[dict]
    _uuid: str

    EXTENT_CRS = pyproj.CRS.from_epsg(9822)

    def __init__(self,
                 path_str: str,
                 data_type: DataType,
                 host_params: HostParameters,
                 name: str = None,
                 uuid_name: bool = False) -> None:
        """
        initiializes the data lcoation. if the proveided path is a folder or zip archive, it tries to find the first
        available file. also prepares paths for the host and docker container based on the file
        :param path_str: the path to the dataset
        :param data_type: the class of data stored
        :param host_params: the hsot parameters
        :param name: the name of the dataset
        """
        self._host_base = host_params.host_base_path
        self._data_type = data_type
        self._benchmark_params = None

        path = Path(path_str).expanduser()

        if not path.exists():
            raise FileNotFoundError(f"Path {path_str} to input for {data_type} data does not exist")

        if path.is_dir():
            self.type = FileType.FOLDER
            self._controller_base = path

            self._files = self._find_files()
        elif zipfile.is_zipfile(path):
            self.type = FileType.ZIP_ARCHIVE
            raise NotImplementedError("ZIP Files are currently not supported")
        else:
            self.type = FileType.FILE
            self._controller_base = path.parent
            self._files = [Path(path.name)]

        self._fix_files()

        self._docker_base = Path("/data")

        self._dir_base = self._controller_base.stem if name is None else name
        self._name_override = None

        self._host_base = self._host_base.joinpath("data")

        self._preprocessed_dir_override = None

        self._preprocessed = False
        self._should_preprocess = True
        self._is_ingested = False
        self.get_metadata()
        self._uuid = str(uuid.uuid4())
        if uuid_name:
            self._preprocessed_dir_override = Path("preprocessed_" + str(self._uuid))
            self._name_override = self.dataset_name + "_" + str(self._uuid)


        self._overwrite_metadata = None

        # if self._data_type == DataType.VECTOR:
        #     self.type = json.loads(
        #         subprocess.check_output(f"ogrinfo -nocount -json -nomd {self.controller_file}", shell=True).decode(
        #             "utf-8"))["layers"][0]["fields"]


    def use_uuid_name(self):
        self._preprocessed_dir_override = Path("preprocessed_" + str(self._uuid))
        self._name_override = self.dataset_name + "_" + str(self._uuid)

    @abstractmethod
    def adjust_target_files(self, benchmark_params: BenchmarkParameters):
        self._benchmark_params = benchmark_params

    @property
    def _preprocessed_dir_base(self):
        """
        the directory where preprocessed files are stored
        :return:
        """
        return Path(f"preprocessed_{self._benchmark_params}")

    def _find_files(self) -> list[Path]:
        """
        find a dataset file within a folder. Normalizes tiff files to a common file ending
        :return:
        """

        if self.type == FileType.FOLDER:
            all_relevant_files = [self._controller_base.glob(e) for e in self._allowed_endings]
            arf_flat = [item for sublist in all_relevant_files for item in sublist]

            if len(arf_flat) == 0:
                raise FileNotFoundError(f"Could not find any relevant file in {self._controller_base}")

            if len(set(f.suffix for f in arf_flat)) > 1:
                raise ValueError(
                    f"Multiple file types found in {self._controller_base}: {set(f.suffix for f in arf_flat)}")

            return [Path(f.name) for f in arf_flat]

        elif self.type == FileType.ZIP_ARCHIVE:
            raise NotImplementedError("ZIP Files are currently not supported")

            # return Path(
            #     [f for f in zipfile.Path(self._controller_location).iterdir() if Path(f.name).match(ending)][0].name)
        else:
            raise FileNotFoundError(f"Could not find file with ending {self._allowed_endings} at/in {self}")

    @property
    def _preprocessed_dir(self) -> Path:
        """
        the directory where preprocessed files are stored
        :return:
        """
        if self._preprocessed_dir_override is not None:
            return self._preprocessed_dir_override
        else:
            return self._preprocessed_dir_base

    @property
    def _dir_name(self):
        return self._dir_base #if not self._dir_override else self._dir_override

    @property
    def docker_dir(self) -> Path:
        """
        the dataset directory as seen from the docker container
        :return:
        """
        ddir = self._docker_base.joinpath(self._dir_name)
        if self._preprocessed:
            ddir = ddir.joinpath(self._preprocessed_dir)

        return ddir

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
        return self._docker_base.joinpath(self._dir_name).joinpath(self._preprocessed_dir)

    @property
    def host_data_base(self) -> Path:
        """
        the base data directory as seen from the host
        :return:
        """
        return self._host_base

    @property
    def host_dir(self) -> Path:
        """
        the dataset directory as seen from the host
        :return:
        """
        hdir = self._host_base.joinpath(self._dir_name)
        if self._preprocessed:
            hdir = hdir.joinpath(self._preprocessed_dir)

        return hdir

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
        return self._host_base.joinpath(self._dir_name).joinpath(self._preprocessed_dir)

    @property
    def name(self) -> str:
        """
        the name of the dataset
        :return:
        """
        return self._dir_base if not self._name_override else self._name_override

    @property
    def original_name(self) -> str:
        return self.name

    @property
    def dataset_name(self) -> str:
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
        return self._controller_base

    @property
    def controller_file(self) -> list[Path]:
        """
        the dataset file as available on the controller
        :return:
        """
        return [self._controller_base.joinpath(f) for f in self._files]

    def set_preprocessed(self, db_connection: DuckDBPyConnection, params: BenchmarkParameters, workload: dict,
                          nm: BasicNetworkManager, optimizer_run: bool = False):
        """
        switch that returned paths shall be output from the preprocess stage
        :return:"""
        self._preprocessed = True

        if self.should_preprocess:
            extent = self.get_remote_extent(nm)
            self.register_file(db_connection, params, workload, extent, True, optimizer_run)

    # def set_name(self, new_name: str = None):
    #     """
    #     set the name of the dataset
    #     :return:
    #     """
    #     self._dir_override = new_name or self.name
    #
    # def set_random_name(self):
    #     """
    #     set a random name for the dataset
    #     :return:
    #     """
    #     self._dir_override = self.original_name + ''.join(random.choice(string.ascii_lowercase) for i in range(16))
    #     self._preprocessed_dir_override = Path("preprocessed_" + ''.join(random.choice(string.ascii_lowercase) for i in range(16)))
    #     return self._overwrite_name

    def set_preprocessed_files(self, preprocessed_dir: str, files: list[str]):
        """
        set the files to be the preprocessed files
        :return:
        """
        self._preprocessed_dir_override = preprocessed_dir
        self._files = [Path(f) for f in files]


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

    @property
    def uuid(self) -> str:
        """
        the uuid of the dataset
        :return:
        """
        return self._uuid

    @uuid.setter
    def uuid(self, val: str):
        self._uuid = val
        self._name_override = self.dataset_name + "_" + str(val)

    @property
    def should_preprocess(self) -> bool:
        """
        whether the dataset should be preprocessed
        :return:
        """
        return self._should_preprocess

    @should_preprocess.setter
    def should_preprocess(self, val: bool):
        self._should_preprocess = val

    @property
    def is_ingested(self) -> bool:
        return self._is_ingested

    def set_ingested(self, val: bool, name: str | None = None, other_uuid: str | None = None):
        self._is_ingested = val

        if name is not None:
            self._name_override = name + "_" + str(other_uuid)



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

    @abstractmethod
    def get_metadata(self, from_remote: bool = False, nm: BasicNetworkManager = None) -> list[dict]:
        """
        return metadata of the dataset
        :return:
        """
        pass

    @abstractmethod
    def get_remote_metadata(self, nm: BasicNetworkManager) -> list[dict]:
        pass

    def get_vector_types(self):
        return

    @abstractmethod
    def get_extent(self) -> Polygon:
        """
        return the extent of the dataset
        :return:
        """
        pass

    @abstractmethod
    def get_remote_extent(self, nm: BasicNetworkManager) -> Polygon:
        pass

    @abstractmethod
    def get_remote_crs(self, nm: BasicNetworkManager) -> CRS:
        pass

    def transform_extent(self, poly: Polygon, crs: CRS) -> Polygon:
        """
        transform the extent of the dataset to the target CRS
        :param target_crs: the target CRS
        :return: the transformed extent
        """
        transformer = pyproj.Transformer.from_crs(crs, self.EXTENT_CRS, always_xy=True).transform
        return shapely.transform(poly, transformer, interleaved=False)

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


    def register_file(self, db_connection: DuckDBPyConnection, params: BenchmarkParameters, workload: dict, extent: Polygon, from_preprocess: bool, optimizer_run: bool = False):
        """
        inserts a file into the database
        :param file: the data location
        :param params: the benchmark parameters object
        :return:
        """
        if optimizer_run:
            self._preprocessed_dir_override = Path("preprocessed_" + str(self._uuid))
            self._name_override = self.dataset_name + "_" + str(self._uuid)

    @abstractmethod
    def find_available_ingested_files(self, db_connection: DuckDBPyConnection) -> DataFrame:
        """
        find available ingested files in the database
        :param db_connection: the database connection
        :return: a dataframe with all available ingested files
        """
        pass

    @abstractmethod
    def find_available_preprocessed_files(self, db_connection: DuckDBPyConnection) -> DataFrame:
        """
        find available preprocessed files in the database
        :param db_connection: the database connection
        :return: a dataframe with all available preprocessed files
        """
        pass

    def __str__(self):
        """
        string representation of the data location as a dict
        """
        str_dict = ",".join([f"{k}={v}" for k, v in self.__dict__.items()])
        return f"{self.__class__.__name__}({str_dict})"



class VectorLocation(DataLocation):
    """
    class that wraps a vector dataset. returns necessary file names depending on the requesting entity
    """

    _allowed_endings = ["*.shp"]

    def __init__(self,
                 path_str: str,
                 host_params: HostParameters,
                 name: str = None,
                 uuid_name: bool = False
                 ) -> None:
        """
        initiializes the data lcoation. if the proveided path is a folder or zip archive, it tries to find the first
        available file. also prepares paths for the host and docker container based on the file
        :param path_str: the path to the dataset
        :param host_params: the hsot parameters
        :param name: the name of the dataset
        """
        super().__init__(path_str, DataType.VECTOR, host_params, name, uuid_name)

        self._suffix = VectorFileType.get_by_value(self._files[0].suffix)

    def get_metadata(self, from_remote: bool = False, nm: BasicNetworkManager = None) -> list[dict]:
        """
        return metadata of the dataset
        :return:
        """

        if from_remote and nm is not None:
            self._metadata = self.get_remote_metadata(nm)
            return self._metadata

        def fetch_metadata(file) -> dict:
            return json.loads(
                subprocess.check_output(f'ogrinfo -json -nomd {file}', shell=True)
                .decode('utf-8'))
        self._metadata = [fetch_metadata(f) for f in self.controller_file]
        return self._metadata

    def get_feature_count(self) -> int:
        """
        return the number of features in the dataset
        :return:
        """
        return int(self._metadata[0]["layers"][0]["featureCount"])

    def get_selectivity(self, filter):
        if not filter:
            return 1.0

        filter_string = SQLBased.build_condition(filter, "", "and")

        features = json.loads(
            subprocess.check_output(
                f'ogrinfo -json -where "{filter_string}" -nomd {self.controller_file[0]}',
                shell=True)
            .decode('utf-8'))["layers"][0]["featureCount"]

        return features / self.get_feature_count()

    def get_extent(self) -> Polygon:
        """
        return the extent of the dataset
        :return:
        """

        def bbox_from_metadata(metadata):
            extent = metadata["layers"][0]["geometryFields"][0]["extent"]
            return box(xmin=float(extent[0]),
                       ymin=float(extent[1]),
                       xmax=float(extent[2]),
                       ymax=float(extent[3]))

        return self.transform_extent(
            Polygon(shapely.unary_union([bbox_from_metadata(m) for m in self._metadata]).convex_hull),
            self.get_crs()
        )

    def get_remote_metadata(self, nm: BasicNetworkManager) -> list[dict]:
        docker_command = f"docker run --rm -v {self._host_base}:/data {gdal_docker_image} ogrinfo -json -nomd"

        def fetch_metadata(file) -> dict:
            result_raw = nm.run_ssh_return_result(f"{docker_command} {file}")
            return json.loads(result_raw)

        return [fetch_metadata(f) for f in self.docker_file]

    def get_remote_extent(self, nm: BasicNetworkManager) -> Polygon:

        def bbox_from_meta(metadata):
            extent = metadata["layers"][0]["geometryFields"][0]["extent"]
            return box(xmin=float(extent[0]),
                       ymin=float(extent[1]),
                       xmax=float(extent[2]),
                       ymax=float(extent[3]))

        bboxes = [bbox_from_meta(f) for f in self.get_remote_metadata(nm)]

        poly = Polygon(shapely.unary_union(bboxes).convex_hull)

        return self.transform_extent(poly, self.get_remote_crs(nm))

    def get_remote_crs(self, nm: BasicNetworkManager) -> CRS:

        crs = self.get_remote_metadata(nm)[0]["layers"][0]["geometryFields"][0]["coordinateSystem"]["projjson"]["id"]["code"]

        return pyproj.CRS.from_epsg(crs)

    def get_feature_type(self) -> str:
        """
        return the feature type of the dataset
        :return:
        """
        return self._metadata[0]["layers"][0]["geometryFields"][0]["type"]


    def get_feature_type(self) -> str:
        """
        return the feature type of the dataset
        :return:
        """
        return self._metadata[0]["layers"][0]["geometryFields"][0]["type"]

    def get_crs(self) -> CRS:
        """
        return the CRS of the dataset
        :return:
        """
        crs = self._metadata[0]["layers"][0]["geometryFields"][0]["coordinateSystem"]["projjson"]["id"]["code"]
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
            raise ValueError(f"Multiple files found in {self._controller_base}: {self._files}")

    def register_file(self, db_connection: DuckDBPyConnection, params: BenchmarkParameters, workload: dict, extent: Polygon, from_preprocess: bool = False, optimizer_run: bool = False):
        if params.align_crs_at_stage == Stage.EXECUTION:
            crs = self.get_crs()
        else:
            crs = params.vector_target_crs

        locations = [str(f) for f in self._files]
        preprocessed_dir = str(self._preprocessed_dir)
        if params.system == System.POSTGIS and not from_preprocess:
            system = System.POSTGIS
            locations = [self.name]
            preprocessed_dir = ""
        else:
            system = "filesystem"

        is_converted_datatype = isinstance(self._target_suffix, RasterFileType)

        with db_connection as conn:
            dataset = conn.execute("""insert into available_files (id, name, location, preprocessed_dir, filetype, crs, extent, datatype, filter_predicate, vector_simplify, system, is_converted_datatype)
                                      values (?, ?, ?, ?, ?, ?, ST_GeomFromText(?::VARCHAR), ?, ?, ?, ?, ?) returning id""",
                                   [
                                    self._uuid,
                                    self.dataset_name,
                                    locations,
                                    preprocessed_dir,
                                    "vector",
                                    crs.name,
                                    extent.wkt,
                                    self.target_suffix.name,
                                    workload.get("condition", {}).get("vector", {}),
                                    params.vector_simplify,
                                    str(system),
                                    is_converted_datatype]
                                   ).fetchone()

            print(f"registered file: {dataset}")

            self._uuid = dataset[0]
            self._should_preprocess = True
            self._is_ingested = False

            super().register_file(db_connection, params, workload, extent, from_preprocess, optimizer_run)

    def find_available_ingested_files(self,
                                      db_connection: DuckDBPyConnection,
                                      ) -> DataFrame:
        """
        find available files in the database that match the current dataset
        :param db_connection: the database connection
        :return: a list of available files
        """
        with db_connection as conn:
            results = conn.execute(
                """select *
                   from available_files
                   where name = ?
                     and filetype = 'vector'
                     and system <> 'filesystem'
                """, # use filter
                [
                    self.dataset_name,
                ]).df()

            return results

    def find_available_preprocessed_files(self,
                                          db_connection: DuckDBPyConnection,
                                          ) -> DataFrame:
        """
        find available preprocessed files in the database that match the current dataset and benchmark parameters
        :param db_connection: the database connection
        :return: a list of available files
        """
        with db_connection as conn:
            results = conn.execute(
                """select *
                   from available_files
                   where name = ?
                     and filetype = 'vector'
                     and system = 'filesystem'
                """, # use filter
                [
                    self.dataset_name,
                ]).df()

            return results

class RasterLocation(DataLocation):
    """
    class that wraps a raster dataset. returns necessary file names depending on the requesting entity
    """

    _allowed_endings = ["*.tif", "*.tiff", "*.geotiff", "*.jp2"]

    def __init__(self,
                 path_str: str,
                 host_params: HostParameters,
                 name: str = None,
                 uuid_name: bool = False
                 ) -> None:
        """
        initiializes the data lcoation. if the proveided path is a folder or zip archive, it tries to find the first
        available file. also prepares paths for the host and docker container based on the file
        :param path_str: the path to the dataset
        :param host_params: the hsot parameters
        :param name: the name of the dataset
        """
        super().__init__(path_str, DataType.RASTER, host_params, name, uuid_name)

        self._suffix = RasterFileType.get_by_value(self._files[0].suffix)

    def get_width(self):
        return sum(m["size"][0] for m in self._metadata)

    def get_height(self):
        return sum(m["size"][1] for m in self._metadata)

    def get_pixels(self):
        return self.get_width() * self.get_height()

    def get_metadata(self, from_remote: bool = False, nm: BasicNetworkManager = None) -> list[dict]:
        """
        return metadata of the dataset
        :return:
        """

        if from_remote and nm is not None:
            self._metadata = self.get_remote_metadata(nm)
            return self._metadata


        def fetch_metadata(file) -> dict:
            return json.loads(
                subprocess.check_output(f'gdalinfo -json {file}', shell=True)
                .decode('utf-8'))
        self._metadata = [fetch_metadata(f) for f in self.controller_file]
        return self._metadata

    def get_extent(self):
        """
        return the extent of the dataset
        :return:
        """
        def bbox_from_meta(metadata):
            extent = metadata["cornerCoordinates"]
            return box(xmin=float(extent["lowerLeft"][0]),
                       ymin=float(extent["lowerLeft"][1]),
                       xmax=float(extent["upperRight"][0]),
                       ymax=float(extent["upperRight"][1]))

        return self.transform_extent(
            Polygon(shapely.unary_union([bbox_from_meta(m) for m in self._metadata]).convex_hull),
            self.get_crs()
        )

    def get_remote_metadata(self, nm: BasicNetworkManager) -> list[dict]:
        docker_command = f"docker run --rm -v {self._host_base}:/data {gdal_docker_image} gdalinfo -json"

        def fetch_metadata(file) -> dict:
            result_raw = nm.run_ssh_return_result(f"{docker_command} {file}")
            return json.loads(result_raw)

        return [fetch_metadata(f) for f in self.docker_file]

    def get_remote_extent(self, nm: BasicNetworkManager) -> Polygon:
        def bbox_from_meta(metadata):
            extent = metadata["cornerCoordinates"]
            return box(xmin=float(extent["lowerLeft"][0]),
                       ymin=float(extent["lowerLeft"][1]),
                       xmax=float(extent["upperRight"][0]),
                       ymax=float(extent["upperRight"][1]))

        bboxes = [bbox_from_meta(f) for f in self.get_remote_metadata(nm)]

        poly = Polygon(shapely.unary_union(bboxes).convex_hull)

        return self.transform_extent(poly, self.get_remote_crs(nm))

    def get_remote_crs(self, nm: BasicNetworkManager) -> CRS:

        crs = self.get_remote_metadata(nm)[0]["stac"]["proj:epsg"]

        return pyproj.CRS.from_epsg(crs)

    def get_crs(self) -> CRS:
        """
        return the CRS of the dataset
        """
        crs = self._metadata[0]["stac"]["proj:epsg"]

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

    def register_file(self, db_connection: DuckDBPyConnection, params: BenchmarkParameters, workload: dict, extent: Polygon, from_preprocess: bool = False, optimizer_run: bool = False):
        if params.align_crs_at_stage == Stage.EXECUTION:
            crs = params.raster_target_crs if params.align_to_crs == DataType.RASTER else params.vector_target_crs
        else:
            crs = params.raster_target_crs

        locations = [str(f) for f in self._files]
        preprocessed_dir = str(self._preprocessed_dir)
        if params.system == System.POSTGIS and not from_preprocess:
            system = System.POSTGIS
            locations = [self.name]
            preprocessed_dir = ""

        elif params.system == System.RASDAMAN and not from_preprocess:
            system = System.RASDAMAN
            locations = [self.name]
            preprocessed_dir = ""
        else:
            system = "filesystem"

        is_converted_datatype = isinstance(self._target_suffix, VectorFileType)

        with db_connection as conn:
            conn.execute("load spatial;")
            dataset = conn.execute(
                """insert into available_files (id, name, location, preprocessed_dir, filetype, crs, extent, datatype, raster_resolution, raster_depth, raster_tile_size, system, is_converted_datatype)
                                      values (?, ?, ?, ?, ?, ?, ST_GeomFromText(?::VARCHAR), ?, ?, ?, ?, ?, ?) returning id""",
                                   [
                                    self._uuid,
                                    self.dataset_name,
                                    locations,
                                    preprocessed_dir,
                                    "raster",
                                    crs.name,
                                    extent.wkt,
                                    self.target_suffix.name,
                                    params.raster_resolution,
                                    params.raster_depth,
                                    params.raster_tile_size.__dict__,
                                    str(system),
                                    is_converted_datatype]
            ).fetchone()

            print(f"registered file: {dataset}")

            self._uuid = dataset[0]
            self._should_preprocess = True
            self._is_ingested = False

            super().register_file(db_connection, params, workload, extent, from_preprocess, optimizer_run)

    def find_available_ingested_files(self,
                                      db_connection: DuckDBPyConnection,
                                      ) -> DataFrame:
        """
        find available files in the database that match the current dataset
        :param db_connection: the database connection
        :return: a list of available files
        """
        with db_connection as conn:
            results = conn.execute(
                """select *
                   from available_files
                   where name = ?
                     and filetype = 'raster'
                     and system <> 'filesystem'
                     and ST_Contains(extent, ST_GeomFromText(?::VARCHAR)::POLYGON_2D)""",
                [
                    self.dataset_name,
                    self.get_extent().wkt
                ]).df()

            return results

    def find_available_preprocessed_files(self,
                                          db_connection: DuckDBPyConnection,
                                          ) -> DataFrame:
        """
        find available preprocessed files in the database that match the current dataset and benchmark parameters
        :param db_connection: the database connection
        :return: a list of available files
        """
        with db_connection as conn:
            results = conn.execute(
                """select *
                   from available_files
                   where name = ?
                     and filetype = 'raster'
                     and system = 'filesystem'
                     and ST_Contains(extent, ST_GeomFromText(?::VARCHAR)::POLYGON_2D)""",
                [
                    self.dataset_name,
                    self.get_extent().wkt
                ]).df()

            return results
