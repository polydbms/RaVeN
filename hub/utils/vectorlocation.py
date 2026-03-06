import json
import subprocess
from pathlib import Path

import pyproj
import shapely
from duckdb.duckdb import DuckDBPyConnection
from pandas import DataFrame
from pyproj import CRS
from shapely import box
from shapely.geometry.polygon import Polygon

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters
from hub.enums.datatype import DataType
from hub.enums.rasterfiletype import RasterFileType
from hub.enums.stage import Stage
from hub.enums.vectorfiletype import VectorFileType
from hub.executor.sqlbased import SQLBased
from hub.utils.datalocation import DataLocation
from hub.utils.network import BasicNetworkManager
from hub.utils.system import System


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

    def get_extent(self, use_extent_crs: bool=True) -> Polygon:
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

        unified_poly = shapely.unary_union([bbox_from_metadata(m) for m in self._metadata])

        if use_extent_crs:
            return self.transform_extent(unified_poly, self.get_crs())
        else:
            return unified_poly

    def get_extent_per_file(self, use_extent_crs: bool=True) -> dict[Path, Polygon]:
        return {self.files[0]: self.get_extent(use_extent_crs=use_extent_crs)}

    def get_remote_metadata(self, nm: BasicNetworkManager) -> list[dict]:
        docker_command = f"docker run --rm -v {self._host_base}:/data {self.GDAL_DOCKER_IMAGE} ogrinfo -json -nomd"

        def fetch_metadata(file) -> dict:
            result_raw = nm.run_ssh_return_result(f"{docker_command} {file}")
            return json.loads(result_raw)

        return [fetch_metadata(f) for f in self.docker_file]

    def get_remote_extent(self, nm: BasicNetworkManager, use_extent_crs: bool=True) -> Polygon:

        def bbox_from_meta(metadata):
            extent = metadata["layers"][0]["geometryFields"][0]["extent"]
            return box(xmin=float(extent[0]),
                       ymin=float(extent[1]),
                       xmax=float(extent[2]),
                       ymax=float(extent[3]))

        bboxes = [bbox_from_meta(f) for f in self.get_remote_metadata(nm)]

        poly = Polygon(shapely.unary_union(bboxes))

        if use_extent_crs:
            return self.transform_extent(poly, self.get_remote_crs(nm))
        else:
            return poly

    def get_remote_crs(self, nm: BasicNetworkManager, idx: int=0) -> CRS:

        crs = self.get_remote_metadata(nm)[idx]["layers"][0]["geometryFields"][0]["coordinateSystem"]["projjson"]["id"]["code"]

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

    def get_crs(self, idx: int=0) -> CRS:
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
        if self._is_multifile:
            raise ValueError(f"Multiple files found in {self._controller_base}: {self.files}")

    def register_file(self, db_connection: DuckDBPyConnection, params: BenchmarkParameters, workload: dict, extent: Polygon, from_preprocess: bool = False, optimizer_run: bool = False, network_manager: BasicNetworkManager = None):
        if params.align_crs_at_stage == Stage.EXECUTION:
            crs = self.get_crs()
        else:
            crs = params.vector_target_crs

        locations = [str(f) for f in self.files]
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

            super().register_file(db_connection, params, workload, extent, from_preprocess, optimizer_run, network_manager)

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
                   where name = $name
                     and filetype = 'vector'
                     and system <> 'filesystem'
                """, # use filter
                {
                    "name": self.dataset_name,
                }).df()

            return results

    def find_available_files(self,
                             db_connection: DuckDBPyConnection,
                             extent: Polygon,
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
                   where name = $name
                     and filetype = 'vector'
                     and system = 'filesystem'
                """, # use filter
                {
                    "name": self.dataset_name,
                }).df()

            return results

