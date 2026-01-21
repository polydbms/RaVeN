import argparse
import base64
import json
import math
import random
import re
import shutil
import string
import subprocess
from time import time
from functools import wraps
from pathlib import Path
from typing import Optional, Any
from venv import logger

import numpy as np
import pandas as pd
import geopandas as gpd
import pyproj
import rioxarray as rxr
import shapely.geometry
import shapely
from osgeo_utils import gdal_polygonize
from pyproj import CRS
from shapely import lib

from hub.executor.sqlbased import SQLBased
from hub.enums.rasterfiletype import RasterFileType
from hub.enums.vectorfiletype import VectorFileType
from hub.enums.vectorizationtype import VectorizationType
from hub.evaluation.measure_time import measure_time
from hub.utils.capabilities import Capabilities


def print_timings(dataset, comment):
    """
    print the amount of time taken for a given function as timing marker strings
    :param dataset: the class of data processed
    :param comment: the function that is running
    :return:
    """

    def decorator_print_timings(func):
        @wraps(func)
        def _decorator_print_timings(self, *args, **kwargs):
            print(f"benchi_marker,{time()},start,preprocess,{self.config.system},{dataset},{comment}")
            func(self, *args, **kwargs)
            print(f"benchi_marker,{time()},end,preprocess,{self.config.system},{dataset},{comment}")

        return _decorator_print_timings

    return decorator_print_timings


class PreprocessConfig:
    """
    class that wraps the preprocess parameters obtained from the controller
    """
    system: str

    _vector_folder: Path
    _vector_files: list[Path]
    vector_name: str
    vector_source_suffix: str
    vector_target_suffix: str
    vector_output_folder: Path
    vector_target_crs: CRS
    vectorization_type: VectorizationType
    vector_simplify: float

    _raster_folder: Path
    _raster_files: list[Path]
    raster_name: str
    raster_source_suffix: str
    raster_target_suffix: str
    raster_output_folder: Path
    raster_target_crs: CRS
    raster_resolution: float
    raster_singlefile: bool

    vector_filter: dict | str
    raster_filter: list[str]
    # vector_clip: bool
    raster_clip: bool

    # intermediate_folders: list[Path]  # FIXME this should be done properly

    def __init__(self, args, capabilities):
        """

        :param args: the arguments sent by the controller
        :param capabilities: the capabilities the systems have, based on the capabilities.yaml
        """
        print(args)

        self.base_path = Path(args.base_path)

        self.system = args.system

        self._vector_folder = Path(args.vector_path)
        self.vector_name = self._vector_folder.name
        self.vector_source_suffix = args.vector_source_suffix
        self._vector_files = self._find_files(self._vector_folder,
                                              VectorFileType.get_by_value(self.vector_source_suffix))

        self.vector_target_suffix = args.vector_target_suffix
        self.vector_output_folder = Path(args.vector_output_folder)
        self.vector_target_crs = CRS.from_epsg(args.vector_target_crs)
        self.vectorization_type = VectorizationType.get_by_value(args.vectorization_type)
        self.vector_simplify = args.vector_simplify
        self.extent_wkt = args.extent_wkt
        self.bbox = args.bbox
        self.bbox_srs = args.bbox_srs

        self._raster_folder = Path(args.raster_path)
        self.raster_name = self._raster_folder.name
        self.raster_source_suffix = args.raster_source_suffix
        self._raster_files = self._find_files(self._raster_folder,
                                              RasterFileType.get_by_value(self.raster_source_suffix))

        self.raster_target_suffix = args.raster_target_suffix
        self.raster_output_folder = Path(args.raster_output_folder)
        self.raster_target_crs = CRS.from_epsg(args.raster_target_crs)
        self.raster_resolution = args.raster_resolution
        self.raster_datatype = args.raster_datatype
        self.raster_singlefile = args.raster_singlefile

        self.vector_filter = json.loads(
            base64.b64decode(args.vector_filter).decode("utf-8")) if args.vector_filter else {}
        # self.raster_filter = []
        # self.vector_clip = False
        self.raster_clip = args.raster_clip

        self.intermediate_folders = []

        self.capabilities = capabilities

        self.should_preprocess_vector = args.preprocess_vector
        self.should_preprocess_raster = args.preprocess_raster



    def set_vector_folder(self, folder: Path):
        self._vector_folder = folder

    def set_vector_suffix(self, suffix: str):
        self._vector_files = [f.with_suffix(suffix) for f in self._vector_files]

    @property
    def vector_folder(self) -> Path:
        return self._vector_folder

    @property
    def vector_file(self) -> list[Path]:
        return self._vector_files

    @vector_file.setter
    def vector_file(self, file: list[Path]):
        self._vector_files = file

    @property
    def vector_file_path(self) -> list[Path]:
        return [self._vector_folder.joinpath(f) for f in self._vector_files]

    def set_raster_folder(self, folder: Path):
        self._raster_folder = folder

    def set_raster_suffix(self, suffix: str):
        self._raster_files = [Path(f).with_suffix(suffix) for f in self._raster_files]

    @property
    def raster_folder(self) -> Path:
        return self._raster_folder

    @property
    def raster_file(self) -> list[Path]:
        return self._raster_files

    @raster_file.setter
    def raster_file(self, file: list[Path]):
        self._raster_files = file

    @property
    def raster_file_path(self) -> list[Path]:
        return [self._raster_folder.joinpath(f) for f in self._raster_files]

    def remove_intermediates(self):
        """
        remove intermediate results from the file system
        :return:
        """
        for folder in self.intermediate_folders:
            print(f"removing {folder}")
            subprocess.call(f"rm -r {folder}", shell=True)

    def copy_to_output(self):
        """
        copy the final intermediate result to the output folder
        :return:
        """
        if self.should_preprocess_vector:
            print(f"copying vector files from {self.vector_folder}")
            shutil.rmtree(self.vector_output_folder, ignore_errors=True)
            self.vector_output_folder.mkdir(parents=True, exist_ok=True)
            for f in self.vector_folder.iterdir():
                if f.is_file():
                    print(f"copy {f} to {self.vector_output_folder}")
                    shutil.copy(f, self.vector_output_folder)
            # shutil.copytree(self.vector_folder, self.vector_output_folder, dirs_exist_ok=True)

        if self.should_preprocess_raster:
            print(f"copying raster files from {self.raster_folder}")
            shutil.rmtree(self.raster_output_folder, ignore_errors=True)
            self.raster_output_folder.mkdir(parents=True, exist_ok=True)
            for f in self.raster_folder.iterdir():
                if f.is_file():
                    print(f"copy {f} to {self.raster_output_folder}")
                    shutil.copy(f, self.raster_output_folder)

            if self.system in self.capabilities["require_geotiff_ending"] or True:
                for f in self.raster_output_folder.iterdir():
                    if f.suffix == ".tiff":
                        f.with_suffix(".geotiff").symlink_to(f)
                        print(f'created symlink {f.with_suffix(".geotiff")} to file {f}')
            # shutil.copytree(self.raster_folder, self.raster_output_folder, dirs_exist_ok=True)

    @staticmethod
    def get_random_str(length):
        """
        generate a random string
        :param length: the length of the string
        :return: the random string
        """
        return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

    @staticmethod
    def _find_files(folder: Path, suffix: [VectorFileType | RasterFileType]) -> list[Path]:
        """
        find a file based on the given class of spatial data
        :param folder: the folder teh file resides in
        :param suffix: the file type
        :return:
        """
        files = [f for f in folder.glob(f"*{suffix.value}")]
        if len(files) > 0:
            return [Path(f.name) for f in files]
        else:
            raise FileNotFoundError(f"no file found in {folder} with suffix {suffix}")

    def __str__(self):
        return ", ".join([
            str(self.system),
            str(self._vector_folder),
            str(self._vector_files),
            str(self.vector_name),
            str(self.vector_source_suffix),
            str(self.vector_target_suffix),
            str(self.vector_output_folder),
            str(self.vector_target_crs),
            str(self.vectorization_type),
            str(self.vector_simplify),
            str(self._raster_folder),
            str(self._raster_files),
            str(self.raster_name),
            str(self.raster_source_suffix),
            str(self.raster_target_suffix),
            str(self.raster_output_folder),
            str(self.raster_target_crs),
            str(self.raster_resolution),
            str(self.raster_clip),
            str(self.vector_filter),
            str(self.extent_wkt),
            str(self.bbox),
            str(self.bbox_srs)
        ])


class Preprocessor:
    """
    main utility for preprocessing data
    """

    def __init__(self, config: PreprocessConfig, base_tmp_folder: Path) -> None:
        """
        init
        :param config: the config the preprocessing steps shall be based on
        :param base_tmp_folder: the path to the root of temporary locations
        """
        self.logger = {}
        self.config = config
        self.base_tmp_folder = base_tmp_folder
        self.base_tmp_folder.mkdir(parents=True, exist_ok=True)

        self._vector_tmp_out_folder = self.base_tmp_folder \
            .joinpath(f"{self.config.vector_name}_{PreprocessConfig.get_random_str(12)}")
        self._vector_tmp_out_folder.mkdir(parents=True, exist_ok=True)

        self._raster_tmp_out_folder = self.base_tmp_folder \
            .joinpath(f"{self.config.raster_name}_{PreprocessConfig.get_random_str(12)}")
        self._raster_tmp_out_folder.mkdir(parents=True, exist_ok=True)

        self.config.intermediate_folders.append(self._vector_tmp_out_folder)
        self.config.intermediate_folders.append(self._raster_tmp_out_folder)

        self.raster_meta = None
        self.vector_meta = None

        # self.vector_path = None
        # self.raster_path = None
        # if config.vector_path and (Path(config.vector_path).exists() and Path(config.vector_path).is_dir()):
        #     self.vector_path = [vector for vector in Path(config.vector_path).glob("*.shp")][0]
        # if config.raster_path and (Path(config.raster_path).exists() and Path(config.raster_path).is_dir()):
        #     for ending in ["*.tif", "*.tiff", "*.jp2"]:
        #         files = [f for f in Path(config.raster_path).glob(ending)]
        #         if len(files) > 0:
        #             self.raster_path = Path(files[0])
        #             break

    def get_raster_meta(self) -> dict[Any, Any]:
        """
        return the CRS of the dataset
        """

        if self.raster_meta is None:
            self.raster_meta = json.loads(
                subprocess.check_output(f'gdalinfo -json {self.config.raster_file_path[0]}', shell=True)
                .decode('utf-8'))

        return self.raster_meta

    def get_vector_meta(self) -> dict[Any, Any]:
        """
        return the metadata of the vector dataset
        """
        if self.vector_meta is None:
            self.vector_meta = json.loads(
                subprocess.check_output(f'ogrinfo -json -nocount -nomd {self.config.vector_file_path[0]}', shell=True)
                .decode('utf-8'))["layers"][0]

        return self.vector_meta

    def get_raster_crs(self) -> CRS:
        """
        return the CRS of the dataset
        """
        crs = self.get_raster_meta()["stac"]["proj:epsg"]

        return pyproj.CRS.from_epsg(crs)

    def get_vector_crs(self) -> CRS:
        """
        return the CRS of the dataset
        :return:
        """
        crs = self.get_vector_meta()["geometryFields"][0]["coordinateSystem"]["projjson"]["id"]["code"]
        return pyproj.CRS.from_epsg(crs)

    # def get_vector(self):
    #     vector = gpd.read_file(self.config.vector_file_path[0]) #FIXME
    #     return vector
    #
    # def get_vector_crs(self):
    #     return self.get_vector().crs

    def get_raster(self):
        return rxr.open_rasterio(self.config.raster_file_path[0], masked=True).squeeze() #FIXME
    #
    # def get_raster_crs(self):
    #     return self.get_raster().rio.crs

    def update_vector_folder(self):
        """
        sets the new (temporary) location of data for a vector dataset
        :return:
        """
        self.config.set_vector_folder(self._vector_tmp_out_folder)

    def update_vector_suffix(self):
        """
        updates the suffix to the one the vector data should have in the end
        :return:
        """
        self.config.set_vector_suffix(self.config.vector_target_suffix)

    def update_raster_folder(self):
        """
        sets the new (temporary) location of data for a raster dataset
        :return:
        """
        self.config.set_raster_folder(self._raster_tmp_out_folder)

    def update_raster_suffix(self):
        """
        updates the suffix to the one the raster data should have in the end
        :return:
        """
        self.config.set_raster_suffix(self.config.raster_target_suffix)


class MultiFilePreprocessor(Preprocessor):
    """
    preprocessor for changing the cardinality of the files
    """

    def __init__(self, config: PreprocessConfig) -> None:
        super().__init__(config, config.base_path.joinpath(f"multifile_tmp"))

    @measure_time
    @print_timings("raster", "merge")
    def merge_raster(self, *args, **kwargs):
        """
        merges multiple raster files into one
        :param args:
        :param kwargs:
        :return:
        """
        print(f"merging raster files {self.config.raster_file}")

        output_file = self._raster_tmp_out_folder.joinpath(self.config.raster_folder.stem + "_merged").with_suffix(
            self.config.raster_source_suffix)

        input_files = " ".join([str(self.config.raster_folder.joinpath(f)) for f in self.config.raster_file])

        cmd_string = f"gdal_warp -multi --config GDAL_CACHEMAX 200000 -wm 200000 " \
                     f"-t_srs {self.config.raster_target_crs} " \
                     f"{input_files} " \
                     f"{output_file}"

        subprocess.call(cmd_string, shell=True)

        self.config.raster_file = [output_file]
        self.update_raster_folder()

    @measure_time
    @print_timings("vector", "merge")
    def merge_vector(self, *args, **kwargs):
        """
        merges multiple vector files into one
        :param args:
        :param kwargs:
        :return:
        """
        print(f"merging vector files {self.config.vector_file}")

        output_file = self._vector_tmp_out_folder.joinpath(self.config.vector_folder.stem + "_merged").with_suffix(
            self.config.vector_source_suffix)

        input_files = " ".join([str(self.config.vector_folder.joinpath(f)) for f in self.config.vector_file])

        cmd_string = f"ogrmerge -single " \
                      f"-t_srs {self.config.vector_target_crs} " \
                      f"{input_files} " \
                      f"{output_file} "

        subprocess.call(cmd_string, shell=True)

        self.config.vector_file = [output_file]
        self.update_vector_folder()

    @measure_time
    @print_timings("raster", "split")
    def split_raster(self, *args, **kwargs):
        """
        splits a raster file into multiple files
        :param args:
        :param kwargs:
        :return:
        """
        print(f"splitting raster file {self.config.raster_file}")

        input_file = " ".join([str(r) for r in self.config.raster_file_path])
        output_folder = self._raster_tmp_out_folder

        max_width = kwargs.get("max_width", -1)
        max_height = kwargs.get("max_height", -1)


        if max_width < 0 or max_height < 0:
            ratio_2gb = self.config.raster_file_path[0].stat().st_size * 1.3 * 2 / (2 * 1024 * 1024 * 1024)  # size in GB

            subdivs_per_axis = int(np.ceil(np.sqrt(ratio_2gb)))
            subdivs_x = subdivs_y = subdivs_per_axis

            if subdivs_per_axis == 1:
                return

            max_width = int(math.ceil(self.get_raster_meta()["size"][0] / subdivs_per_axis))
            max_height = int(math.ceil(self.get_raster_meta()["size"][1] / subdivs_per_axis))
        else:
            subdivs_x = int(math.ceil(self.get_raster_meta()["size"][0] / max_width))
            subdivs_y = int(math.ceil(self.get_raster_meta()["size"][1] / max_height))

        print(f"subdividing raster into {subdivs_x} x {subdivs_y} tiles of size {max_width} x {max_height}")

        has_next_step = kwargs.get("has_next_step", False)

        self.config.raster_file = []
        for x in range(0, subdivs_x):
            for y in range(0, subdivs_y):
                cmd_string = f"gdal_translate " \
                             f"-srcwin {x * max_width} {y * max_height} {max_width} {max_height} "

                desired_suffix = self.config.raster_target_suffix

                if has_next_step:
                    cmd_string += "-of VRT "
                    desired_suffix = ".vrt"

                output_file = output_folder.joinpath(f'{self.config.raster_name}_{x}_{y}').with_suffix(desired_suffix)

                cmd_string += f"{input_file} " \
                                f"{output_file} "

                subprocess.call(cmd_string, shell=True)

                self.config.raster_file.append(output_file)


        self.update_raster_folder()


class CRSFilterPreprocessor(Preprocessor):
    """
    preprocessor for performing coordinate reference system transformations
    """

    def __init__(self, config: PreprocessConfig) -> None:
        super().__init__(config, config.base_path.joinpath(f"reproject_tmp"))

    @measure_time
    @print_timings("raster", "reproject")
    def clip_reproject_resolution_raster(self, *args, **kwargs):
        """
        reprojects a raster dataset to the target CRS using gdalwarp
        :param args:
        :param kwargs:
        :return:
        """
        print(f"{'clipping and ' if self.config.raster_clip else ''}reprojecting raster file {self.config.raster_file}")

        cmd_string = f"gdalwarp " \
                     f"-t_srs {self.config.raster_target_crs} "

        if self.config.raster_clip:  # and False:
            # fix this by re-tiling all datasets from top-left, if a tile size is given. otherwise, use merged dataset

            if len(self.config.raster_file) > 1:
                logger.warning("raster clipping is not supported for multiple raster files. merging files. this may take a while")
                mf_preprocessor = MultiFilePreprocessor(self.config)
                mf_preprocessor.merge_raster()

            extent = np.asarray(self.get_vector_meta()["geometryFields"][0]["extent"])

            # affine_transf = self.get_raster().rio.transform()
            affine_transf = self.get_raster_meta()["geoTransform"]

            pixel_size = max(affine_transf[1], -affine_transf[5])
            fixpoint = np.asarray([affine_transf[0], affine_transf[3], affine_transf[0], affine_transf[3]]) \
                .reshape((2, 2))
            transformer = pyproj.Transformer.from_crs(self.get_vector_crs(), self.get_raster_crs(),
                                                      always_xy=True).transform

            extent_proj = np.asarray(shapely.transform(shapely.geometry.box(*extent), transformer, include_z=False,
                                               # FIXME switch back to shapely if possible
                                               interleaved=False).bounds).reshape((2, 2))

            px_count = (extent_proj - fixpoint) / pixel_size
            px_count[0] = np.floor(px_count[0]) - 2
            px_count[1] = np.ceil(px_count[1]) + 2

            (l, b, r, t) = list((px_count * pixel_size + fixpoint).reshape((4, 1)[0]))

            cmd_string += f"{f'-te_srs {self.get_raster_crs().to_string()} -te {l} {b} {r} {t}' if self.config.raster_clip else ''} "


        current_raster_type = self.get_raster_meta()["bands"][0]["type"]
        if ((self.config.raster_datatype or self.config.system in self.config.capabilities["raster_require_double_precision"])
                and current_raster_type != self.config.raster_datatype):
            if self.config.system in self.config.capabilities["raster_require_double_precision"]:
                print("current raster type:", current_raster_type)
                match current_raster_type:
                    case ("Float32" | "CFloat32" | "CFloat64"):
                        self.config.raster_datatype = "Float64"
                    case ("Byte" | "Int8" | "Int16" | "UInt16", "CInt16"):
                        self.config.raster_datatype = "Int32"
                    case ("UInt32", "CInt32"):
                        self.config.raster_datatype = "Int64"
                    case _:
                        self.config.raster_datatype = current_raster_type


            cmd_string += f"-ot {self.config.raster_datatype} "

        for f in self.config.raster_file_path:

            output_file = self._raster_tmp_out_folder.joinpath(f.name)

            if self.config.raster_resolution != 1.0:
                width, height = json.loads(
                    subprocess.check_output(f'gdalinfo -json {self.config.raster_file_path}', shell=True)
                    .decode('utf-8'))["size"]
                cmd_string += f"-ts {int(width / self.config.raster_resolution)} {int(height / self.config.raster_resolution)} "

            target_suffix = self.config.raster_target_suffix if not self.config.raster_target_suffix in [".shp", ".geojson"] else self.config.raster_source_suffix

            cmd_string += f"{f} " \
                          f"{output_file.with_suffix(target_suffix)} " \
                          f"--debug ON " \
                          f""

            print(f"executing command for raster: {cmd_string}")
            subprocess.call(cmd_string, shell=True)

        # TODO this could probably be streamlined for efficiency

        self.update_raster_folder()

        print(f"Transferred {self.config.raster_file_path} CRS to {self.config.raster_target_crs}")

    @measure_time
    @print_timings("vector", "reproject")
    def filter_reproject_simplify_vector(self, *args, **kwargs):
        """

        reprojects a vector dataset to the target CRS using ogr2ogr
        :param args:
        :param kwargs:
        :return:
        """
        print(
            f"{'filtering and ' if self.config.vector_filter else ''}reprojecting vector file {self.config.vector_file}")

        # vector = self.get_vector()
        # out = vector.to_crs(self.config.vector_target_crs)
        # out.to_file(self._vector_tmp_out_folder.joinpath(self.config.vector_file), encoding="UTF-8")

        filter_string = SQLBased.build_condition(self.config.vector_filter, "",
                                                 "and") if self.config.vector_filter else ""

        singleparts = "-explodecollections" if self.config.system in Capabilities.read_capabilities().get(
            "requires_singleparts") else ""

        output_file = self._vector_tmp_out_folder.joinpath(self.config.vector_file[0])
        cmd_string = f"ogr2ogr " \
                     f"-t_srs {self.config.vector_target_crs} " \
                     f"""{'-where "' if self.config.vector_filter else ''} {filter_string} {'"' if self.config.vector_filter else ''} """ \
                     f"""{f'-clipsrc "{self.config.extent_wkt}"' if self.config.extent_wkt else ''} """ \
                     f"""{f'-spat {" ".join(self.config.bbox)} -spat_srs "EPSG:{self.config.bbox_srs}"' if self.config.bbox else ''} """ \
                     f"-simplify {self.config.vector_simplify} " \
                     f"{singleparts} " \
                     f"{output_file} " \
                     f"{self.config.vector_file_path[0]} " \
                     f"-lco ENCODING=UTF-8 " \
                     f"--debug ON " \
                     f""

        print(f"executing command for vector: {cmd_string}")

        subprocess.call(cmd_string,
                        shell=True)

        self.update_vector_folder()

        print(f"Transfered {self.config.vector_file_path} CRS to {self.config.vector_target_crs}")
        # print(f"Transfered {self.config.vector_file_path} CRS from {vector.crs} to {out.crs}")


class FileConverterPreprocessor(Preprocessor):
    """
    preprocessor for performing translations between different file types
    """

    def __init__(self, config: PreprocessConfig) -> None:
        super().__init__(config, config.base_path.joinpath(f"translate_tmp"))

    @measure_time
    @print_timings("raster", "translate")
    def raster_to_geotiff(self, *args, **kwargs):
        """
        translates a raster dataset into a GeoTIFF
        :param args:
        :param kwargs:
        :return:
        """

        for f in self.config.raster_file_path:
            print(f"translating raster file {self.config.raster_file} to geotiff")

            output_file = self._raster_tmp_out_folder \
                .joinpath(f.name).with_suffix(self.config.raster_target_suffix)

            subprocess.call(f"gdal_translate -of GTiff {f} {output_file}", shell=True)

            print(f"translated raster file to {output_file}")

        self.update_raster_suffix()
        self.update_raster_folder()

    @measure_time
    @print_timings("raster", "translate")
    def raster_to_xyz(self):
        """
        transforms a raster dataset into a xyz file
        :return:
        """

        for idx, f in enumerate(self.config.raster_file_path):
            print(f"translating raster file {f} to xyz")

            output_file = self._raster_tmp_out_folder \
                .joinpath(f).with_suffix(f".xyz.{idx}")

            subprocess.call(f"gdal_translate -of XYZ {self.config.raster_file_path} {output_file}", shell=True)

            print(f"translated raster file to {output_file}")

        subprocess.call(
            f"cat {self._raster_tmp_out_folder}/*.xyz.* > {self._raster_tmp_out_folder}/{self.config.raster_file}.xyz",
            shell=True)

        self.config.set_raster_suffix(".xyz")
        self.update_raster_folder()

    @measure_time
    @print_timings("vector", "translate")
    def vector_to_csv_wkt(self, *args, **kwargs):
        """
        converts a vector file into a csv file with the geometries as WKT
        :param args:
        :param kwargs:
        :return:
        """
        for f in self.config.vector_file_path:
            print(f"converting file {f} to csv with WKT")

            output_file = self._vector_tmp_out_folder \
                .joinpath(f.name).with_suffix(".csv")

            subprocess.call(f"ogr2ogr -f CSV -lco GEOMETRY=AS_WKT {output_file} {f}", shell=True)

            print(f"translated vector file to {output_file}")

        self.config.set_vector_suffix(".csv")

        self.update_vector_folder()


class DataModelProcessor(Preprocessor):
    """
    preprocessor for converting classes of spatial data between each other
    """

    def __init__(self, config: PreprocessConfig, output=None) -> None:
        super().__init__(config, config.base_path.joinpath(f"transform_tmp"))

    @measure_time
    @print_timings("raster", "vectorize")
    def vectorize_polygons(self, *args, **kwargs):
        """
        vectorize a raster file into polygons using gdal_polygonize.py
        :param args:
        :param kwargs:
        :return:
        """
        for idx, f in enumerate(self.config.raster_file_path):
            print(f"vectorizing raster file {self.config.raster_file} with polygons")

            if len(self.config.raster_file_path) > 1:
                print("warning: cannot vectorize multiple raster files into one. only processing the first file")

            output_file = self._raster_tmp_out_folder \
                .joinpath(f.name).with_suffix(self.config.raster_target_suffix)

            gdal_polygonize.gdal_polygonize(src_filename=str(self.config.raster_file_path[0]),
                                            dst_filename=str(output_file),
                                            driver_name=self.get_driver_name(output_file),
                                            dst_fieldname="values")

            print(f"done vectorizing with polygons, saved output to {output_file}")

        self.update_raster_folder()
        self.update_raster_suffix()

    @print_timings("raster", "vectorize")
    def vectorize_points(self, *args, **kwargs):
        """

        vectorize a XYZ raster file into points using geopandas
        :param args:
        :param kwargs:
        :return:
        """

        # we assume there only exists one file after xyz conversion

        xyz_file = self.config.raster_file_path[0]

        output_file = self._raster_tmp_out_folder \
            .joinpath(self.config.raster_file[0]).with_suffix(self.config.raster_target_suffix)

        points = pd.read_csv(xyz_file, header=None, names=["x", "y", "values"], sep=" ")
        geo_points = gpd.GeoDataFrame({
            "geometry": gpd.points_from_xy(points["x"], points["y"], crs=self.config.raster_target_crs),
            "values": points["values"]
        })

        geo_points.to_file(str(output_file), driver=self.get_driver_name(output_file), encoding="UTF-8")

        print(f"done vectorizing with polygons, saved output to {output_file}")

        self.update_raster_folder()
        self.update_raster_suffix()

    def get_driver_name(self, output_file):
        """
        returns the GDAL Driver name corresponding to a file type
        :param output_file:
        :return:
        """
        raster_target_suffix = VectorFileType.get_by_value(self.config.raster_target_suffix)

        if raster_target_suffix == VectorFileType.SHP:
            with open(output_file.with_suffix(".prj"), 'w') as f:
                f.write(self.config.raster_target_crs.to_proj4())

            return "ESRI Shapefile"

        elif raster_target_suffix == VectorFileType.GEOJSON:
            return "GeoJSON"
        else:
            raise Exception(f"Invalid Raster Target Suffix provided: {self.config.raster_target_suffix}")

    # @measure_time
    # @print_timings("vector", "rasterize")
    # def rasterize(
    #         self, file: str, pixel_size=10, nod_data=0, options=None, filters=None, **kwargs
    # ):
    #     print(f"rasterizing vector file {self.config.vector_file}")
    #     output_file = self.config.vector_file_path.with_suffix(self.config.vector_target_suffix)
    #     # output = f"{self.output}/{self.vector_path.stem}.tif"
    #     open_shp = ogr.Open(file)
    #     shp_layer = open_shp.GetLayer()
    #     source_srs = shp_layer.GetSpatialRef()
    #     for filter in filters:
    #         shp_layer.SetAttributeFilter(filter)
    #     x_min, x_max, y_min, y_max = shp_layer.GetExtent()
    #     # calculate raster resolution
    #     x_res = int((x_max - x_min) / pixel_size)
    #     y_res = int((y_max - y_min) / pixel_size)
    #     # set the image type for export
    #     driver = gdal.GetDriverByName("GTiff")
    #     new_raster = driver.Create(str(output_file), x_res, y_res, 1, gdal.GDT_Float32)
    #     new_raster.SetGeoTransform((x_min, pixel_size, 0, y_max, 0, -pixel_size))
    #     new_raster.SetProjection(source_srs.ExportToWkt())
    #     # get the raster band we want to export too
    #     raster_band = new_raster.GetRasterBand(1)
    #     # assign the no data value to empty cells
    #     raster_band.SetNoDataValue(nod_data)
    #     # run vector to raster on new raster with input Shapefile
    #     gdal.RasterizeLayer(new_raster, [1], shp_layer, options=options)
    #
    #     self.update_vector_folder()
    #     self.update_vector_suffix()


def main():
    """
    main method for preprocessing spatial datasets. is ivided into 3 stages

    1. transform the coordinate reference systems
    2. if necessary, translate file types
    3. if necessary, rasterize vector datasets or vectorize raster datasets
    :return:
    """
    capabilities = Capabilities.read_capabilities()
    parser = argparse.ArgumentParser()
    parser.add_argument("--base_path", help="Base path for all operations", required=False, default="/data")
    parser.add_argument("--vector_path", help="Specify the absolute path to vector dataset", required=True)
    parser.add_argument("--vector_source_suffix", help="source file suffix of the vector files", required=True)
    parser.add_argument("--vector_target_suffix", help="target file suffix of the vector files", required=True)
    parser.add_argument("--vector_output_folder", help="absolute path to the output folder of the vector files",
                        required=True)
    parser.add_argument("--vector_target_crs", help="target CRS for the vector file", required=True)
    parser.add_argument("--vectorization_type", help="vectorization type to be applied", required=True)
    parser.add_argument("--vector_simplify", help="simplify the vector geometries using douglas-peucker",
                        required=False, default=0.0, type=float)
    parser.add_argument("--raster_path", help="Specify the absolute path to raster dataset", required=True)
    parser.add_argument("--raster_source_suffix", help="source file suffix of the raster files", required=True)
    parser.add_argument("--raster_target_suffix", help="target file suffix of the raster files", required=True)
    parser.add_argument("--raster_output_folder", help="absolute path to the output folder of the raster files",
                        required=True)
    parser.add_argument("--raster_target_crs", help="target CRS for the raster file", required=True)
    parser.add_argument("--raster_resolution", help="target resolution for the raster file", required=False,
                        default=1.0, type=float)
    parser.add_argument("--raster_singlefile", help="whether the output of the raster should be a single file",
                        required=False,
                        action=argparse.BooleanOptionalAction)
    parser.add_argument("--raster_datatype", help="the datatype of the raster file", required=False, default=None)
    parser.add_argument("--system", help="Specify which system should be benchmarked")
    parser.add_argument("--vector_filter", help="Filters to be applied on the vector feature fields", required=False,
                        default="")
    # parser.add_argument("--raster-filter", help="Filters to be applied on the raster pixels", required=False, action="append", default=[])
    parser.add_argument("--raster_clip", help="Whether to clip the Raster on the vector extent", required=True,
                        action=argparse.BooleanOptionalAction)
    parser.add_argument("--extent_wkt", help="The extent of the area to query", required=False, default="")
    parser.add_argument("--bbox", help="The bounding box of the area to query", required=False, nargs=4)
    parser.add_argument("--bbox_srs", help="The CRS of the bounding box", required=False, default="")
    parser.add_argument("--preprocess_vector", help="Whether to preprocess the vector data", required=False, action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--preprocess_raster", help="Whether to preprocess the raster data", required=False, action=argparse.BooleanOptionalAction, default=True)
    preprocess_config = PreprocessConfig(parser.parse_args(), capabilities)
    print(preprocess_config)

    should_preprocess_vector = preprocess_config.should_preprocess_vector
    should_preprocess_raster = preprocess_config.should_preprocess_raster


    mf_preprocessor = MultiFilePreprocessor(preprocess_config)
    crs_preprocessor = CRSFilterPreprocessor(preprocess_config)

    will_reproject_vector = preprocess_config.vector_target_crs != crs_preprocessor.get_vector_crs() or preprocess_config.vector_filter != [] or preprocess_config.vector_simplify != 0.0
    will_reproject_raster = preprocess_config.raster_target_crs != crs_preprocessor.get_raster_crs() or preprocess_config.raster_clip or preprocess_config.raster_resolution != 1.0

    if preprocess_config.raster_singlefile and len(preprocess_config.raster_file) > 1:
        mf_preprocessor.merge_raster()


    # todo if CRS already correct


    if will_reproject_vector and (should_preprocess_vector or (preprocess_config.raster_clip and preprocess_config.vector_filter)):
        crs_preprocessor.filter_reproject_simplify_vector(log_time=crs_preprocessor.logger)

    if will_reproject_raster and should_preprocess_raster:
        crs_preprocessor.clip_reproject_resolution_raster(log_time=crs_preprocessor.logger)

    if preprocess_config.system in capabilities["raster_max_2gb"]:
        mf_preprocessor.split_raster(has_next_step=False)

    # TODO if raster -> raster needs to be converted

    if (preprocess_config.system not in capabilities["vectorize"]
            and preprocess_config.raster_source_suffix != preprocess_config.raster_target_suffix
            and should_preprocess_raster):
        file_converter = FileConverterPreprocessor(preprocess_config)
        file_converter.raster_to_geotiff()

    # todo if vector -> vector needs to be converted

    if preprocess_config.system in capabilities["vectorize"] and should_preprocess_raster:
        if preprocess_config.vectorization_type == VectorizationType.TO_POLYGONS:
            data_preprocessor = DataModelProcessor(preprocess_config)
            data_preprocessor.vectorize_polygons()
        elif preprocess_config.vectorization_type == VectorizationType.TO_POINTS:
            file_converter = FileConverterPreprocessor(preprocess_config)
            file_converter.raster_to_xyz()

            data_preprocessor = DataModelProcessor(preprocess_config)
            data_preprocessor.vectorize_points()

    # todo fi rasterize

    if preprocess_config.system in capabilities["wkt_csv"]:
        file_type_preprocessor = FileConverterPreprocessor(preprocess_config)
        file_type_preprocessor.vector_to_csv_wkt(log_time=file_type_preprocessor.logger)
        print(file_type_preprocessor.logger)

    preprocess_config.copy_to_output()
    preprocess_config.remove_intermediates()


if __name__ == "__main__":
    main()
