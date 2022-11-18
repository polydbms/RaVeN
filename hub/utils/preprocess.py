import argparse
import random
import re
import shutil
import string
import subprocess
from datetime import time
from pathlib import Path

import pandas as pd
import geopandas as gpd
import rioxarray as rxr
import rasterio.crs
from osgeo import ogr, gdal
from osgeo_utils import gdal_polygonize
from pyproj import CRS

from hub.enums.vectorfiletype import VectorFileType
from hub.enums.vectorizationtype import VectorizationType
from hub.evaluation.measure_time import measure_time
from hub.utils.capabilities import Capabilities


class PreprocessConfig:
    system: str

    _vector_folder: Path
    _vector_file: str
    vector_name: str
    vector_target_suffix: str
    vector_output_folder: Path
    vector_target_crs: CRS
    vectorization_type: VectorizationType

    _raster_folder: Path
    _raster_file: str
    raster_name: str
    raster_target_suffix: str
    raster_output_folder: Path
    raster_target_crs: CRS

    # intermediate_folders: list[Path]  # FIXME this should be done properly

    def __init__(self, args):
        self.system = args.system

        self._vector_folder = Path(args.vector_path)
        self._vector_file = self._find_file(self._vector_folder, ["*.shp"]).parts[-1]
        self.vector_name = self._vector_folder.name
        self.vector_target_suffix = args.vector_target_suffix
        self.vector_output_folder = Path(args.vector_output_folder)
        self.vector_target_crs = CRS.from_epsg(args.vector_target_crs)
        self.vectorization_type = VectorizationType.get_by_value(args.vectorization_type)

        self._raster_folder = Path(args.raster_path)
        self._raster_file = self._find_file(self._raster_folder, ["*.tif", "*.tiff", "*.jp2"]).parts[-1]
        self.raster_name = self._raster_folder.name
        self.raster_target_suffix = args.raster_target_suffix
        self.raster_output_folder = Path(args.raster_output_folder)
        self.raster_target_crs = CRS.from_epsg(args.raster_target_crs)

        self.intermediate_folders = []

    def set_vector_folder(self, folder: Path):
        self._vector_folder = folder

    def set_vector_suffix(self, suffix: str):
        self._vector_file = self.vector_file_path.with_suffix(suffix).parts[-1]

    @property
    def vector_folder(self) -> Path:
        return self._vector_folder

    @property
    def vector_file(self) -> str:
        return self._vector_file

    @property
    def vector_file_path(self) -> Path:
        return self._vector_folder.joinpath(self._vector_file)

    def set_raster_folder(self, folder: Path):
        self._raster_folder = folder

    def set_raster_suffix(self, suffix: str):
        self._raster_file = self.raster_file_path.with_suffix(suffix).parts[-1]

    @property
    def raster_folder(self) -> Path:
        return self._raster_folder

    @property
    def raster_file(self) -> str:
        return self._raster_file

    @property
    def raster_file_path(self) -> Path:
        return self._raster_folder.joinpath(self._raster_file)

    def remove_intermediates(self):
        for folder in self.intermediate_folders:
            shutil.rmtree(folder)

    def copy_to_output(self):
        shutil.rmtree(self.vector_output_folder, ignore_errors=True)
        self.vector_output_folder.mkdir(parents=True, exist_ok=True)
        for f in self.vector_folder.iterdir():
            if f.is_file():
                shutil.copy(f, self.vector_output_folder)
        # shutil.copytree(self.vector_folder, self.vector_output_folder, dirs_exist_ok=True)

        shutil.rmtree(self.raster_output_folder, ignore_errors=True)
        self.raster_output_folder.mkdir(parents=True, exist_ok=True)
        for f in self.raster_folder.iterdir():
            if f.is_file():
                shutil.copy(f, self.raster_output_folder)
        # shutil.copytree(self.raster_folder, self.raster_output_folder, dirs_exist_ok=True)

    @staticmethod
    def get_random_str(length):
        return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

    @staticmethod
    def _find_file(folder: Path, ending: [str]) -> Path:
        for e in ending:
            files = [f for f in folder.glob(e)]
            if len(files) > 0:
                return Path(files[0].name)

    def __str__(self):
        return ", ".join([
            str(self.system),
            str(self._vector_folder),
            str(self._vector_file),
            str(self.vector_name),
            str(self.vector_target_suffix),
            str(self.vector_output_folder),
            str(self.vector_target_crs),
            str(self.vectorization_type),
            str(self._raster_folder),
            str(self._raster_file),
            str(self.raster_name),
            str(self.raster_target_suffix),
            str(self.raster_output_folder),
            str(self.raster_target_crs)
        ])


class Preprocessor:
    def __init__(self, config: PreprocessConfig, base_tmp_folder: Path) -> None:
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

    def get_vector(self):
        vector = gpd.read_file(self.config.vector_file_path)
        return vector

    def get_vector_crs(self):
        return self.get_vector().crs

    def get_raster(self):
        return rxr.open_rasterio(self.config.raster_file_path, masked=True).squeeze()

    def get_raster_crs(self):
        return self.get_raster().rio.crs

    def update_vector_folder(self):
        self.config.set_vector_folder(self._vector_tmp_out_folder)

    def update_vector_suffix(self):
        self.config.set_vector_suffix(self.config.vector_target_suffix)

    def update_raster_folder(self):
        self.config.set_raster_folder(self._raster_tmp_out_folder)

    def update_raster_suffix(self):
        self.config.set_raster_suffix(self.config.raster_target_suffix)


class CRSPreprocessor(Preprocessor):
    def __init__(self, config: PreprocessConfig) -> None:
        super().__init__(config, Path(f"/data/reproject_tmp"))

    @measure_time
    def reproject_raster(self, **kwargs):
        print(f"reprojecting raster file {self.config.raster_file}")

        rio_target_crs = rasterio.crs.CRS.from_user_input(self.config.raster_target_crs)
        raster = self.get_raster()
        out = raster.rio.reproject(rio_target_crs)
        out_file = str(self._raster_tmp_out_folder.joinpath(self.config.raster_file))
        try:
            out.rio.to_raster(out_file)
        except OverflowError:
            print("Unable to parse nodata value correctly. Setting to 0")
            out.rio.write_nodata(0, inplace=True)
            out.rio.to_raster(out_file)

        self.update_raster_folder()

        print(f"Transfered {self.config.raster_file_path} CRS from {raster.rio.crs} to {out.rio.crs}")

    @measure_time
    def reproject_vector(self, **kwargs):
        print(f"reprojecting vector file {self.config.vector_file}")

        vector = self.get_vector()
        out = vector.to_crs(self.config.vector_target_crs)
        out.to_file(self._vector_tmp_out_folder)

        self.update_vector_folder()

        print(f"Transfered {self.config.vector_file_path} CRS from {vector.crs} to {out.crs}")


class FileConverterPreprocessor(Preprocessor):
    def __init__(self, config: PreprocessConfig) -> None:
        super().__init__(config, Path(f"/data/translate_tmp"))

    @measure_time
    def raster_to_xyz(self):
        print(f"translating raster file {self.config.raster_file} to xyz")

        output_file = self._raster_tmp_out_folder \
            .joinpath(self.config.raster_file).with_suffix(".xyz")

        subprocess.call(f"gdal_translate -of XYZ {self.config.raster_file_path} {output_file}", shell=True)

        print(f"translated raster file to {output_file}")

        self.config.set_raster_suffix(".xyz")
        self.update_raster_folder()


class FileTypeProcessor(Preprocessor):
    def __init__(self, config: PreprocessConfig) -> None:
        super().__init__(config, Path(f"/data/convert_tmp"))

    @measure_time
    def shape_to_wkt(self, system, **kwargs):
        print(f"converting file {self.config.vector_file} from shp to wkt")

        vector = self.get_vector()
        # invert lat with long
        wkt = [
            re.sub("([-]?\d*[.]\d*) ([-]?\d*[.]\d*)", r"\2 \1", geom.wkt)
            for geom in vector.geometry
        ]
        vector["wkt"] = wkt
        # wkt_out = list(self.config.vector_file_path.parts)
        # wkt_out.insert(-1, f"preprocessed_{system}")
        output = self._vector_tmp_out_folder.joinpath(self.config.vector_name).with_suffix(".json")
        json_out = vector.to_json()
        with open(output, "w") as f:
            f.write(json_out)

    # @measure_time
    # def read_wkt(self, **kwargs):
    #     output = f"/data/{self.vector_path.stem}.json"  # TODO maybe change
    #     wkt = Path(output).read_bytes()
    #     return json.loads(wkt)


class DataModelProcessor(Preprocessor):
    def __init__(self, config: PreprocessConfig, output=None) -> None:
        super().__init__(config, Path(f"/data/transform_tmp"))

    @measure_time
    def vectorize_polygons(self):
        print(f"vectorizing raster file {self.config.raster_file} with polygons")

        output_file = self._raster_tmp_out_folder \
            .joinpath(self.config.raster_file).with_suffix(self.config.raster_target_suffix)

        gdal_polygonize.gdal_polygonize(src_filename=str(self.config.raster_file_path),
                                        dst_filename=str(output_file),
                                        driver_name=self.get_driver_name(output_file),
                                        dst_fieldname="values")

        print(f"done vectorizing with polygons, saved output to {output_file}")

        self.update_raster_folder()
        self.update_raster_suffix()

    def vectorize_points(self):
        print(f"vectorizing raster file {self.config.raster_file} with points")

        output_file = self._raster_tmp_out_folder \
            .joinpath(self.config.raster_file).with_suffix(self.config.raster_target_suffix)

        points = pd.read_csv(self.config.raster_file_path, header=None, names=["x", "y", "values"], sep=" ")
        geo_points = gpd.GeoDataFrame({
            "geometry": gpd.points_from_xy(points["x"], points["y"], crs=self.config.raster_target_crs),
            "values": points["values"]
        })

        geo_points.to_file(str(output_file), driver=self.get_driver_name(output_file))

        print(f"done vectorizing with polygons, saved output to {output_file}")

        self.update_raster_folder()
        self.update_raster_suffix()

    def get_driver_name(self, output_file):
        raster_target_suffix = VectorFileType.get_by_value(self.config.raster_target_suffix)

        if raster_target_suffix == VectorFileType.SHP:
            with open(output_file.with_suffix(".prj"), 'w') as f:
                f.write(self.config.raster_target_crs.to_proj4())

            return "ESRI Shapefile"

        elif raster_target_suffix == VectorFileType.GEOJSON:
            return "GeoJSON"
        else:
            raise Exception(f"Invalid Raster Target Suffix provided: {self.config.raster_target_suffix}")

    @measure_time
    def rasterize(
            self, file: str, pixel_size=10, nod_data=0, options=None, filters=None, **kwargs
    ):
        print(f"rasterizing vector file {self.config.vector_file}")
        output_file = self.config.vector_file_path.with_suffix(self.config.vector_target_suffix)
        # output = f"{self.output}/{self.vector_path.stem}.tif"
        open_shp = ogr.Open(file)
        shp_layer = open_shp.GetLayer()
        source_srs = shp_layer.GetSpatialRef()
        for filter in filters:
            shp_layer.SetAttributeFilter(filter)
        x_min, x_max, y_min, y_max = shp_layer.GetExtent()
        # calculate raster resolution
        x_res = int((x_max - x_min) / pixel_size)
        y_res = int((y_max - y_min) / pixel_size)
        # set the image type for export
        driver = gdal.GetDriverByName("GTiff")
        new_raster = driver.Create(str(output_file), x_res, y_res, 1, gdal.GDT_Float32)
        new_raster.SetGeoTransform((x_min, pixel_size, 0, y_max, 0, -pixel_size))
        new_raster.SetProjection(source_srs.ExportToWkt())
        # get the raster band we want to export too
        raster_band = new_raster.GetRasterBand(1)
        # assign the no data value to empty cells
        raster_band.SetNoDataValue(nod_data)
        # run vector to raster on new raster with input Shapefile
        gdal.RasterizeLayer(new_raster, [1], shp_layer, options=options)

        self.update_vector_folder()
        self.update_vector_suffix()


def main():
    capabilities = Capabilities.read_capabilities()
    parser = argparse.ArgumentParser()
    parser.add_argument("--vector_path", help="Specify the path to vector dataset", required=True)
    parser.add_argument("--vector_target_suffix", help="target file suffix of the vector files", required=True)
    parser.add_argument("--vector_output_folder", help="output folder of the vector files", required=True)
    parser.add_argument("--vector_target_crs", help="target CRS for the vector file", required=True)
    parser.add_argument("--vectorization_type", help="vectorization type to be applied", required=True)
    parser.add_argument("--raster_path", help="Specify the path to raster dataset", required=True)
    parser.add_argument("--raster_target_suffix", help="target file suffix of the raster files", required=True)
    parser.add_argument("--raster_output_folder", help="output folder of the raster files", required=True)
    parser.add_argument("--raster_target_crs", help="target CRS for the raster file", required=True)
    parser.add_argument("--system", help="Specify which system should be benchmarked")
    preprocess_config = PreprocessConfig(parser.parse_args())
    print(preprocess_config)

    # todo if CRS already correct

    crs_preprocessor = CRSPreprocessor(preprocess_config)
    print(f"benchi_marker,{time()},start,preprocess,{preprocess_config.system},raster,reproject")
    crs_preprocessor.reproject_raster(log_time=crs_preprocessor.logger)
    print(f"benchi_marker,{time()},end,preprocess,{preprocess_config.system},raster,reproject")
    print(f"benchi_marker,{time()},start,preprocess,{preprocess_config.system},vector,reproject")
    crs_preprocessor.reproject_vector(log_time=crs_preprocessor.logger)
    print(f"benchi_marker,{time()},end,preprocess,{preprocess_config.system},vector,reproject")

    # TODO if raster -> raster needs to be converted

    # todo if vector -> vector needs to be converted

    if preprocess_config.system in capabilities["vectorize"]:
        if preprocess_config.vectorization_type == VectorizationType.TO_POLYGONS:
            data_preprocessor = DataModelProcessor(preprocess_config)
            print(f"benchi_marker,{time()},start,preprocess,{preprocess_config.system},raster,vectorize")
            data_preprocessor.vectorize_polygons()
            print(f"benchi_marker,{time()},end,preprocess,{preprocess_config.system},raster,vectorize")
        elif preprocess_config.vectorization_type == VectorizationType.TO_POINTS:
            file_converter = FileConverterPreprocessor(preprocess_config)
            print(f"benchi_marker,{time()},start,preprocess,{preprocess_config.system},raster,transform")
            file_converter.raster_to_xyz()
            print(f"benchi_marker,{time()},end,preprocess,{preprocess_config.system},raster,transform")

            data_preprocessor = DataModelProcessor(preprocess_config)
            print(f"benchi_marker,{time()},start,preprocess,{preprocess_config.system},raster,vectorize")
            data_preprocessor.vectorize_points()
            print(f"benchi_marker,{time()},end,preprocess,{preprocess_config.system},raster,vectorize")

    # todo fi rasterize

    if preprocess_config.system in capabilities["rasterize"]:
        file_type_preprocessor = FileTypeProcessor(preprocess_config)
        print(f"benchi_marker,{time()},start,preprocess,{preprocess_config.system},vector,rasterize")
        file_type_preprocessor.shape_to_wkt(log_time=file_type_preprocessor.logger)
        print(f"benchi_marker,{time()},end,preprocess,{preprocess_config.system},vector,rasterize")
        print(file_type_preprocessor.logger)

    preprocess_config.copy_to_output()
    preprocess_config.remove_intermediates()


if __name__ == "__main__":
    main()
