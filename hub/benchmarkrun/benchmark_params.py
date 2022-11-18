import sys

from pyproj import CRS

from hub.enums.rasterfiletype import RasterFileType
from hub.enums.vectorfiletype import VectorFileType
from hub.benchmarkrun.tilesize import TileSize
from hub.enums.vectorizationtype import VectorizationType
from hub.utils.system import System


class BenchmarkParameters:
    system: System

    raster_target_format: RasterFileType | VectorFileType
    raster_target_crs: CRS
    raster_tile_size: TileSize
    raster_depth: int  # as bit
    raster_resolution: float  # as reduction factor
    vectorize_type: VectorizationType

    vector_target_format: VectorFileType | RasterFileType
    vector_target_crs: CRS
    vector_resolution: float  # as reduction factor

    iterations: int

    def __init__(self,
                 system: System,
                 raster_target_format=None,
                 raster_target_crs=None,
                 raster_tile_size=TileSize(100, 100),
                 raster_depth=sys.maxsize,
                 raster_resolution=1.0,
                 vectorize_type=VectorizationType.TO_POLYGONS,
                 vector_target_format=None,
                 vector_target_crs=None,
                 vector_resolution=1.0,
                 iterations=1) -> None:
        self.system = system

        self.raster_target_format = raster_target_format
        self.raster_target_crs = raster_target_crs
        self.raster_tile_size = raster_tile_size
        self.raster_depth = raster_depth
        self.raster_resolution = raster_resolution
        self.vectorize_type = vectorize_type

        self.vector_target_format = vector_target_format
        self.vector_target_crs = vector_target_crs
        self.vector_resolution = vector_resolution

        self.iterations = iterations

    def __str__(self):
        return "_".join([
            self.system.name,
            self.raster_target_format.value.lstrip(".") if self.raster_target_format is not None else "",
            str(self.raster_target_crs.to_epsg()) if self.raster_target_crs is not None else "",
            self.raster_tile_size.postgis_str,
            str(self.raster_depth),
            str(self.raster_resolution).replace(".", "-"),
            self.vectorize_type.value,
            self.vector_target_format.value.lstrip(".") if self.vector_target_format is not None else "",
            str(self.vector_target_crs.to_epsg()) if self.vector_target_crs is not None else "",
            str(self.vector_resolution).replace(".", "-"),
            str(self.iterations)
        ])

    def validate(self, capabilities) -> bool:
        # if self.raster_target_format is None:
        #     raster_type_check = True
        # else:
        raster_type = VectorFileType if self.system.name in capabilities["vectorize"] else RasterFileType
        raster_type_check = isinstance(self.raster_target_format, raster_type)

        raster_tile_size_check = self.raster_tile_size.width > 0 and self.raster_tile_size.height > 0
        raster_depth_check = self.raster_depth > 0
        raster_resolution_check = 0 < self.raster_resolution <= 1

        # if self.vector_target_format is None:
        #     vector_type_check = True
        # else:
        vector_type = RasterFileType if self.system.name in capabilities["rasterize"] else VectorFileType
        vector_type_check = isinstance(self.vector_target_format, vector_type)

        vector_resolution_check = 0 < self.vector_resolution <= 1

        if raster_type_check and \
               raster_tile_size_check and \
               raster_depth_check and \
               raster_resolution_check and \
               vector_type_check and \
               vector_resolution_check:
            return True
        else:
            err_msg = f"Could not validate benchmark params {self}: "
            if not raster_type_check:
                err_msg += f"raster target format check failed: {self.raster_target_format}, "
            if not raster_tile_size_check:
                err_msg += f"raster tile size check failed: {self.raster_tile_size}, "
            if not raster_depth_check:
                err_msg += f"raster depth check failed: {self.raster_depth}, "
            if not raster_resolution_check:
                err_msg += f"raster tile size check failed: {self.raster_resolution}, "
            if not vector_type_check:
                err_msg += f"vector target format check failed: {self.vector_target_format}, "
            if not vector_resolution_check:
                err_msg += f"vector resolution check failed: {self.vector_resolution}, "

            raise Exception(err_msg.strip(" ,"))


