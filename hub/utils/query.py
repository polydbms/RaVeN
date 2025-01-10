from functools import partial

import pyproj
from shapely.creation import box
from shapely.io import from_wkt
from shapely.ops import transform

from hub.enums.datatype import DataType
from hub.enums.stage import Stage


def extent_to_geom(extent, benchmark_params, initial_crs = None):
    if not extent:
        return None

    extenttype = list(extent.keys())[0]

    if initial_crs:
        target_crs = initial_crs
    else:
        target_crs = benchmark_params.vector_target_crs \
            if benchmark_params.align_to_crs == DataType.RASTER and benchmark_params.align_crs_at_stage == Stage.PREPROCESS \
            else benchmark_params.raster_target_crs


    project = partial(
        pyproj.transform,
        pyproj.Proj(f"epsg:{extent[extenttype]['srid']}"),
        target_crs
    )

    match extenttype:
        case "bbox":
            geom = box(extent["bbox"]["xmin"], extent["bbox"]["ymin"], extent["bbox"]["xmax"],
                       extent["bbox"]["ymax"])
        case "wkt":
            geom = from_wkt(extent["wkt"])
        case _:
            raise ValueError("Extent type not supported")

    return transform(project, geom)
