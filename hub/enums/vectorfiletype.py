from enum import Enum


class VectorFileType(Enum):
    """
    supported raster file types
    """
    SHP = ".shp"
    GEOJSON = ".geojson"
    WKT = ".wkt"
    WKB = ".wkb"

    @staticmethod
    def get_by_value(value: str):
        return {v.value: v for v in list(VectorFileType)}.get(f".{value.lower().lstrip('.')}")  # removeprefix
