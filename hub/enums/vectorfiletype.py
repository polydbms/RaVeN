from enum import Enum


class VectorFileType(Enum):
    SHP = ".shp"
    GEOJSON = ".geojson"
    WKT = ".wkt"
    WKB = ".wkb"

    @staticmethod
    def get_by_value(value):
        return {v.value: v for v in list(VectorFileType)}.get(f".{value.lstrip('.')}")  # removeprefix
