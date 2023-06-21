from enum import Enum


class RasterFileType(Enum):
    """
    supported raster file types
    """
    TIFF = ".tiff"
    JP2 = ".jp2"

    @staticmethod
    def get_by_value(value):
        return {r.value: r for r in list(RasterFileType)}.get(f".{value.lstrip('.')}")  # removeprefix
