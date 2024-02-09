from enum import Enum


class DataType(Enum):
    """
    the class of data
    """
    RASTER = "raster"
    VECTOR = "vector"

    @staticmethod
    def get_by_value(stage):
        return {s.value: s for s in list(DataType)}.get(stage.lower())
