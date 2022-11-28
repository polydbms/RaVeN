from enum import Enum


class VectorizationType(Enum):
    TO_POINTS = "points"
    TO_POLYGONS = "polygons"

    @staticmethod
    def get_by_value(vec_type):
        return {v.value: v for v in list(VectorizationType)}.get(vec_type)
