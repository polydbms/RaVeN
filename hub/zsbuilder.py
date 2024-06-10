from enum import Enum
from pathlib import Path

from enums.datatype import DataType
from hub.enums.stage import Stage
from hub.enums.vectorizationtype import VectorizationType
from hub.benchmarkrun.tilesize import TileSize
from hub.utils.system import System


class ZSAgg(Enum):
    SUM = "sum"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    AVG = "avg"


class ZSJoin(Enum):
    INTERSECT = "intersect"
    BEST = "best"
    CONTAINS = "contains"


class ZSGen:
    def __init__(self, raster: Path | str, vector: Path | str):
        self._raster: Path = raster if isinstance(raster, Path) else Path(raster)
        self._vector: Path = vector if isinstance(vector, Path) else Path(vector)

        self._raster_aggs: dict[str, ZSAgg] = {}
        self._group_attr: list[str] = []
        self._vector_filter: list[str] = []
        self._raster_filter: list[str] = []
        self._systems: list[System] = []
        self._join_condition: ZSJoin = ZSJoin.INTERSECT

        self._raster_tile_size: list[TileSize] = []
        self._raster_resolution: list[float] = []

        self._vectorize_type: list[VectorizationType] = []
        self._vector_resolution: list[float] = []

        self._align_to_crs: list[DataType] = []
        self._align_crs_at_stage: list[Stage] = []
        self._raster_clip: list[bool] = []
        self._vector_filter_at_stage: list[Stage] = []

        self._iterations = 1
        self._warm_starts = 0
        self._timeout = 10000

    def group(self, group: list[str] | str):
        self._group_attr = [group] if isinstance(group, str) else group
        return self

    def summarize(self, summarize: dict[str, ZSAgg]):
        self._raster_aggs.update(summarize)
        return self

    def systems(self, systems: list[System] | System):
        self._systems = [systems] if isinstance(systems, System) else systems
        return self

    def filter(self, predicate: list[str] | str):
        filter_list = [predicate] if isinstance(predicate, str) else predicate
        self._raster_filter = list(filter(lambda f: "sval" in f.lower(), filter_list))
        self._vector_filter = list(filter(lambda f: "sval" not in f.lower(), filter_list))
        return self

    def join_condition(self, join_condition: ZSJoin):
        self._join_condition = join_condition
        return self

    def raster_tile_size(self, tile_size: list[TileSize] | TileSize):
        self._raster_tile_size = [tile_size] if isinstance(tile_size, TileSize) else tile_size
        return self

    def raster_resolution(self, resolution: list[float] | float):
        self._raster_resolution = [resolution] if isinstance(resolution, float) else resolution
        return self

    def vectorize_type(self, vectorize_type: list[VectorizationType] | VectorizationType):
        self._vectorize_type = [vectorize_type] if isinstance(vectorize_type, VectorizationType) else vectorize_type
        return self

    def vector_resolution(self, vector_resolution: list[float] | float):
        self._vector_resolution = [vector_resolution] if isinstance(vector_resolution, float) else vector_resolution
        return self

    def align_to_crs(self, crs: list[DataType] | DataType):
        self._align_to_crs = [crs] if isinstance(crs, DataType) else crs
        return self

    def align_crs_at_stage(self, stage: list[Stage] | Stage):
        self._align_crs_at_stage = [stage] if isinstance(stage, Stage) else stage
        return self

    def raster_clip(self, clip: list[bool] | bool):
        self._raster_clip = [clip] if isinstance(clip, bool) else clip
        return self

    def vector_filter_at(self, stage: list[Stage] | Stage):
        self._vector_filter_at_stage = [stage] if isinstance(stage, Stage) else stage
        return self

    def iterations(self, iterations: int):
        self._iterations = iterations
        return self

    def warm_starts(self, warm_starts: int):
        self._warm_starts = warm_starts
        return self

    def timeout(self, timeout: int):
        self._timeout = timeout
        return self

    def build(self):
        vector_fields = self._vector

        parameters = {
            "align_crs_at_stage": self._align_crs_at_stage,
            "align_to_crs": self._align_to_crs,
            "raster_clip": self._raster_clip,
            "raster_resolution": self._raster_resolution,
            "raster_tile_size": self._raster_tile_size,
            "vector_filter_at_stage": self._vector_filter_at_stage,
            "vector_resolution": self._vector_resolution,
            "vectorize_type": self._vectorize_type,
        }

        workload = {'get': {'vector': vector_fields, 'raster': [
            {'sval': {
                'aggregations': self._raster_aggs}}]},
                    'join': {'table1': 'vector',
                             'table2': 'raster',
                             'condition': 'intersect(raster, vector)'},
                    'group': {'vector': vector_fields},
                    'order': {'vector': vector_fields},
                    'condition': {'vector': self._vector_filter}
                    }

        runs, iterations = FileIO.create_configs({"raster": self._raster, "vector": self._vector},
                                                 {}, "qgis.yaml", self._systems,
                                                 [System('postgis', 25432),
                                                  System('omnisci', 6274),
                                                  System('sedona', 80),
                                                  System('beast', 80),
                                                  System('rasdaman', 8080)],
                                                 workload
                                                 , "/home/gereon/git/dima/benchi/config/controller_config.qgis.yaml",
                                                 parameters)
