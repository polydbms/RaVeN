from enum import Enum
from pathlib import Path

from hub.enums.datatype import DataType
from hub.enums.stage import Stage
from hub.enums.vectorizationtype import VectorizationType
from hub.benchmarkrun.tilesize import TileSize
from hub.utils.system import System
from hub.utils.fileio import FileIO
from hub.raven import Setup

import pandas as pd

from hub.zsresultsdb.init_duckdb import InitializeDuckDB


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
    _raster: Path
    _vector: Path

    _raster_aggs: dict[str, ZSAgg]
    _group_attr: list[str]
    _vector_filter: list[str]
    _raster_filter: list[str]
    _systems: list[System]
    _join_condition: ZSJoin

    _raster_tile_size: list[TileSize]
    _raster_resolution: list[float]

    _vectorize_type: list[VectorizationType]
    _vector_resolution: list[float]

    _align_to_crs: list[DataType]
    _align_crs_at_stage: list[Stage]
    _raster_clip: list[bool]
    _vector_filter_at_stage: list[Stage]

    _iterations: int
    _warm_starts: int
    _timeout: int

    benchi: Setup

    def __init__(self, raster: Path | str, vector: Path | str):
        self._transporter = None
        self._run_cursor = None
        self._network_manager = None
        self._selected_system = None
        self._selected_run = None
        self._runs = None
        self._set_id = None

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

        self.benchi = Setup()

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

        parameters = {k: v for k, v in parameters.items() if len(v) > 0}

        workload = {'get': {'vector': self._group_attr, 'raster': [
            {'sval': {
                'aggregations': list(map(lambda a: a[1].value, self._raster_aggs.items()))}}]},
                    'join': {'table1': 'vector',
                             'table2': 'raster',
                             'condition': 'intersect(raster, vector)'},
                    'group': {'vector': self._group_attr},
                    'order': {'vector': self._group_attr},
                    'condition': {'vector': self._vector_filter}
                    }

        runs, _ = FileIO.create_configs({"raster": str(self._raster), "vector": str(self._vector)},
                                        {}, "zsbuilder.yaml", list(map(lambda s: s.value, self._systems)),
                                        [System('postgis'),
                                         System('omnisci'),
                                         System('sedona'),
                                         System('beast'),
                                         System('rasdaman')],
                                        workload
                                        , "/home/gereon/git/dima/benchi/config/controller_config.qgis.yaml",
                                        parameters)

        host_params = FileIO.get_host_params("/home/gereon/git/dima/benchi/config/controller_config.qgis.yaml")
        InitializeDuckDB(host_params.controller_db_connection, runs, "qgis.yaml")

        self._set_id = runs[0].host_params.controller_db_connection.initialize_benchmark_set("qgis.yaml", {})

        self._runs = runs


    def prepare(self):
        self._selected_run = self._runs[0]

        self._selected_system = self._selected_run.benchmark_params.system
        print(self._selected_system)

        self._network_manager, self._run_cursor, self._transporter = self.benchi.setup_host(0, self._selected_run, self._selected_run.benchmark_params.system)

    def do_preprocess(self):
        self.benchi.do_preprocess(self._network_manager, self._selected_run, self._selected_system)

    def do_ingestion(self):
        self.benchi.do_ingestion(self._network_manager, self._selected_run, self._selected_system)

    def do_execution(self):
        self.benchi.do_execution(self._network_manager, self._selected_run, self._selected_system)

    def do_teardown(self):
        self.benchi.do_teardown(self._network_manager, self._selected_run, self._selected_system)



    def do_benchmark(self):
        result = []
        for run in self._runs:
            results = self.benchi.run_tasks(run)

            result.append({"all_results": results, "results": pd.read_csv(results[0]), "parameters": run.benchmark_params,
                    "set_id": self._set_id})

