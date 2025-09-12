from shapely.set_operations import intersection

from hub.benchmarkrun.tilesize import TileSize
from hub.enums.stage import Stage
from hub.enums.datatype import DataType
from hub.enums.rasterfiletype import RasterFileType
from hub.enums.vectorfiletype import VectorFileType
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.utils.datalocation import RasterLocation, VectorLocation
from hub.utils.system import System


class Optimizer:
    @staticmethod
    def create_run_config(workload, rl: RasterLocation, vl: VectorLocation) -> BenchmarkParameters:
        pixels_per_feature = rl.get_pixels() / vl.get_feature_count()
        tile_size = TileSize(-1, -1)

        if pixels_per_feature >= 10_000_000:
            tile_size = TileSize(1000, 1000)
        elif pixels_per_feature >= 5_000_000:
            tile_size = TileSize(800, 800)

        extent_selectivity = intersection(rl.get_extent(), vl.get_extent()).area / max(vl.get_extent().area, rl.get_extent().area)
        filter_selectivity = vl.get_selectivity(workload.get("condition", {}).get("vector", {}))



        if (rl.get_pixels() >= 500_000_000) and (vl.get_feature_count() <= 70):
            return BenchmarkParameters(
                system = System.RASDAMAN,
                align_to_crs=DataType.RASTER,
                align_crs_at_stage=Stage.PREPROCESS,
                vector_filter_at_stage=Stage.PREPROCESS,
                raster_clip=True,
                raster_singlefile=True
            )


        if (rl.get_pixels() <= 5_000_000) and (vl.get_feature_count() <= 100_000):
            return BenchmarkParameters(
                system=System.POSTGIS,
                align_to_crs=DataType.RASTER,
                align_crs_at_stage=Stage.PREPROCESS,
                vector_filter_at_stage=Stage.PREPROCESS,
                raster_clip=False,
                raster_tile_size=tile_size
            )

        # if (extent_selectivity <= 0.05): FIXME not relevant until adaptive optimiation
        #     return BenchmarkParameters(
        #         system=System.POSTGIS,
        #         align_to_crs=DataType.RASTER,
        #         align_crs_at_stage=Stage.PREPROCESS,
        #         vector_filter_at_stage=Stage.PREPROCESS,
        #         raster_clip=True,
        #         raster_tile_size=tile_size
        #     )

        return BenchmarkParameters(
            system=System.BEAST,
            align_crs_at_stage=Stage.EXECUTION,
            vector_filter_at_stage=Stage.PREPROCESS if filter_selectivity <= 0.05 else Stage.EXECUTION,
            raster_clip=extent_selectivity <= 0.1,
        )


