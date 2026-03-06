import json
from dataclasses import dataclass
from pathlib import Path
from typing import TypedDict

import duckdb
import pandas as pd
import pyproj
from duckdb.duckdb import DuckDBPyConnection
from pandas import DataFrame
from shapely import Polygon
from shapely.set_operations import intersection

from hub.enums.rasterfiletype import RasterFileType
from hub.enums.vectorfiletype import VectorFileType
from hub.executor.sqlbased import SQLBased
from hub.benchmarkrun.tilesize import TileSize
from hub.enums.stage import Stage
from hub.enums.datatype import DataType
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.utils.rasterlocation import RasterLocation
from hub.utils.vectorlocation import VectorLocation
from hub.utils.system import System
from hub.utils.network import BasicNetworkManager
from hub.utils.check_predicate import ASTQuery
from hub.utils.capabilities import Capabilities

CAPABILITIES = Capabilities.read_capabilities()

class Optimizer:
    @staticmethod
    def create_run_config(workload, rl: RasterLocation, vl: VectorLocation, db_connection: DuckDBPyConnection,
                          nm: BasicNetworkManager,
                          dry_run: bool = False
                          ) -> BenchmarkParameters:

        if False:
            return BenchmarkParameters(
                system=System.BEAST,
                align_to_crs=DataType.RASTER,
                align_crs_at_stage=Stage.PREPROCESS,
                vector_filter_at_stage=Stage.PREPROCESS,
                raster_clip=True,
            )

        ast_query_current = ASTQuery(vl.get_metadata()[0],
                                     SQLBased.build_condition(workload.get('condition', {}).get('vector', {}), "vector", "and"))

        # ingested_vector = vl.find_available_ingested_files(db_connection.cursor())
        # ingested_vector = Optimizer.add_vector_info(ingested_vector, vl, ast_query_current)

        preprocessed_vector = vl.find_available_files(db_connection.cursor(), rl.get_extent())
        preprocessed_vector = Optimizer.add_vector_info(preprocessed_vector, vl, ast_query_current)

        if preprocessed_vector.empty:
            preprocessed_vector = pd.DataFrame([{
                "id": vl.uuid,
                "system": "none",
                "name": vl.name,
                "location": None,
                "crs": vl.get_crs().name,
                "is_converted_datatype": False,
                "is_implied": False,
                "is_equals": False,
            }])


        # ingested_raster = rl.find_available_ingested_files(db_connection.cursor())
        # tiles_matching_ingested_raster = rl.find_matching_tiles(db_connection.cursor(), vl.get_extent(), ingested_raster)
        # ingested_raster = Optimizer.add_raster_tiles_info(ingested_raster, tiles_matching_ingested_raster, vl.get_extent())

        preprocessed_raster = rl.find_available_files(db_connection.cursor(), vl.get_extent())
        # tiles_matching_preprocessed_raster = rl.find_matching_tiles(db_connection.cursor(), vl.get_extent(),
        #                                                             preprocessed_raster)
        # preprocessed_raster = Optimizer.add_raster_tiles_info(preprocessed_raster, tiles_matching_preprocessed_raster, vl.get_extent())

        if preprocessed_raster.empty:
            vl_extent = vl.get_extent()
            rl_tile_extent = rl.get_extent_per_file().items()
            any_tile_covers_vector = any(e.covers(vl_extent) for _, e in rl_tile_extent)
            tiles_intersect_vector = sum(e.intersects(vl_extent) for _, e in rl_tile_extent)
            tiles_intersect_ratio = tiles_intersect_vector / len(rl.files)

            preprocessed_raster = pd.DataFrame([{
                "id": rl.uuid,
                "system": "none",
                "name": rl.name,
                "location": None,
                "crs": rl.get_crs().name,
                "is_converted_datatype": False,
                "any_tile_covers_vector_available": False,
                "any_tile_covers_vector": any_tile_covers_vector,
                "merged_available_covers_vector": False,
                "intersects_ratio_available": 0,
                "intersects_ratio": tiles_intersect_ratio,
                "needs_merge": tiles_intersect_vector > 1,
                "tile_stems": rl.get_tiles()['tile_stem'].tolist()
            }])


        # matching_crs_ingested = pd.merge(ingested_raster, ingested_vector, how='inner', on=['crs'], suffixes=('_raster', '_vector'))

        # print("OPTIMIZER: Ingested Vector", ingested_vector[["name", "location", "crs"]])
        # print("OPTIMIZER: Ingested Raster", ingested_raster[["name", "location", "crs"]])
        # print("OPTIMIZER: matching crs", matching_crs_ingested[["name_vector", "name_raster", "location_vector", "location_raster"]])
        #
        # if not matching_crs_ingested.empty:
        #     if not dry_run:
        #         rl.set_ingested(True, matching_crs_ingested.iloc[0]['name_raster'], matching_crs_ingested.iloc[0]['id_raster'])
        #         vl.set_ingested(True, matching_crs_ingested.iloc[0]['name_vector'], matching_crs_ingested.iloc[0]['id_vector'])
        #
        #         rl.should_preprocess = False
        #         vl.should_preprocess = False
        #     print("OPTIMIZER: Dry run: found matching ingested files for both raster and vector with same CRS.")
        #
        #     filter_vector_at_stage = Stage.PREPROCESS
        #
        #     if all(ingested_vector['is_implied']): # FIXME
        #         filter_vector_at_stage = Stage.EXECUTION
        #
        #
        #     return BenchmarkParameters(
        #         system=System.POSTGIS,
        #         align_to_crs=DataType.RASTER,
        #         align_crs_at_stage=Stage.PREPROCESS,
        #         vector_filter_at_stage=filter_vector_at_stage,
        #         raster_clip=False
        #     )
        # elif not ingested_raster.empty:
        #     if not dry_run:
        #         rl.set_ingested(True, ingested_raster.iloc[0]['location'][0])
        #         rl.should_preprocess = False
        #     print("OPTIMIZER: found matching ingested file for raster.")
        # elif not ingested_vector.empty:
        #     if not dry_run:
        #         vl.set_ingested(True, ingested_vector.iloc[0]['location'][0])
        #         vl.should_preprocess = False
        #     print("OPTIMIZER: found matching ingested file for vector.")

        pixels_per_feature = rl.get_pixels() / vl.get_feature_count()
        tile_size = TileSize(-1, -1)

        raster_size = sum(f.stat().st_size for f in rl.controller_file)
        vector_size = sum(f.stat().st_size for f in vl.controller_file)

        if pixels_per_feature >= 10_000_000:
            tile_size = TileSize(1000, 1000)
        elif pixels_per_feature >= 5_000_000:
            tile_size = TileSize(800, 800)
        else:
            tile_size = TileSize(600, 600)

        print("OPTIMIZER: pixels per feature:", pixels_per_feature)
        print("OPTIMIZER: selected tile size:", tile_size)

        extent_selectivity = intersection(rl.get_extent(), vl.get_extent()).area / max(vl.get_extent().area, rl.get_extent().area)
        filter_selectivity = vl.get_selectivity(workload.get("condition", {}).get("vector", {}))

        print("OPTIMIZER: extent selectivity:", extent_selectivity)
        print("OPTIMIZER: filter selectivity:", filter_selectivity)



        preprocessed_raster = preprocessed_raster[~preprocessed_raster['is_converted_datatype']]
        preprocessed_vector = preprocessed_vector[~preprocessed_vector['is_converted_datatype']]

        preprocessed_raster = pd.merge(preprocessed_raster, preprocessed_vector, on=["crs", "system"], how="left", indicator="matching_vector", suffixes=(None, "_REMOVE"))
        preprocessed_raster.drop(columns=[c for c in preprocessed_raster.columns if c.endswith("_REMOVE")], inplace=True)
        preprocessed_raster["vector_crs_match_preprocessed"] = preprocessed_raster["matching_vector"] == "both"
        preprocessed_vector = pd.merge(preprocessed_vector, preprocessed_raster, on=["crs", "system"], how="left", indicator="matching_raster", suffixes=(None, "_REMOVE"))
        preprocessed_vector.drop(columns=[c for c in preprocessed_vector.columns if c.endswith("_REMOVE")], inplace=True)
        preprocessed_vector["raster_crs_match_preprocessed"] = preprocessed_vector["matching_raster"] == "both"

        preprocessed_raster["vector_crs_match_base"] = preprocessed_raster['crs'] == vl.get_crs().name
        preprocessed_vector["raster_crs_match_base"] = preprocessed_vector['crs'] == rl.get_crs().name


        preprocessed_raster["score_base"] = preprocessed_raster.apply(Optimizer.calculate_raster_base_score, axis=1)
        preprocessed_vector["score_base"] = preprocessed_vector.apply(Optimizer.calculate_vector_base_score, axis=1)


        optimized_sytems = pd.DataFrame([
            {"target_system_name": System.RASDAMAN.value,},
            {"target_system_name": System.POSTGIS.value,},
            {"target_system_name": System.BEAST.value,},
            {"target_system_name": System.SEDONA.value,},
        ])

        optimized_raster = pd.merge(preprocessed_raster, optimized_sytems, how="cross")
        optimized_vector = pd.merge(preprocessed_vector, optimized_sytems, how="cross")

        # # sort
        # preprocessed_raster = preprocessed_raster.sort_values(by=['score'], ascending=False)
        #
        # pre_matching_crs_both = pd.merge(preprocessed_raster, preprocessed_vector, how='inner', on=['crs'], suffixes=('_raster', '_vector'))
        # pre_matching_crs_raster = preprocessed_raster[preprocessed_raster['crs'] == vl.get_crs().name]
        # pre_matching_crs_vector = preprocessed_vector[preprocessed_vector['crs'] == rl.get_crs().name]
        #

        print("OPTIMIZER: Preprocessed Vector", preprocessed_vector[["name", "location", "crs"]])
        print("OPTIMIZER: Preprocessed Raster", preprocessed_raster[["name", "location", "crs"]])
        # print("OPTIMIZER: Preprocessed Merged", pre_matching_crs_both[["name_vector", "name_raster", "location_vector", "location_raster"]])
        # print("OPTIMIZER: Preprocessed Match Vector", pre_matching_crs_vector[["name", "location"]])
        # print("OPTIMIZER: Preprocessed Match Raster", pre_matching_crs_raster[["name", "location"]])

        optimized_raster.loc[((optimized_raster['target_system_name'] == System.POSTGIS.value) & (optimized_raster['vector_crs_match_preprocessed'])), 'score_base'] += 10_000
        optimized_vector.loc[((optimized_vector['target_system_name'] == System.POSTGIS.value) & (optimized_vector['raster_crs_match_preprocessed'])), 'score_base'] += 2_000

        optimized_raster.loc[((optimized_raster['target_system_name'] == System.BEAST.value) & (optimized_raster['needs_merge'])), 'score_base'] -= 80

        if (vl.get_feature_count() <= 100) and (rl.get_pixels() >= 500_000_000):
            print("OPTIMIZER: found matching ingested file for target rasdaman")

            optimized_raster.loc[((optimized_raster['target_system_name'] == System.RASDAMAN.value) & (optimized_raster['system' == System.RASDAMAN.value])), 'score_base'] += 100_000

        if vl.get_feature_type() == "LineString":
            print("OPTIMIZER: Vector is LineString, selecting PostGIS with raster alignment.")
            # increase score for system_name == PostGIS
            optimized_vector.loc[(optimized_vector['target_system_name'] == System.POSTGIS.value), 'score_base'] += 200


        if (rl.get_pixels() >= 500_000_000) and (vl.get_feature_count() <= 70):
            print("OPTIMIZER: Large raster with few vector features, selecting Rasdaman.")
            # increase score for system_name == Rasdaman
            optimized_raster.loc[(optimized_raster['target_system_name'] == System.RASDAMAN.value), 'score_base'] += 100
            optimized_vector.loc[(optimized_vector['target_system_name'] == System.RASDAMAN.value), 'score_base'] += 100


        if (rl.get_pixels() <= 5_000_000) and (vl.get_feature_count() <= 100_000):
            print("OPTIMIZER: Small raster and vector, selecting PostGIS without raster clipping.")
            optimized_raster.loc[(optimized_raster['target_system_name'] == System.POSTGIS.value), 'score_base'] += 50
            optimized_vector.loc[(optimized_vector['target_system_name'] == System.POSTGIS.value), 'score_base'] += 50

        if extent_selectivity <= 0.05:
            print("OPTIMIZER: found matching ingested file for raster with low extent selectivity.")
            optimized_raster.loc[((optimized_raster['target_system_name'] == System.POSTGIS.value) & (optimized_raster['system'] == System.POSTGIS.value)), 'score_base'] += 50


        if (extent_selectivity <= 0.05) and raster_size <= 0.7 * 10**9:

            print("OPTIMIZER: found matching ingested file for vector with low extent selectivity.")
            optimized_vector.loc[((optimized_vector['target_system_name'] == System.POSTGIS.value) & (optimized_vector['system'] == System.POSTGIS.value)), 'score_base'] += 20




        print("OPTIMIZER: no raster selected, no vector selected. Using default Beast configuration.")
        optimized_raster.loc[(optimized_raster['target_system_name'] == System.BEAST.value), 'score_base'] += 10
        optimized_vector.loc[(optimized_vector['target_system_name'] == System.BEAST.value), 'score_base'] += 10

        optimized_raster = optimized_raster.sort_values(by=['score_base'], ascending=False)
        optimized_vector = optimized_vector.sort_values(by=['score_base'], ascending=False)

        optimized = pd.merge(optimized_raster, optimized_vector, how='outer', on=['target_system_name'], suffixes=('_raster', '_vector'))
        optimized['total_score'] = optimized['score_base_raster'].fillna(0) + optimized['score_base_vector'].fillna(0)
        optimized = optimized.sort_values(by=['total_score'], ascending=False)



        optimized_config = optimized.iloc[0]

        if optimized_config['system_raster'] is not None:
            rl.set_tiles_status(optimized_config['tile_stems_raster'], "is_preprocessed", True)

        if optimized_config['system_raster'] in [s.value for s in System]:
            rl.set_tiles_status(optimized_config['tile_stems_raster'], "is_ingested", True)

        if optimized_config['system_raster'] == 'filesystem' and 0.0 < optimized_config['intersects_ratio_available_raster'] < 1.0:
            # do partial merge
            rl.should_preprocess = True
            rl.uuid = optimized_config['id_raster']
            rl.target_suffix = RasterFileType.get_by_value(optimized_config['datatype_raster'])

            if optimized_config['is_merged_raster']:
                rl.set_merged_base(optimized_config['preprocessed_dir_raster'], optimized_config['location_raster'])

            location = optimized_config['location_raster']

            rl.set_preprocessed_files(optimized_config['preprocessed_dir_raster'], location)

        elif optimized_config['system_raster'] == 'filesystem' and optimized_config['intersects_ratio_available_raster'] == 1.0:
            location = optimized_config['location_raster']
            rl.should_preprocess = False
            rl.uuid = optimized_config['id_raster']
            rl.target_suffix = RasterFileType.get_by_value(optimized_config['datatype_raster'])
            rl.set_preprocessed(True, None, None, None)
            rl.set_preprocessed_files(optimized_config['preprocessed_dir_raster'], location)

        elif optimized_config['system_raster'] in [System.RASDAMAN.value, System.POSTGIS.value]: # case: ingested
            if optimized_config['intersects_ratio_available_raster'] < 1.0:
                pass
            else:
                rl.set_ingested(True, optimized_config['name_raster'], optimized_config['id_raster'])
            rl.should_preprocess = False


        if optimized_config['system_vector'] == 'filesystem': # case: preprocessed, not ingested
            location = optimized_config['location_vector']
            vl.should_preprocess = False
            vl.uuid = optimized_config['id_vector']
            vl.target_suffix = VectorFileType.get_by_value(optimized_config['datatype_vector'])
            vl.set_preprocessed(True, None, None, None)
            vl.set_preprocessed_files(optimized_config['preprocessed_dir_vector'], location)

        elif optimized_config['system_vector'] in [System.POSTGIS.value]: # case: ingested
            vl.set_ingested(True, optimized_config['name_vector'], optimized_config['id_vector'])
            vl.should_preprocess = False



        system = System.get_by_value(optimized_config['target_system_name'])
        align_to_crs = DataType.RASTER if rl.should_preprocess else DataType.VECTOR
        align_crs_at_stage = Stage.PREPROCESS
        vector_filter_at_stage = Stage.EXECUTION if system in [System.BEAST] and filter_selectivity > 0.05 else Stage.PREPROCESS
        raster_clip = True

        if system == System.POSTGIS and rl.get_pixels() <= 5_000_000 and vl.get_feature_count() <= 100_000:
            raster_clip = False

        # raster_singlefile = system in [System.BEAST]
        raster_tile_size = tile_size

        rl.end_init()
        vl.end_init()

        return BenchmarkParameters(
            system=system,
            align_to_crs=align_to_crs,
            align_crs_at_stage=align_crs_at_stage,
            vector_filter_at_stage=vector_filter_at_stage,
            raster_clip=raster_clip,
            # raster_singlefile=raster_singlefile,
            raster_tile_size=raster_tile_size
        )

        # if not dry_run:
        #     if not pre_matching_crs_both.empty:
        #         preprocessed_both = pre_matching_crs_both.iloc[0]
        #
        #         if preprocessed_both['needs_merge_raster']:
        #             raster_files = Optimizer.set_raster_tiles_add(preprocessed_both['id_raster'], preprocessed_both['system_raster'], preprocessed_both['location_raster'], preprocessed_both['tiles_available_raster'], rl.docker_file)
        #         else:
        #             raster_files = [preprocessed_both['location_raster']]
        #
        #         rl.should_preprocess = False
        #         vl.should_preprocess = False
        #         rl.uuid = preprocessed_both['id_raster']
        #         vl.uuid = preprocessed_both['id_vector']
        #         rl.target_suffix = RasterFileType.get_by_value(preprocessed_both['datatype_raster'])
        #         vl.target_suffix = VectorFileType.get_by_value(preprocessed_both['datatype_vector'])
        #         rl.set_preprocessed(True, None, None, None)
        #         vl.set_preprocessed(True, None, None, None)
        #
        #         rl.set_preprocessed_files(preprocessed_both['preprocessed_dir_raster'], preprocessed_both['location_raster'])
        #         vl.set_preprocessed_files(preprocessed_both['preprocessed_dir_vector'], preprocessed_both['location_vector'])
        #
        #     elif not pre_matching_crs_raster.empty:
        #
        #         # check if partial merge
        #
        #
        #         preprocessed_raster_tuple = pre_matching_crs_raster.iloc[0]
        #         rl.should_preprocess = False
        #         rl.uuid = preprocessed_raster_tuple['id']
        #         rl.target_suffix = RasterFileType.get_by_value(preprocessed_raster_tuple['datatype'])
        #         rl.set_preprocessed(True, None, None, None)
        #         rl.set_preprocessed_files(preprocessed_raster_tuple['preprocessed_dir'], preprocessed_raster_tuple['location'])
        #
        #     elif not pre_matching_crs_vector.empty:
        #         preprocessed_vector_tuple = pre_matching_crs_vector.iloc[0]
        #         vl.should_preprocess = False
        #         vl.uuid = preprocessed_vector_tuple['id']
        #         vl.target_suffix = VectorFileType.get_by_value(preprocessed_vector_tuple['datatype'])
        #         vl.set_preprocessed(True, None, None, None)
        #         vl.set_preprocessed_files(preprocessed_vector_tuple['preprocessed_dir'], preprocessed_vector_tuple['location'])
        #
        #     if not preprocessed_raster.empty:
        #
        #         # check if partial merge
        #
        #         rl.should_preprocess = False
        #         rl.uuid = preprocessed_raster['id'][0]
        #         rl.target_suffix = RasterFileType.get_by_value(preprocessed_raster['datatype'][0])
        #         rl.set_preprocessed(True, None, None, None)
        #         rl.set_preprocessed_files(preprocessed_raster.iloc[0]['preprocessed_dir'], preprocessed_raster.iloc[0]['location'])
        #         rl.get_metadata(from_remote=True, nm=nm)
        #
        #     if not preprocessed_vector.empty:
        #         vl.should_preprocess = False
        #         vl.uuid = preprocessed_vector['id'][0]
        #         vl.target_suffix = VectorFileType.get_by_value(preprocessed_vector['datatype'][0])
        #         vl.set_preprocessed(True, None, None, None)
        #         vl.set_preprocessed_files(preprocessed_vector.iloc[0]['preprocessed_dir'], preprocessed_vector.iloc[0]['location'])
        #         vl.get_metadata(from_remote=True, nm=nm)
        # else:
        #     if not pre_matching_crs_both.empty:
        #         print("Dry run: found matching preprocessed files for both raster and vector with same CRS.")
        #     elif not pre_matching_crs_raster.empty:
        #         print("Dry run: found matching preprocessed file for raster.")
        #     elif not pre_matching_crs_vector.empty:
        #         print("Dry run: found matching preprocessed file for vector.")
        #     elif not preprocessed_raster.empty:
        #         print("Dry run: found preprocessed file for raster.")
        #     elif not preprocessed_vector.empty:
        #         print("Dry run: found preprocessed file for vector.")
        #
        #
        # rasdaman_ingested_raster = ingested_raster[ingested_raster['system'] == System.RASDAMAN.value]
        # postgis_ingested_raster = ingested_raster[ingested_raster['system'] == System.POSTGIS.value]
        # postgis_ingested_vector = ingested_vector[ingested_vector['system'] == System.POSTGIS.value]
        #
        # align_to_crs = DataType.RASTER if rl.should_preprocess else DataType.VECTOR
        #
        # print("OPTIMIZER: aligning to CRS of", align_to_crs.value)
        #
        #
        # if not rasdaman_ingested_raster.empty and (vl.get_feature_count() <= 100) and (rl.get_pixels() >= 500_000_000):
        #     if not dry_run:
        #         raster_is_ingest = rasdaman_ingested_raster.iloc[0]
        #         rl.set_ingested(True, raster_is_ingest['location_raster'][0])
        #
        #         rl.should_preprocess = False
        #     print("OPTIMIZER: found matching ingested file for target rasdaman")
        #
        #     return BenchmarkParameters(
        #         system=System.RASDAMAN,
        #         align_to_crs=DataType.RASTER,
        #         align_crs_at_stage=Stage.PREPROCESS,
        #         vector_filter_at_stage=Stage.PREPROCESS,
        #         raster_clip=True,
        #         raster_singlefile=True,
        #         vector_target_crs=pyproj.CRS.from_string(raster_is_ingest['crs'])
        #     )
        #
        # if vl.get_feature_type() == "LineString":
        #     print("OPTIMIZER: Vector is LineString, selecting PostGIS with raster alignment.")
        #
        #     return BenchmarkParameters(
        #         system = System.POSTGIS,
        #         align_to_crs=DataType.RASTER,
        #         align_crs_at_stage=Stage.PREPROCESS,
        #         vector_filter_at_stage=Stage.PREPROCESS,
        #         raster_clip=True,
        #         raster_singlefile=True,
        #         raster_tile_size=tile_size
        #     )
        #
        # if (rl.get_pixels() >= 500_000_000) and (vl.get_feature_count() <= 70):
        #     print("OPTIMIZER: Large raster with few vector features, selecting Rasdaman.")
        #
        #     return BenchmarkParameters(
        #         system = System.RASDAMAN,
        #         align_to_crs=DataType.RASTER,
        #         align_crs_at_stage=Stage.PREPROCESS,
        #         vector_filter_at_stage=Stage.PREPROCESS,
        #         raster_clip=True,
        #         raster_singlefile=True
        #     )
        #
        #
        # if (rl.get_pixels() <= 5_000_000) and (vl.get_feature_count() <= 100_000):
        #     print("OPTIMIZER: Small raster and vector, selecting PostGIS without raster clipping.")
        #
        #     return BenchmarkParameters(
        #         system=System.POSTGIS,
        #         align_to_crs=align_to_crs,
        #         align_crs_at_stage=Stage.PREPROCESS,
        #         vector_filter_at_stage=Stage.PREPROCESS,
        #         raster_clip=False,
        #         raster_tile_size=tile_size
        #     )
        #
        # if (extent_selectivity <= 0.05) and not postgis_ingested_raster.empty:
        #     if not dry_run:
        #         raster_is_ingested = postgis_ingested_raster.iloc[0]
        #         rl.set_ingested(True, raster_is_ingested['location_raster'][0])
        #     print("OPTIMIZER: found matching ingested file for raster with low extent selectivity.")
        #
        #     return BenchmarkParameters(
        #         system=System.POSTGIS,
        #         align_to_crs=align_to_crs,
        #         align_crs_at_stage=Stage.PREPROCESS,
        #         vector_filter_at_stage=Stage.PREPROCESS,
        #         raster_clip=True,
        #         raster_tile_size=tile_size
        #     )
        #
        # if (extent_selectivity <= 0.05) and not postgis_ingested_vector.empty and raster_size <= 0.7 * 10**9:
        #     if not dry_run:
        #         vector_is_ingested = postgis_ingested_vector.iloc[0]
        #         vl.set_ingested(True, vector_is_ingested['location_vector'][0])
        #     print("OPTIMIZER: found matching ingested file for vector with low extent selectivity.")
        #
        #     return BenchmarkParameters(
        #         system=System.POSTGIS,
        #         align_to_crs=align_to_crs,
        #         align_crs_at_stage=Stage.PREPROCESS,
        #         vector_filter_at_stage=Stage.PREPROCESS,
        #         raster_clip=True,
        #         raster_tile_size=tile_size
        #     )
        #
        # print("OPTIMIZER: no raster selected, no vector selected. Using default Beast configuration.")
        #
        # return BenchmarkParameters(
        #     system=System.BEAST,
        #     align_crs_at_stage=Stage.PREPROCESS,
        #     align_to_crs=DataType.RASTER,
        #     vector_filter_at_stage=Stage.PREPROCESS if filter_selectivity <= 0.05 else Stage.EXECUTION,
        #     raster_clip=extent_selectivity <= 0.1,
        # )

    @staticmethod
    def add_vector_info(vector_ds: DataFrame, vl: VectorLocation, ast: ASTQuery) -> DataFrame:
        vector_ds['filter_predicate'] = vector_ds['filter_predicate'].apply(
            lambda pred: json.loads(pred)
        )
        vector_ds['predicate_as_sql'] = vector_ds['filter_predicate'].apply(
            lambda pred: SQLBased.build_condition(pred, "vector", "and")
        )
        vector_ds['ast_query'] = vector_ds['predicate_as_sql'].apply(
            lambda pred: ASTQuery(vl.get_metadata()[0], pred)
        )

        vector_ds['is_implied'] = vector_ds['ast_query'].apply(
            lambda aq: aq.check_implies(ast)
        )
        vector_ds['is_equals'] = vector_ds['ast_query'].apply(
            lambda aq: aq.check_equals(ast)
        )

        vector_ds = vector_ds.query('is_implied')
        return vector_ds



    @staticmethod
    def calculate_raster_base_score(pr):
        score = 0
        score += pr['any_tile_covers_vector_available'] * 100_000
        score += pr['any_tile_covers_vector'] * 10_000
        score += pr['merged_available_covers_vector'] * 5_000
        score += pr['intersects_ratio_available'] * 1000
        score += pr['intersects_ratio'] * 500

        score += pr['vector_crs_match_preprocessed'] * 200
        score += pr['vector_crs_match_base'] * 100

        return score

    @staticmethod
    def calculate_vector_base_score(pv):
        score = 0
        score += pv['is_equals'] * 10_000
        score += pv['is_implied'] * 5_000

        score += pv['raster_crs_match_preprocessed'] * 200
        score += pv['raster_crs_match_base'] * 100

        return score

    # @staticmethod
    # def set_raster_tiles_add(raster_id, raster_fs, raster_location, tiles_available, og_tiles_files) -> list[Path]:
    #     res = duckdb.execute("""
    #                 select tile_stem
    #                 from tiles_available ta
    #                 where ta.id = $raster_id
    #                   and ta.system = $raster_fs
    #                   and not ta.is_available
    #                   and tile_intersects_vector
    #                  """,
    #                  {
    #                         "raster_id": raster_id,
    #                         "raster_fs": raster_fs
    #                  }
    #               ).df()
    #
    #     new_tiles = [f.stem in res['tile_stem'] for f in og_tiles_files]
    #
    #     used_tiles = [raster_location]
    #     used_tiles.extend(new_tiles)
    #
    #     return used_tiles
    #

    # @staticmethod
    # def add_raster_tiles_info(ingested_raster: DataFrame, tiles_matching: DataFrame, vector_extent: Polygon) -> DataFrame:
    #     res = duckdb.execute("""
    #                         install spatial;
    #                         load spatial;
    #                          with tile_info as
    #                                   (select ir.id,
    #                                           ir.system,
    #                                           sum(tmr.tile_covers_vector::INT) >= 1 as any_tile_covers_vector,
    #                                           sum((tmr.tile_covers_vector AND is_available)::INT) >= 1 as any_tile_covers_vector_available,
    #                                           sum(tmr.tile_intersects_vector::INT)  as count_tile_intersects_vector,
    #                                           sum((tmr.tile_intersects_vector AND is_available)::INT)  as count_tile_intersects_vector_available,
    #                                           count(*)                              as total_tiles,
    #                                           count_tile_intersects_vector_available / total_tiles as intersects_ratio_available,
    #                                           count_tile_intersects_vector / total_tiles as intersects_ratio,
    #                                           length(location) < sum(is_available::INT) as is_merged,
    #                                           (not is_merged AND count_tile_intersects_vector > 1) as needs_merge,
    #                                           ST_Covers(ST_GeomFromText(ir.extent_wkt)::POLYGON_2D, ST_GeomFromText($vector_extent::VARCHAR)::POLYGON_2D) as merged_available_covers_vector,
    #                                           0 as score
    #                                    from tiles_matching tmr
    #                                             join ingested_raster ir
    #                                                  on tmr.id = ir.id
    #                                                      and tmr.system = ir.system
    #                                    group by ir.id, ir.system, ir.location, ir.extent_wkt)
    #                          select *
    #                          from tile_info
    #                                   join ingested_raster ir
    #                                        on tile_info.id = ir.id
    #                                            and tile_info.system = ir.system
    #                          """,
    #                          {
    #                                 "vector_extent": vector_extent.wkt
    #                          }
    #                       ).df()
    #
    #
    #     return res

