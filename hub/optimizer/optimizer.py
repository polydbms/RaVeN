import json
from typing import TypedDict

import pandas as pd
import pyproj
from duckdb.duckdb import DuckDBPyConnection
from shapely.set_operations import intersection

from hub.executor.sqlbased import SQLBased
from hub.benchmarkrun.tilesize import TileSize
from hub.enums.stage import Stage
from hub.enums.datatype import DataType
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.utils.datalocation import RasterLocation, VectorLocation
from hub.utils.system import System
from hub.utils.network import BasicNetworkManager
from hub.utils.check_predicate import ASTQuery


class Optimizer:
    @staticmethod
    def create_run_config(workload, rl: RasterLocation, vl: VectorLocation, db_connection: DuckDBPyConnection,
                          nm: BasicNetworkManager,
                          dry_run: bool = False
                          ) -> BenchmarkParameters:
        ast_query_current = ASTQuery(vl.get_metadata()[0],
                                     SQLBased.build_condition(workload.get('condition', {}).get('vector', {}), "vector", "and"))

        ingested_raster = rl.find_available_ingested_files(db_connection.cursor())
        ingested_vector = vl.find_available_ingested_files(db_connection.cursor())
        ingested_vector['filter_predicate'] = ingested_vector['filter_predicate'].apply(
            lambda pred: json.loads(pred)
        )
        ingested_vector['predicate_as_sql'] = ingested_vector['filter_predicate'].apply(
            lambda pred: SQLBased.build_condition(pred, "vector", "and")
        )
        ingested_vector['ast_query'] = ingested_vector['predicate_as_sql'].apply(
            lambda pred: ASTQuery(vl.get_metadata()[0], pred)
        )
        ingested_vector['is_implied'] = ingested_vector['ast_query'].apply(
            lambda aq: aq.check_implies(ast_query_current)
        )
        ingested_vector['is_equals'] = ingested_vector['ast_query'].apply(
            lambda aq: aq.check_equals(ast_query_current)
        )

        ingested_vector = ingested_vector.query('is_implied')

        preprocessed_raster = rl.find_available_preprocessed_files(db_connection.cursor())
        preprocessed_vector = vl.find_available_preprocessed_files(db_connection.cursor())
        preprocessed_vector['filter_predicate'] = preprocessed_vector['filter_predicate'].apply(
            lambda pred: json.loads(pred)
        )
        preprocessed_vector[ 'predicate_as_sql'] = preprocessed_vector['filter_predicate'].apply(
            lambda pred: SQLBased.build_condition(pred, "vector", "and")
        )
        preprocessed_vector['ast_query'] = preprocessed_vector['predicate_as_sql'].apply(
            lambda pred: ASTQuery(vl.get_metadata()[0], pred)
        )

        preprocessed_vector['is_implied'] = preprocessed_vector['ast_query'].apply(
            lambda aq: aq.check_implies(ast_query_current)
        )
        preprocessed_vector['is_equals'] = preprocessed_vector['ast_query'].apply(
            lambda aq: aq.check_equals(ast_query_current)
        )

        preprocessed_vector = preprocessed_vector.query('is_implied')

        matching_crs_ingested = pd.merge(ingested_raster, ingested_vector, how='inner', on=['crs'], suffixes=('_raster', '_vector'))

        print("Ingested Vector", ingested_vector[["name", "location", "crs"]])
        print("Ingested Raster", ingested_raster[["name", "location", "crs"]])
        print("matching crs", matching_crs_ingested[["name_vector", "name_raster", "location_vector", "location_raster"]])

        if not matching_crs_ingested.empty:
            if not dry_run:
                rl.set_ingested(True, matching_crs_ingested.iloc[0]['name_raster'], matching_crs_ingested.iloc[0]['id_raster'])
                vl.set_ingested(True, matching_crs_ingested.iloc[0]['name_vector'], matching_crs_ingested.iloc[0]['id_vector'])

                rl.should_preprocess = False
                vl.should_preprocess = False
            else:
                print("Dry run: found matching ingested files for both raster and vector with same CRS.")

            filter_vector_at_stage = Stage.PREPROCESS

            if all(ingested_vector['is_implied']): # FIXME
                filter_vector_at_stage = Stage.EXECUTION


            return BenchmarkParameters(
                system=System.POSTGIS,
                align_to_crs=DataType.RASTER,
                align_crs_at_stage=Stage.PREPROCESS,
                vector_filter_at_stage=filter_vector_at_stage,
                raster_clip=False
            )
        elif not ingested_raster.empty:
            if not dry_run:
                rl.set_ingested(True, ingested_raster.iloc[0]['location'][0])
                rl.should_preprocess = False
            else:
                print("Dry run: found matching ingested file for raster.")
        elif not ingested_vector.empty:
            if not dry_run:
                vl.set_ingested(True, ingested_vector.iloc[0]['location'][0])
                vl.should_preprocess = False
            else:
                print("Dry run: found matching ingested file for vector.")

        pixels_per_feature = rl.get_pixels() / vl.get_feature_count()
        tile_size = TileSize(-1, -1)

        raster_size = sum(f.stat().st_size for f in rl.controller_file)
        vector_size = sum(f.stat().st_size for f in vl.controller_file)

        if pixels_per_feature >= 10_000_000:
            tile_size = TileSize(1000, 1000)
        elif pixels_per_feature >= 5_000_000:
            tile_size = TileSize(800, 800)

        extent_selectivity = intersection(rl.get_extent(), vl.get_extent()).area / max(vl.get_extent().area, rl.get_extent().area)
        filter_selectivity = vl.get_selectivity(workload.get("condition", {}).get("vector", {}))


        preprocessed_raster = preprocessed_raster[~preprocessed_raster['is_converted_datatype']]
        preprocessed_vector = preprocessed_vector[~preprocessed_vector['is_converted_datatype']]

        pre_matching_crs_both = pd.merge(preprocessed_raster, preprocessed_vector, how='inner', on=['crs'], suffixes=('_raster', '_vector'))
        pre_matching_crs_raster = preprocessed_raster[preprocessed_raster['crs'] == vl.get_crs().name]
        pre_matching_crs_vector = preprocessed_vector[preprocessed_vector['crs'] == rl.get_crs().name]


        print("Preprocessed Vector", preprocessed_vector[["name", "location", "crs"]])
        print("Preprocessed Raster", preprocessed_raster[["name", "location", "crs"]])
        print("Preprocessed Merged", pre_matching_crs_both[["name_vector", "name_raster", "location_vector", "location_raster"]])
        print("Preprocessed Match Vector", pre_matching_crs_vector[["name", "location"]])
        print("Preprocessed Match Raster", pre_matching_crs_raster[["name", "location"]])

        if not dry_run:
            if not pre_matching_crs_both.empty:
                preprocessed_both = pre_matching_crs_both.iloc[0]
                rl.should_preprocess = False
                vl.should_preprocess = False
                rl.uuid = preprocessed_both['id_raster']
                vl.uuid = preprocessed_both['id_vector']
                rl.set_preprocessed(True, None, None, None)
                vl.set_preprocessed(True, None, None, None)

                rl.set_preprocessed_files(preprocessed_both['preprocessed_dir_raster'], preprocessed_both['location_raster'])
                vl.set_preprocessed_files(preprocessed_both['preprocessed_dir_vector'], preprocessed_both['location_vector'])

            elif not pre_matching_crs_raster.empty:
                preprocessed_raster_tuple = pre_matching_crs_raster.iloc[0]
                rl.should_preprocess = False
                rl.uuid = preprocessed_raster_tuple['id']
                rl.set_preprocessed(True, None, None, None)
                rl.set_preprocessed_files(preprocessed_raster_tuple['preprocessed_dir'], preprocessed_raster_tuple['location'])

            elif not pre_matching_crs_vector.empty:
                preprocessed_vector_tuple = pre_matching_crs_vector.iloc[0]
                vl.should_preprocess = False
                vl.uuid = preprocessed_vector_tuple['id']
                vl.set_preprocessed(True, None, None, None)
                vl.set_preprocessed_files(preprocessed_vector_tuple['preprocessed_dir'], preprocessed_vector_tuple['location'])

            if not preprocessed_raster.empty:
                rl.should_preprocess = False
                rl.uuid = preprocessed_raster['id'][0]
                rl.set_preprocessed(True, None, None, None)
                rl.set_preprocessed_files(preprocessed_raster.iloc[0]['preprocessed_dir'], preprocessed_raster.iloc[0]['location'])
                rl.get_metadata(from_remote=True, nm=nm)

            if not preprocessed_vector.empty:
                vl.should_preprocess = False
                vl.uuid = preprocessed_vector['id'][0]
                vl.set_preprocessed(True, None, None, None)
                vl.set_preprocessed_files(preprocessed_vector.iloc[0]['preprocessed_dir'], preprocessed_vector.iloc[0]['location'])
                vl.get_metadata(from_remote=True, nm=nm)
        else:
            if not pre_matching_crs_both.empty:
                print("Dry run: found matching preprocessed files for both raster and vector with same CRS.")
            elif not pre_matching_crs_raster.empty:
                print("Dry run: found matching preprocessed file for raster.")
            elif not pre_matching_crs_vector.empty:
                print("Dry run: found matching preprocessed file for vector.")
            elif not preprocessed_raster.empty:
                print("Dry run: found preprocessed file for raster.")
            elif not preprocessed_vector.empty:
                print("Dry run: found preprocessed file for vector.")


        rasdaman_ingested_raster = ingested_raster[ingested_raster['system'] == System.RASDAMAN.value]
        postgis_ingested_raster = ingested_raster[ingested_raster['system'] == System.POSTGIS.value]
        postgis_ingested_vector = ingested_vector[ingested_vector['system'] == System.POSTGIS.value]


        if not rasdaman_ingested_raster.empty and (vl.get_feature_count() <= 100) and (rl.get_pixels() >= 500_000_000):
            if not dry_run:
                raster_is_ingest = rasdaman_ingested_raster.iloc[0]
                rl.set_ingested(True, raster_is_ingest['location_raster'][0])

                rl.should_preprocess = False
            else:
                print("Dry run: found matching ingested file for target rasdaman")

            return BenchmarkParameters(
                system=System.RASDAMAN,
                align_to_crs=DataType.RASTER,
                align_crs_at_stage=Stage.PREPROCESS,
                vector_filter_at_stage=Stage.PREPROCESS,
                raster_clip=True,
                raster_singlefile=True,
                vector_target_crs=pyproj.CRS.from_string(raster_is_ingest['crs'])
            )

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
                align_to_crs=DataType.RASTER if rl.should_preprocess else DataType.VECTOR,
                align_crs_at_stage=Stage.PREPROCESS,
                vector_filter_at_stage=Stage.PREPROCESS,
                raster_clip=False,
                raster_tile_size=tile_size
            )

        if (extent_selectivity <= 0.05) and not postgis_ingested_raster.empty:
            if not dry_run:
                raster_is_ingested = postgis_ingested_raster.iloc[0]
                rl.set_ingested(True, raster_is_ingested['location_raster'][0])
            else:
                print("Dry run: found matching ingested file for raster with low extent selectivity.")

            return BenchmarkParameters(
                system=System.POSTGIS,
                align_to_crs=DataType.RASTER if rl.should_preprocess else DataType.VECTOR,
                align_crs_at_stage=Stage.PREPROCESS,
                vector_filter_at_stage=Stage.PREPROCESS,
                raster_clip=True,
                raster_tile_size=tile_size
            )

        if (extent_selectivity <= 0.05) and not postgis_ingested_vector.empty and raster_size <= 0.7 * 10**9:
            if not dry_run:
                vector_is_ingested = postgis_ingested_vector.iloc[0]
                vl.set_ingested(True, vector_is_ingested['location_vector'][0])
            else:
                print("Dry run: found matching ingested file for vector with low extent selectivity.")

            return BenchmarkParameters(
                system=System.POSTGIS,
                align_to_crs=DataType.VECTOR if vl.should_preprocess else DataType.RASTER,
                align_crs_at_stage=Stage.PREPROCESS,
                vector_filter_at_stage=Stage.PREPROCESS,
                raster_clip=True,
                raster_tile_size=tile_size
            )

        return BenchmarkParameters(
            system=System.BEAST,
            align_crs_at_stage=Stage.EXECUTION,
            vector_filter_at_stage=Stage.PREPROCESS if filter_selectivity <= 0.05 else Stage.EXECUTION,
            raster_clip=extent_selectivity <= 0.1,
        )


