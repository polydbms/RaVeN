import json
import subprocess
import uuid
from pathlib import Path

import pandas as pd
import pyproj
import shapely
from duckdb.duckdb import DuckDBPyConnection
from pandas import DataFrame
from pyproj import CRS
from shapely import box, MultiPolygon
from shapely.geometry.polygon import Polygon

from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.benchmarkrun.host_params import HostParameters
from hub.enums.datatype import DataType
from hub.enums.rasterfiletype import RasterFileType
from hub.enums.stage import Stage
from hub.enums.vectorfiletype import VectorFileType
from hub.utils.datalocation import DataLocation
from hub.utils.network import BasicNetworkManager
from hub.utils.system import System


class RasterLocation(DataLocation):
    """
    class that wraps a raster dataset. returns necessary file names depending on the requesting entity
    """

    _allowed_endings = ["*.tif", "*.tiff", "*.geotiff", "*.jp2"]

    def __init__(self,
                 path_str: str,
                 host_params: HostParameters,
                 name: str = None,
                 uuid_name: bool = False
                 ) -> None:
        """
        initiializes the data lcoation. if the proveided path is a folder or zip archive, it tries to find the first
        available file. also prepares paths for the host and docker container based on the file
        :param path_str: the path to the dataset
        :param host_params: the hsot parameters
        :param name: the name of the dataset
        """

        super().__init__(path_str, DataType.RASTER, host_params, name, uuid_name)

        self._suffix = RasterFileType.get_by_value(self._files[0].suffix)
        self._is_clipped = False
        self._tiles = pd.DataFrame([
            {
                "tile_stem": Path(t).stem,
                "extent": e,
                "base_id": self.uuid,
                "is_relevant": False,
                "is_preprocessed": False,
                "is_ingested": False,
                "is_merged": False
            } for t, e in self.get_extent_per_file().items()
        ])

        # existing merged file
        self._merged_base = None

        # merged file to be used after preprocess
        self._merged_name = None

    @DataLocation.files.getter
    def files(self):
        if self._should_preprocess and not self._preprocessed and not self._while_init:
            ret_files = [self._merged_base] if self._merged_base else []
            ret_files += list(self._tiles[self._tiles["is_relevant"] & (~self._tiles["is_preprocessed"])]["tile_stem"].apply(lambda s: Path(s).with_suffix(self._suffix.value)))

            return ret_files

        else:
            return self._files

    @property
    def is_clipped(self) -> bool:
        return self._is_clipped

    @is_clipped.setter
    def is_clipped(self, val: bool):
        self._is_clipped = val

    @property
    def merged_name(self) -> str | None:
        return self._merged_name

    def set_merged_base(self, merged_preprocess_dir: str, merged_base: str):
        self._merged_base = Path(merged_preprocess_dir).joinpath(merged_base)

    @property
    def merged_name_suffix(self) -> str | None:
        return self._merged_name + self._target_suffix.value if self._merged_name else None

    def prepare_merged_name(self):
        self._merged_name = self.dataset_name + "_merged_" + str(uuid.uuid4())

    def set_tiles_status(self, tiles: list[str], status: str, value: bool):
        if status not in {"is_relevant", "is_preprocessed", "is_ingested", "is_merged"}:
            raise ValueError(f"Invalid status: {status}")

        self._tiles.loc[self._tiles["tile_stem"].isin(tiles), status] = value

    def get_relevant_status(self, status: str) -> DataFrame:
        if status not in {"is_relevant", "is_preprocessed", "is_ingested", "is_merged"}:
            raise ValueError(f"Invalid status: {status}")

        return self._tiles[self._tiles["is_relevant"]]["tile_stem", "status"]

    def get_tiles(self) -> DataFrame:
        return self._tiles

    def docker_file_not_ingested(self) -> list[Path]:
        return [self.docker_dir.joinpath(f"{tile_stem}{self._target_suffix.value}") for tile_stem in self._tiles[self._tiles["is_relevant"] & ~self._tiles["is_ingested"]]["tile_stem"]]

    # @property
    # def relevant_tiles(self):
    #     return self._relevant_tiles

    def get_dimensions(self) -> dict[str, tuple[int, int]]:
        """
        return the dimensions of the dataset per file
        :return:
        """
        return {str(file): (m["size"][0], m["size"][1]) for file, m in zip(self.files, self._metadata)}

    def get_pixels(self) -> int:
        return sum(t[0] * t[1] for t in self.get_dimensions().values())

    def get_approx_pixels_covered(self) -> int:
        total_extent = self.get_extent(use_extent_crs=False).bounds
        total_width = abs(total_extent[2] - total_extent[0])
        total_height = abs(total_extent[3] - total_extent[1])

        tile_bounds = [extent.bounds for extent in self.get_extent_per_file().items()]
        avg_tile_width = abs(sum(t[2] - t[0] for t in tile_bounds) / len(tile_bounds))
        avg_tile_height = abs(sum(t[3] - t[1] for t in tile_bounds) / len(tile_bounds))

        width_in_tiles = total_width / avg_tile_width
        height_in_tiles = total_height / avg_tile_height

        avg_pixels_x = sum(dim[0] for dim in self.get_dimensions().values()) / len(self.get_dimensions())
        avg_pixels_y = sum(dim[1] for dim in self.get_dimensions().values()) / len(self.get_dimensions())

        return int(width_in_tiles * height_in_tiles * avg_pixels_x * avg_pixels_y)

    def get_metadata(self, from_remote: bool = False, nm: BasicNetworkManager = None) -> list[dict]:
        """
        return metadata of the dataset
        :return:
        """

        if from_remote and nm is not None:
            self._metadata = self.get_remote_metadata(nm)
            return self._metadata

        def fetch_metadata(file) -> dict:
            return json.loads(
                subprocess.check_output(f'gdalinfo -json {file}', shell=True)
                .decode('utf-8'))

        self._metadata = [fetch_metadata(f) for f in self.controller_file]
        return self._metadata

    def get_extent_per_file(self, use_extent_crs: bool = True) -> dict[str, Polygon]:
        """
        return the extent of the dataset per file
        :return:
        """

        def bbox_from_meta(metadata):
            extent = metadata["cornerCoordinates"]
            tile_crs = pyproj.CRS.from_epsg(metadata["stac"]["proj:epsg"])
            bbox = box(
                xmin=float(extent["lowerLeft"][0]),
                ymin=float(extent["lowerLeft"][1]),
                xmax=float(extent["upperRight"][0]),
                ymax=float(extent["upperRight"][1])
            )

            if use_extent_crs:
                return self.transform_extent(bbox, tile_crs)
            else:
                return bbox

        return {str(file): bbox_from_meta(meta) for file, meta in zip(self.files, self._metadata)}

    def get_extent(self, use_extent_crs: bool = True) -> Polygon | MultiPolygon:
        """
        return the extent of the dataset
        :return:
        """

        return shapely.unary_union(list(self.get_extent_per_file(use_extent_crs=use_extent_crs).values())).normalize()

    def get_remote_metadata(self, nm: BasicNetworkManager) -> list[dict]:
        docker_command = f"docker run --rm -v {self._host_base}:/data {self.GDAL_DOCKER_IMAGE} gdalinfo -json"

        def fetch_metadata(file) -> dict:
            print("getting remote metadata for file:", file)
            result_raw = nm.run_ssh_return_result(f"{docker_command} {file}")
            return json.loads(result_raw)

        return [fetch_metadata(f) for f in self.docker_file]

    def get_remote_extent_per_file(self, nm: BasicNetworkManager, use_extent_crs: bool = True) -> dict[Path, Polygon]:
        def bbox_from_meta(metadata):
            extent = metadata["cornerCoordinates"]
            tile_crs = pyproj.CRS.from_epsg(metadata["stac"]["proj:epsg"])
            bbox = box(
                xmin=float(extent["lowerLeft"][0]),
                ymin=float(extent["lowerLeft"][1]),
                xmax=float(extent["upperRight"][0]),
                ymax=float(extent["upperRight"][1])
            )

            if use_extent_crs:
                return self.transform_extent(bbox, tile_crs)
            else:
                return bbox

        remote_metadata = self.get_remote_metadata(nm)
        return {Path(file.name): bbox_from_meta(meta) for file, meta in zip(self.files, remote_metadata)}

    def get_remote_extent(self, nm: BasicNetworkManager, use_extent_crs: bool = True) -> Polygon | MultiPolygon:
        return shapely.unary_union(
            list(self.get_remote_extent_per_file(nm, use_extent_crs=use_extent_crs).values())).normalize()

    def get_remote_crs(self, nm: BasicNetworkManager, idx: int = 0) -> CRS:

        crs = self.get_remote_metadata(nm)[idx]["stac"]["proj:epsg"]

        return pyproj.CRS.from_epsg(crs)

    def get_crs(self, idx: int = 0) -> CRS:
        """
        return the CRS of the dataset
        """
        crs = self._metadata[idx]["stac"]["proj:epsg"]

        return pyproj.CRS.from_epsg(crs)

    def _fix_files(self):
        """
        fix the file endings of the dataset
        :return:
        """
        for file in self._files:
            if file.suffix in {".tif", ".tiff", ".geotiff"}.difference({str(RasterFileType.TIFF.value)}):
                new_f = file.with_suffix(str(RasterFileType.TIFF.value))
                file.rename(new_f)

    def adjust_target_files(self, benchmark_params: BenchmarkParameters):
        """
        adjust the target files based on the benchmark parameters
        :param benchmark_params: the benchmark parameters
        :return:
        """
        super().adjust_target_files(benchmark_params)

        if self._benchmark_params.raster_target_format is None:
            self._target_suffix = self._suffix
        else:
            self._target_suffix = self._benchmark_params.raster_target_format

    def impose_limitations(self, benchmark_params: BenchmarkParameters):
        pass
        # if len(self._files) > 1 and benchmark_params.raster_clip:
        #     raise ValueError(f"Multiple raster files not supported for clipping: {self._files}")

    def register_file(self, db_connection: DuckDBPyConnection, params: BenchmarkParameters, workload: dict,
                      extent: Polygon, from_preprocess: bool = False, optimizer_run: bool = False, network_manager: BasicNetworkManager = None):
        if params.align_crs_at_stage == Stage.EXECUTION:
            crs = params.raster_target_crs if params.align_to_crs == DataType.RASTER else params.vector_target_crs
        else:
            crs = params.raster_target_crs

        locations = [str(f) for f in self.files]
        preprocessed_dir = str(self._preprocessed_dir)
        if params.system == System.POSTGIS and not from_preprocess:
            system = System.POSTGIS
            locations = [self.name]
            preprocessed_dir = ""

        elif params.system == System.RASDAMAN and not from_preprocess:
            system = System.RASDAMAN
            locations = [self.name]
            preprocessed_dir = ""
        else:
            system = "filesystem"

        is_converted_datatype = isinstance(self._target_suffix, VectorFileType)

        with db_connection as conn:
            conn.execute("load spatial;")
            dataset = conn.execute(
                """insert into available_files (id, name, preprocessed_dir, filetype, crs, datatype,
                                                raster_resolution, raster_depth, raster_tile_size, system,
                                                is_converted_datatype, is_clipped)
                   values ($uuid,
                           $name,
                           $preprocessed_dir,
                           $filetype,
                           $crs,
                           $datatype,
                           $raster_resolution,
                           $raster_depth,
                           $raster_tile_size,
                           $system,
                           $is_converted_datatype,
                           $is_clipped) returning id""",
                {"uuid": self._uuid,
                 "name": self.dataset_name,
                 "preprocessed_dir": preprocessed_dir,
                 "filetype": "raster",
                 "crs": crs.name,
                 "datatype": self.target_suffix.name,
                 "raster_resolution": params.raster_resolution,
                 "raster_depth": params.raster_depth,
                 "raster_tile_size": params.raster_tile_size.__dict__,
                 "system": str(system),
                 "is_converted_datatype": is_converted_datatype,
                 "is_clipped": self.is_clipped
                 }
            ).fetchone()

            print(f"registered file: {dataset}")


            self._uuid = dataset[0]
            self._should_preprocess = True
            self._is_ingested = False

            super().register_file(db_connection, params, workload, extent, from_preprocess, optimizer_run, network_manager)

            self.register_available_tiles("filesystem", db_connection, network_manager)

    def check_file_is_merged(self, raster_singlefile: bool):
        if raster_singlefile and len(self.files) > 1:
            self._files = [Path(self.merged_name_suffix)]

    def find_available_ingested_files(self,
                                      db_connection: DuckDBPyConnection,
                                      ) -> DataFrame:
        """
        find available files in the database that match the current dataset
        :param db_connection: the database connection
        :return: a list of available files
        """
        with db_connection as conn:
            results = conn.execute(
                """
                   select *,
                          ST_AsText(extent) as extent_wkt,
                   from available_files
                       where name = $name
                         and filetype = 'raster'
                         and system <> 'filesystem'
                         and ST_Contains(extent, ST_GeomFromText($extent::VARCHAR)::POLYGON_2D)
                """,
                {
                    "name": self.dataset_name,
                    "extent": self.get_extent().wkt
                }).df()

            return results

    def find_matching_tiles(self,
                            db_connection: DuckDBPyConnection,
                            vector_extent: Polygon,
                            relevant_raster: DataFrame
                            ) -> DataFrame:
        with db_connection as conn:
            results = conn.execute(
                """select rr.id,
                          rr.system,
                          tr.name,
                          tr.tile_stem,
                          ST_AsText(tr.extent)                                                           as tile_extent_wkt,
                          -- (tia.id_available IS NOT NULL)                                                 as is_available,
                          ST_Intersects(tr.extent, ST_GeomFromText($vector_extent::VARCHAR)::POLYGON_2D) as tile_intersects_vector,
                          ST_Covers(tr.extent, ST_GeomFromText($vector_extent::VARCHAR)::POLYGON_2D)     as tile_covers_vector
                   from tiles_raster tr
                            join relevant_raster rr
                                 on tr.name = rr.name
                            -- left outer join tile_in_available tia
                            --                 on rr.id = tia.id_available
                            --                     and rr.system = tia.system_available
                            --                     and tr.name = tia.name_tile
                            --                     and tr.tile_stem = tia.tile_stem_tile

                """,
                {
                    "vector_extent": vector_extent.wkt
                }).df()

            return results

    def find_available_files(self,
                             db_connection: DuckDBPyConnection,
                             extent: Polygon,
                             ) -> DataFrame:
        """
        find available preprocessed files in the database that match the current dataset and benchmark parameters
        :param db_connection: the database connection
        :return: a list of available files
        """
        with db_connection as conn:
            results = conn.execute(
                """
                with files_available as (
                    select * EXCLUDE (location, extent),
                    from available_files
                    where name = $name
                        and filetype = 'raster'),
                tiles_matching as (
                    select tr.name,
                        tr.tile_stem,
                        ST_AsText(tr.extent)                                                           as tile_extent_wkt,
                        ST_Intersects(tr.extent, ST_GeomFromText($vector_extent::VARCHAR)::POLYGON_2D) as tile_intersects_vector,
                        ST_Covers(tr.extent, ST_GeomFromText($vector_extent::VARCHAR)::POLYGON_2D)     as tile_covers_vector
                    from tiles_raster tr
                ),
                tile_locations_available as (
                    select fa.id, fa.system,
                           list(tia.tile_stem_tile || '.' || lower(fa.datatype)) as location,
                           list(tia.tile_stem_tile) as tile_stems,
                           ST_Union_Agg(tia.extent_ingested) as extent
                    from tile_in_available tia join files_available fa on tia.id_available = fa.id
                    group by fa.id, fa.system
                ),
                merged_tiles_available as (
                    select fa.id, fa.system, mf.filename_stem,
                           list(mf.filename_stem || '.' || lower(fa.datatype)) as location,
                           list(tim.tile_stem_tile) as tile_stems,
                           mf.extent as extent
                    from merged_file mf join files_available fa on mf.available_id = fa.id
                                        join tile_in_merged tim on tim.available_id = mf.available_id and tim.filename_stem = mf.filename_stem
                    group by fa.id, fa.system, mf.filename_stem, mf.extent
                ),
                available_tiles as (
                    select *,
                        (tia.id_available IS NOT NULL) as is_available,
                    from tiles_matching tm 
                        join files_available fa 
                            on tm.name = fa.name
                        left outer join tile_in_available tia 
                            on tm.name = tia.name_tile 
                            and tm.tile_stem = tia.tile_stem_tile
                            and fa.id = tia.id_available
                            and fa.system = tia.system_available
                ),
                merged_tiles as (
                    select *,
                           (tim.available_id IS NOT NULL) as is_available,
                    from tiles_matching tm 
                        join files_available fa 
                            on tm.name = fa.name
                        join merged_file mf 
                            on fa.id = mf.available_id
                            and fa.system = mf.available_system
                        left outer join tile_in_merged tim
                            on tm.name = tim.name_tile
                            and tm.tile_stem = tim.tile_stem_tile
                            and fa.id = tim.available_id
                            and mf.filename_stem = tim.filename_stem
                ),
                meta_merged as (select ir.id,
                                       ir.system,
                                       ir.filename_stem,
                                       sum(tmr.tile_covers_vector::INT) >= 1 as any_tile_covers_vector,
                                       sum((tmr.tile_covers_vector AND is_available)::INT) >= 1 as any_tile_covers_vector_available,
                                       sum(tmr.tile_intersects_vector::INT)  as count_tile_intersects_vector,
                                       sum((tmr.tile_intersects_vector AND is_available)::INT)  as count_tile_intersects_vector_available,
                                       count(*)                              as total_tiles,
                                       count_tile_intersects_vector_available / total_tiles as intersects_ratio_available,
                                       count_tile_intersects_vector / total_tiles as intersects_ratio,
                                       length(location) < sum(is_available::INT) as is_merged,
                                       (not is_merged AND count_tile_intersects_vector > 1) as needs_merge,
                                       ST_Covers(ir.extent, ST_GeomFromText($vector_extent::VARCHAR)::POLYGON_2D) as merged_available_covers_vector,
                                       from merged_tiles tmr
                                                join merged_tiles_available ir
                                                     on tmr.id = ir.id
                                                         and tmr.system = ir.system
                                       group by ir.id, ir.system, ir.location, ir.filename_stem, ir.extent),
                meta_available as (select ir.id,
                                       ir.system,
                                       sum(tmr.tile_covers_vector::INT) >= 1 as any_tile_covers_vector,
                                       sum((tmr.tile_covers_vector AND is_available)::INT) >= 1 as any_tile_covers_vector_available,
                                       sum(tmr.tile_intersects_vector::INT)  as count_tile_intersects_vector,
                                       sum((tmr.tile_intersects_vector AND is_available)::INT)  as count_tile_intersects_vector_available,
                                       count(*)                              as total_tiles,
                                       count_tile_intersects_vector_available / total_tiles as intersects_ratio_available,
                                       count_tile_intersects_vector / total_tiles as intersects_ratio,
                                       length(location) < sum(is_available::INT) as is_merged,
                                       (not is_merged AND count_tile_intersects_vector > 1) as needs_merge,
                                       ST_Covers(ir.extent, ST_GeomFromText($vector_extent::VARCHAR)::POLYGON_2D) as merged_available_covers_vector,
                                from available_tiles tmr
                                         join tile_locations_available ir
                                              on tmr.id = ir.id
                                                  and tmr.system = ir.system
                                group by ir.id, ir.system, ir.location, ir.extent)
                select *,
                       ST_Contains(extent, ST_GeomFromText($vector_extent::VARCHAR)::POLYGON_2D) as existing_contains_vector,
                       ST_AsText(available_locations.extent) as extent_wkt,
                       0 as score
                from 
                    files_available fa join
                        (select (* EXCLUDE (filename_stem))
                         from merged_tiles_available mta
                            join meta_merged mm
                                on mta.id = mm.id
                                and mta.system = mm.system
                                and mta.filename_stem = mm.filename_stem
                    union
                        select *
                        from tile_locations_available
                            join meta_available ma
                                on tile_locations_available.id = ma.id
                                and tile_locations_available.system = ma.system)
                            as available_locations on fa.id = available_locations.id
                """,
                {
                    "name": self.dataset_name,
                    "vector_extent": extent.wkt
                }).df()

            return results

    def select_relevant_tiles(self, vector_extent: Polygon, system: str, db_connection: DuckDBPyConnection):
        relevant_raster_files = {name: {"intersects": tile_extent.intersects(vector_extent),
                                        "covers": tile_extent.covers(vector_extent)}
                                 for name, tile_extent
                                 in self.get_extent_per_file().items()}

        if any([v["covers"] for v in relevant_raster_files.values()]):
            relevant_raster_files = [[name for name, v in relevant_raster_files.items() if v["covers"]][0]]
        else:
            relevant_raster_files = [name for name, v in relevant_raster_files.items() if v["intersects"]]

        if not relevant_raster_files:
            raise ValueError(f"No overlap between raster tiles and vector dataset found.")

        self._files = [Path(Path(f).name) for f in relevant_raster_files]
        self._tiles["is_relevant"] = self._tiles["tile_stem"].apply(lambda n: n in [f.stem for f in self._files])

    def register_available_tiles(self, system: str, db_connection: DuckDBPyConnection, nm: BasicNetworkManager = None):

        rt_spread = self._tiles[self._tiles["is_relevant"]][["tile_stem"]]

        remote_extent = self.get_remote_extent(nm)

        if not self._merged_name:
            db_connection.execute("""
                     insert into tile_in_available
                     select tr.name                                                                   as name_tile,
                            tr.tile_stem                                                              as tile_stem_tile,
                            $uuid                                                                     as id_available,
                            $system                                                                   as system_available,
                            ST_Contains(ST_GeomFromText($remote_extent::VARCHAR)::POLYGON_2D, tr.extent) as fully_ingested,
                            ST_Intersection(ST_GeomFromText($remote_extent::VARCHAR)::POLYGON_2D, tr.extent) as extent_ingested
                     from tiles_raster tr
                              join rt_spread on tr.tile_stem = rt_spread.tile_stem
                     where ST_intersects(tr.extent, ST_GeomFromText($remote_extent::VARCHAR)::POLYGON_2D)
                       and tr.name = $dsname
                     """,
                     {
                         "dsname": self.dataset_name,
                         "uuid": self._uuid,
                         "system": str(system),
                         "remote_extent": remote_extent.wkt
                     })

        if self._merged_name:
            db_connection.execute("""
            insert into merged_file (filename_stem, available_id, available_system, extent)
            values
                ($filename_stem, $available_id, $system, ST_GeomFromText($remote_extent::VARCHAR)::POLYGON_2D)
            """,
            {
                "filename_stem": self._merged_name,
                "available_id": self._uuid,
                "system": str(system),
                "remote_extent": remote_extent.wkt,
            })

            merged_tiles = self._tiles[self._tiles["is_relevant"]]

            db_connection.execute("""
            insert into tile_in_merged (filename_stem, available_id, name_tile, tile_stem_tile)
            select
                $filename_stem,
                $available_id,
                $dsname,
                tile_stem
            from merged_tiles mt
            """,
            {
                "filename_stem": self._merged_name,
                "available_id": self._uuid,
                "dsname": self.dataset_name,
            })



    # def set_preprocessed(self, db_connection: DuckDBPyConnection, params: BenchmarkParameters, workload: dict,
    #                       nm: BasicNetworkManager, optimizer_run: bool = False):
    #     super().set_preprocessed(db_connection, params, workload, nm, optimizer_run)
    #
    #     if self.should_preprocess:
    #         self.register_available_tiles("filesystem", db_connection)


