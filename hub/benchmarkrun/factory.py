import copy

from pyproj import CRS

from hub.enums.rasterfiletype import RasterFileType
from hub.enums.stage import Stage
from hub.enums.vectorfiletype import VectorFileType
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.enums.vectorizationtype import VectorizationType
from hub.benchmarkrun.tilesize import TileSize
from hub.enums.datatype import DataType
from hub.utils.system import System


class BenchmarkRunFactory:
    """
    creates the different benchmark parameter combinations based on the parameters definition in the benchmark config file
    """

    def __init__(self, capabilities):
        self.capabilities = capabilities

    def create_params_iterations(self, systems: list[System], params_dict: dict) -> list[BenchmarkParameters]:
        """
        create the iterations
        :param systems: the systems to create parameter combinations for
        :param params_dict: the parameters
        :return: the benchmark parameters as BenchmarkParameters obejcts
        """
        params_list = [BenchmarkParameters(system) for system in systems]
        return self._create_param_iter_step(params_dict, params_list)

    def _create_param_iter_step(self, params_dict: dict, params_list: list[BenchmarkParameters]):
        if not params_dict:
            return params_list

        params_key = list(params_dict.keys()).pop()

        updated_params_list = copy.copy(params_list)
        param: BenchmarkParameters
        for param in params_list:
            match params_key:
                case "raster_format":
                    if param.system.name not in self.capabilities["vectorize"]:
                        for f in params_dict["raster_format"]:
                            p = copy.deepcopy(param)
                            p.raster_target_format = RasterFileType.get_by_value(str(f).lower()) if isinstance(f, str) else f
                            updated_params_list.append(p)

                        updated_params_list.remove(param)

                case "rasterize_format":
                    if param.system.name in self.capabilities["rasterize"]:
                        for f in params_dict["rasterize_format"]:
                            p = copy.deepcopy(param)
                            p.vector_target_format = RasterFileType.get_by_value(str(f).lower()) if isinstance(f, str) else f
                            updated_params_list.append(p)

                        updated_params_list.remove(param)

                case "raster_target_crs":
                    for c in params_dict["raster_target_crs"]:
                        p = copy.deepcopy(param)
                        p.raster_target_crs = CRS.from_string(c)
                        updated_params_list.append(p)

                    updated_params_list.remove(param)

                case "raster_tile_size":
                    if param.system.name in self.capabilities["variable_tile_size"]:
                        for t in params_dict["raster_tile_size"]:
                            p = copy.deepcopy(param)
                            if str(t) == "auto":
                                p.raster_tile_size = TileSize(-1, -1)
                            else:
                                width, height = tuple(str(t).split("x"))
                                p.raster_tile_size = TileSize(int(width), int(height))
                            updated_params_list.append(p)

                        updated_params_list.remove(param)

                case "raster_depth":
                    for d in params_dict["raster_depth"]:
                        p = copy.deepcopy(param)
                        p.raster_depth = int(d)
                        updated_params_list.append(p)

                    updated_params_list.remove(param)

                case "raster_resolution":
                    for r in params_dict["raster_resolution"]:
                        p = copy.deepcopy(param)
                        p.raster_resolution = float(r)
                        updated_params_list.append(p)

                    updated_params_list.remove(param)

                case "vectorize_type":
                    if param.system.name in self.capabilities["vectorize"]:
                        for f in params_dict["vectorize_type"]:
                            p = copy.deepcopy(param)
                            p.vectorize_type = VectorizationType.get_by_value(str(f).lower()) if isinstance(f, str) else f
                            updated_params_list.append(p)

                        updated_params_list.remove(param)

                case "vector_format":
                    if param.system.name not in self.capabilities["rasterize"]:
                        for f in params_dict["vector_format"]:
                            p = copy.deepcopy(param)
                            p.vector_target_format = VectorFileType.get_by_value(str(f).lower()) if isinstance(f, str) else f
                            updated_params_list.append(p)

                        updated_params_list.remove(param)

                case "vectorize_format":
                    if param.system.name in self.capabilities["vectorize"]:
                        for f in params_dict["vectorize_format"]:
                            p = copy.deepcopy(param)
                            p.raster_target_format = VectorFileType.get_by_value(str(f).lower()) if isinstance(f, str) else f
                            updated_params_list.append(p)

                        updated_params_list.remove(param)

                case "vector_target_crs":
                    for c in params_dict["vector_target_crs"]:
                        p = copy.deepcopy(param)
                        p.vector_target_crs = CRS.from_string(c)
                        updated_params_list.append(p)

                    updated_params_list.remove(param)

                case "vector_simplify":
                    for r in params_dict["vector_simplify"]:
                        p = copy.deepcopy(param)
                        p.vector_simplify = float(r)
                        updated_params_list.append(p)

                    updated_params_list.remove(param)

                case "align_to_crs":
                    if isinstance(params_dict["align_to_crs"], list):
                        for s in params_dict["align_to_crs"]:
                            p = copy.deepcopy(param)
                            p.align_to_crs = DataType.get_by_value(s) if isinstance(s, str) else s
                            updated_params_list.append(p)

                        updated_params_list.remove(param)
                    else:
                        match params_dict["align_to_crs"]:
                            case "vector":
                                param.align_to_crs = DataType.VECTOR
                            case "raster":
                                param.align_to_crs = DataType.RASTER
                            case "both":
                                p_v = copy.deepcopy(param)
                                p_v.align_to_crs = DataType.VECTOR
                                updated_params_list.append(p_v)

                                p_r = copy.deepcopy(param)
                                p_r.align_to_crs = DataType.RASTER
                                updated_params_list.append(p_r)

                                updated_params_list.remove(param)

                case "align_crs_at_stage":
                    if param.system.name in self.capabilities["no_st_transform"]:
                        param.align_crs_at_stage = Stage.PREPROCESS
                    else:
                        for s in params_dict["align_crs_at_stage"]:
                            p = copy.deepcopy(param)
                            p.align_crs_at_stage = Stage.get_by_value(s) if isinstance(s, str) else s
                            updated_params_list.append(p)

                        updated_params_list.remove(param)

                case "vector_filter_at_stage":
                    # if param.system.name in self.capabilities["no_st_transform"]:
                    #     param.align_crs_at_stage = Stage.PREPROCESS
                    # else: TODO maybe enable to use systems that do not support filtering
                        for s in params_dict["vector_filter_at_stage"]:
                            p = copy.deepcopy(param)
                            p.vector_filter_at_stage = Stage.get_by_value(s) if isinstance(s, str) else s
                            updated_params_list.append(p)

                        updated_params_list.remove(param)

                case "raster_clip":
                    for r in params_dict["raster_clip"]:
                        p = copy.deepcopy(param)
                        p.raster_clip = bool(r)
                        updated_params_list.append(p)

                    updated_params_list.remove(param)

        del params_dict[params_key]
        return self._create_param_iter_step(params_dict, updated_params_list)
