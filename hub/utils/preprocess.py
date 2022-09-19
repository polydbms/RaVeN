import json
import zipfile

from osgeo import ogr, gdal
import re
import argparse
from os import mkdir
import rioxarray as rxr
import geopandas as gpd
from pathlib import Path
from hub.evaluation.main import measure_time
from hub.utils.datalocation import DataLocation, FileType
from hub.utils.network import NetworkManager


class Preprocessor:
    vector_path: str
    raster_path: str

    def __init__(self, vector_path=None, raster_path=None) -> None:
        self.logger = {}
        self.vector_path = vector_path.find_file(vector_path, "*.shp")
        self.raster_path = raster_path.find_file(raster_path, "*.tif*")

    def get_vector(self):
        vector = gpd.read_file(self.vector_path)
        return vector

    def get_vector_crs(self):
        return self.get_vector().crs

    def get_raster(self):
        return rxr.open_rasterio(self.raster_path, masked=True).squeeze()

    def get_raster_crs(self):
        return self.get_raster().rio.crs

    def create_dir(self, dir):
        Path(dir).mkdir(parents=True, exist_ok=True)
        return dir


class CRSPreprocessor(Preprocessor):
    def __init__(self, vector_path=None, raster_path=None, output=None) -> None:
        super().__init__(vector_path, raster_path)
        self.output = self.create_dir(output)

    @measure_time
    def reproject_raster(self, **kwargs):
        vector_crs = self.get_vector_crs()
        raster = self.get_raster()
        out = raster.rio.reproject(vector_crs)
        out.rio.to_raster(self.raster_path)
        print(
            f"Transfered {self.raster_path} CRS from {raster.rio.crs} to {out.rio.crs}"
        )

    @measure_time
    def reproject_vector(self, **kwargs):
        raster_crs = self.get_raster_crs()
        vector = self.get_vector()
        out = vector.to_crs(f"{raster_crs}")
        out.to_file(self.output)
        print(f"Transfered {self.vector_path} CRS from {vector.crs} to {out.crs}")


class FileTypeProcessor(Preprocessor):
    def __init__(self, vector_path, output, raster_path=None) -> None:
        super().__init__(vector_path)
        self.output = self.create_dir(output)

    @measure_time
    def shape_to_wkt(self, **kwargs):
        vector = self.get_vector()
        # invert lat with long
        wkt = [
            re.sub("([-]?\d*[.]\d*) ([-]?\d*[.]\d*)", r"\2 \1", geom.wkt)
            for geom in vector.geometry
        ]
        vector["wkt"] = wkt
        output = f"/data/{self.vector_path.stem}.json"
        json_out = vector.to_json()
        with open(output, "w") as f:
            f.write(json_out)

    @measure_time
    def read_wkt(self, **kwargs):
        output = f"/data/{self.vector_path.stem}.json"
        wkt = Path(output).read_bytes()
        return json.loads(wkt)


class DataModelProcessor(Preprocessor):
    def __init__(self, vector_path, raster_path, output=None) -> None:
        super().__init__(vector_path, raster_path)
        self.output = self.create_dir(output)
        self.shp_driver = ogr.GetDriverByName("ESRI Shapefile")

    @measure_time
    def vectorize(self, band_nr=1, **kwargs):
        reprojected_raster_path = Path(f"{self.output}/{self.raster_path.name}")
        output = f"{self.output}/{reprojected_raster_path.stem}.shp"
        open_image = gdal.Open(str(reprojected_raster_path))
        input_band = open_image.GetRasterBand(band_nr)
        input_band.SetNoDataValue(0)

        output_shapefile = self.shp_driver.CreateDataSource(output)
        layer = output_shapefile.CreateLayer(Path(output).stem, srs=None)
        newField = ogr.FieldDefn("values", ogr.OFTReal)
        layer.CreateField(newField)
        gdal.FPolygonize(input_band, None, layer, 0, [], callback=None)
        layer.SyncToDisk()

    @measure_time
    def rasterize(
            self, file: str, pixel_size=10, nod_data=0, options=None, filters=None, **kwargs
    ):
        output = f"{self.output}/{self.vector_path.stem}.tif"
        open_shp = ogr.Open(file)
        shp_layer = open_shp.GetLayer()
        source_srs = shp_layer.GetSpatialRef()
        for filter in filters:
            shp_layer.SetAttributeFilter(filter)
        x_min, x_max, y_min, y_max = shp_layer.GetExtent()
        # calculate raster resolution
        x_res = int((x_max - x_min) / pixel_size)
        y_res = int((y_max - y_min) / pixel_size)
        # set the image type for export
        driver = gdal.GetDriverByName("GTiff")
        new_raster = driver.Create(output, x_res, y_res, 1, gdal.GDT_Float32)
        new_raster.SetGeoTransform((x_min, pixel_size, 0, y_max, 0, -pixel_size))
        new_raster.SetProjection(source_srs.ExportToWkt())
        # get the raster band we want to export too
        raster_band = new_raster.GetRasterBand(1)
        # assign the no data value to empty cells
        raster_band.SetNoDataValue(nod_data)
        # run vector to raster on new raster with input Shapefile
        gdal.RasterizeLayer(new_raster, [1], shp_layer, options=options)


class FileTransporter:
    def __init__(self, network_manager: NetworkManager) -> None:
        self.network_manager = network_manager
        remote = self.network_manager.ssh_connection.split("ssh ")[-1]
        private_key_path = self.network_manager.private_key_path
        self.ssh_command = (
            f"ssh {remote} -o 'StrictHostKeyChecking=no' -o 'IdentitiesOnly=yes' -i {private_key_path}"
        )
        self.scp_command_send = f"scp -o 'StrictHostKeyChecking=no' -o 'IdentitiesOnly=yes' -i {private_key_path} options from_File_plch {remote}:to_File_plch"
        self.scp_command_recieve = f"scp -o 'StrictHostKeyChecking=no' -o 'IdentitiesOnly=yes' -i {private_key_path} options {remote}:from_File_plch to_File_plch"
        print(self.ssh_command)
        print(self.scp_command_send)

    @measure_time
    def send_folder(self, local, remote, **kwargs):
        command = (
            self.scp_command_send.replace("options", "-r")
            .replace("from_File_plch", local)
            .replace("to_File_plch", remote)
        )
        return self.network_manager.run_command(command)

    @measure_time
    def send_file(self, local, remote, **kwargs):
        if Path(local).exists():
            command = (
                self.scp_command_send.replace("options", "")
                .replace("from_File_plch", local)
                .replace("to_File_plch", remote)
            )
            return self.network_manager.run_command(command)
        raise FileNotFoundError(local)

    @measure_time
    def send_folder(self, local, remote, **kwargs):
        if Path(local).exists():
            command = (
                self.scp_command_send.replace("options", "-r")
                .replace("from_File_plch", local)
                .replace("to_File_plch", remote)
            )
            return self.network_manager.run_command(command)
        raise FileNotFoundError(local)

    @measure_time
    def get_file(self, remote, local, **kwargs):
        command = (
            self.scp_command_recieve.replace("options", "")
            .replace("from_File_plch", remote)
            .replace("to_File_plch", local)
        )
        return self.network_manager.run_command(command)

    @measure_time
    def get_folder(self, remote, local, **kwargs):
        command = (
            self.scp_command_recieve.replace("options", "-r")
            .replace("from_File_plch", remote)
            .replace("to_File_plch", local)
        )
        return self.network_manager.run_command(command)

    @measure_time
    def send_configs(self, rootPath, **kwargs):
        self.send_folder(
            f"{rootPath}/hub/deployment/files/{self.network_manager.system}", "~/config"
        )

    @measure_time
    def send_data(self, file: DataLocation, **kwargs):
        """Method for sending data to remote."""
        print(file)
        self.network_manager.run_command(f"{self.ssh_command} mkdir -p {file.host_dir}")
        if file.type == FileType.FILE:
            raise NotImplementedError("Single Files are currently not supported")
            # self.send_file(file.controller_location, file.host_dir)
        elif file.type == FileType.FOLDER:
            self.send_folder(file.controller_location, file.host_dir)
        elif file.type == FileType.ZIP_ARCHIVE:
            self.send_file(file.controller_location, file.host_dir)
            self.network_manager.run_command(f"{self.ssh_command} unzip")
        else:
            print("sent nothing")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--vector_path", help="Specify the path to vector dataset", required=True
    )
    parser.add_argument(
        "--raster_path", help="Specify the path to raster dataset", required=True
    )
    parser.add_argument("--output", help="Specify the output path")
    parser.add_argument("--system", help="Specify which system should be benchmarked")
    args = parser.parse_args()
    print(args)
    crs_preprocessor = CRSPreprocessor(
        vector_path=args.vector_path,
        raster_path=args.raster_path,
        output=args.output,
    )
    data_preprocessor = DataModelProcessor(
        vector_path=args.vector_path,
        raster_path=args.raster_path,
        output=args.output,
    )
    crs_preprocessor.reproject_raster(log_time=crs_preprocessor.logger)
    if args.system != "postgis" and args.system != "rasdaman":
        data_preprocessor.vectorize()
    print(crs_preprocessor.logger)
    print(data_preprocessor.logger)
    if args.system == "rasdaman":
        file_type_preprocessor = FileTypeProcessor(
            vector_path=args.vector_path, output=args.output
        )
        file_type_preprocessor.shape_to_wkt(log_time=file_type_preprocessor.logger)
        print(file_type_preprocessor.logger)


if __name__ == "__main__":
    main()
