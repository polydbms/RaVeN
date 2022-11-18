from pathlib import Path

import rasterio
import requests
from jinja2 import Template
from lxml import etree

from configuration import PROJECT_ROOT
from hub.benchmarkrun.benchmark_params import BenchmarkParameters
from hub.evaluation.measure_time import measure_time
from hub.utils.datalocation import DataLocation
from hub.utils.filetransporter import FileTransporter
from hub.utils.network import NetworkManager


class Ingestor:
    def __init__(self, vector_path: DataLocation, raster_path: DataLocation, network_manager: NetworkManager, benchmark_params: BenchmarkParameters) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.vector_path = vector_path
        self.raster_path = raster_path
        self.host_base_path = self.network_manager.host_params.host_base_path
        self.benchmark_params = benchmark_params
        # self.vector_path = None
        # self.raster_path = None
        self.transporter = FileTransporter(network_manager)
        # if Path(vector_path).exists() and Path(vector_path).is_dir():
        #     self.vector_path = [vector for vector in Path(vector_path).glob("*.shp")][0]
        # if Path(raster_path).exists() and Path(raster_path).is_dir():
        #     self.raster_path = [raster for raster in Path(raster_path).glob("*.tif*")][
        #         0
        #     ]

        socks_proxy_url = self.network_manager.open_socks_proxy()
        self.proxies = dict(http=socks_proxy_url, https=socks_proxy_url)
        self.base_url = f"http://0.0.0.0:48080/rasdaman"

    @measure_time
    def ingest_raster(self, **kwargs):
        template_path = Path(PROJECT_ROOT.joinpath("hub/deployment/files/rasdaman/ingestion.json.j2"))
        try:
            with open(template_path) as file_:
                template = Template(file_.read())
        except FileNotFoundError:
            print(f"{template_path} not found")

        with rasterio.open(self.raster_path.controller_file) as f:
            epsg_crs = f.crs.to_epsg()

        payload = {
                      "coverage_id": str(self.raster_path.name),
                      "paths": [str(self.raster_path.docker_file)],
                      "epsg_crs": str(epsg_crs)
                  } | self.get_axes_abbrs(str(epsg_crs))
        print(payload)

        rendered = template.render(**payload)
        ingest_def_path = Path("hub/deployment/files/rasdaman/ingredients.json")

        with open(ingest_def_path, "w") as f:
            f.write(rendered)

        self.transporter.send_file(
            ingest_def_path,
            self.host_base_path.joinpath("config/rasdaman/ingredients.json")
        )
        self.network_manager.run_ssh(
            str(self.host_base_path.joinpath("config/rasdaman/ingest.sh"))
        )
        # ingest_def_path.unlink()
        self.network_manager.stop_socks_proxy()

    @measure_time
    def ingest_vector(self, **kwargs):
        print("cannot ingest vectors")

    def get_axes_abbrs(self, epsg):
        response = requests.get(f"{self.base_url}/def/crs/EPSG/0/{epsg}", proxies=self.proxies)
        crs_xml = etree.fromstring(response.content)

        axes_abbr = {'lon_axis_abbr': '', 'lat_axis_abbr': ''}

        search_string = ".//gml:cartesianCS/gml:CartesianCS/gml:axis" \
            if crs_xml.findall(".//gml:cartesianCS", crs_xml.nsmap) \
            else ".//gml:ellipsoidalCS/gml:EllipsoidalCS/gml:axis"  # TODO is this correct? see benchi#13

        for axis in crs_xml.findall(search_string, crs_xml.nsmap):
            direction = axis.find("./gml:CoordinateSystemAxis/gml:axisDirection", crs_xml.nsmap).text
            if direction in ("east", "west"):
                axes_abbr['lon_axis_abbr'] = axis.find("./gml:CoordinateSystemAxis/gml:axisAbbrev", crs_xml.nsmap).text
            elif direction in ("north", "south"):
                axes_abbr['lat_axis_abbr'] = axis.find("./gml:CoordinateSystemAxis/gml:axisAbbrev", crs_xml.nsmap).text
            else:
                raise Exception("axis direction not found. please check EPSG in rasdaman")

        if not axes_abbr['lon_axis_abbr'] or not axes_abbr['lat_axis_abbr']:
            raise Exception("axes information not found. please check info on CRS")

        return axes_abbr
