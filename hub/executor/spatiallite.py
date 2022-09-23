from pathlib import Path
from hub.utils.filetransporter import FileTransporter
from hub.evaluation.measure_time import measure_time


class Executor:
    def __init__(self, vector_path, raster_path, network_manager) -> None:
        self.logger = {}
        self.network_manager = network_manager
        self.transporter = FileTransporter(network_manager)
        if Path(vector_path).exists() and Path(vector_path).is_dir():
            vector_path = [vector for vector in Path(vector_path).glob("*.shp")][0]
        if Path(raster_path).exists() and Path(raster_path).is_dir():
            raster_path = [raster for raster in Path(raster_path).glob("*.tif*")][0]
        self.table1 = f"{vector_path.stem}".split(".")[0]
        self.table2 = f"{raster_path.stem}".split(".")[0]

    def __translate(self, workload):
        print(f"translating to {self.network_manager.system} query language")
        return workload

    @measure_time
    def run_query(self, workload, **kwargs):
        self.__translate(workload)
        selection = 'select t1.name, avg(t2."values") as avg, max(t2."values") as max, min(t2."values") as min, count(t2."values") as count'
        join = f"from {self.table1} as t1, {self.table2} as t2"
        condition = (
            'where st_intersects(t1.geometry,t2.geometry) = 1 and t2."values" >= 0'
        )
        group = "group by t1.name"
        order = "order by t1.name"
        query = f"{selection} {join} {condition} {group} {order}"
        self.network_manager.run_ssh(f"~/config/execute.sh -q '{query}'", **kwargs)
        self.transporter.get_file(
            "~/data/results.csv",
            f"~/results_{self.network_manager.system}.csv",
            **kwargs,
        )
