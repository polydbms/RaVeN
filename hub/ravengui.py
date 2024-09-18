import logging
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import pandas as pd
import processing
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QAction, QDialog, QProgressBar
from qgis.core import QgsFeatureRequest, QgsProject, QgsVectorLayer, QgsGraduatedSymbolRenderer, \
    QgsSymbol, \
    QgsLayerTreeLayer, \
    QgsStyle

from hub.configuration import PROJECT_ROOT
from hub.zsresultsdb.init_duckdb import InitializeDuckDB
from hub.gui.main_ui import Ui_RaVeN
from hub.gui.webdialog import WebDialog
from hub.raven import Setup
from hub.utils.fileio import FileIO
from hub.utils.system import System

formatter = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=formatter)


class Raven:
    def __init__(self, iface):
        self.iface = iface
        self.benchi = Setup(self.increase_progress_bar)
        self.dialog_web = None

    def show_progress_toast(self):
        progressMessageBar = self.iface.messageBar().createMessage("Running RaVeN …")
        print(progressMessageBar)

        self.progress = QProgressBar()
        self.progress.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.progress.setMaximum(self.benchi.max_progress)
        progressMessageBar.layout().addWidget(self.progress)
        self.iface.messageBar().pushWidget(progressMessageBar, 0)

    def increase_progress_bar(self, progress, max_progress):
        # self.iface.statusBarIface().showMessage("Running RaVeN … {} %".format(int(progress / max_progress)))
        self.progress.setValue(progress)

    def initGui(self):
        self.raven_button = QAction("RaVeN", self.iface.mainWindow())
        self.raven_button.triggered.connect(self.run_raven)
        self.iface.addToolBarIcon(self.raven_button)

        self.web_button = QAction("RaVeN Benchmark", self.iface.mainWindow())
        self.web_button.triggered.connect(self.run_results)
        self.iface.addToolBarIcon(self.web_button)

    def unload(self):
        self.iface.removeToolBarIcon(self.raven_button)
        del self.raven_button

        self.iface.removeToolBarIcon(self.web_button)
        del self.web_button

    def run_raven(self):
        self.dialog_raven = RaVeNDialog(self.iface, self.execute_tasks)
        self.dialog_raven.show()

    def run_results(self):
        if self.dialog_web is None:
            self.dialog_web = WebDialog(iface=self.iface, window_name="RaVeN Results")
            self.dialog_web.ui.add_tab("End-to-End Runtime Overview",
                                       "/home/gereon/git/dima/tpctc-graphs/plots/demo-e2e.png")
            self.dialog_web.ui.add_tab("Phase Breakdown",
                                       "/home/gereon/git/dima/vldb-benchi/figures/breakdown_amtrak.png")
            self.dialog_web.ui.add_tab("Phase Breakdown",
                                       "/tmp/stagegraph.png")
        self.dialog_web.show()

    def execute_tasks(self, runs, inp_vector, vector_fields, inp_raster, inp_raster_agg, iterations, warm_starts,
                      interaction_params):
        if len(runs) > 0:
            self.benchi.init_progress(len(runs), iterations.value(), warm_starts.value())
            self.show_progress_toast()

            set_id = runs[0].host_params.controller_db_connection.initialize_benchmark_set("qgis.yaml", {})

            result_files = []
            # run = next(iter(runs))
            print(f"running {len(runs)} runs: {','.join(str(runs))}")

            def run_bg():
                for run in runs:
                    result_files.extend(self.benchi.run_tasks(run))

                runs[0].host_params.controller_db_connection.close_connection()

                raven_result_layer = inp_vector.currentLayer().materialize(
                    QgsFeatureRequest().setFilterFids(inp_vector.currentLayer().allFeatureIds()))
                raven_result_layer.setName("RaVeN Result")
                raven_result_layer.startEditing()

                result = pd.read_csv(str(result_files[0]))
                types = []
                for colname, type in result.dtypes.items():
                    match type:
                        case "int64":
                            types.append('"Integer"')
                        case "float64":
                            types.append('"Real"')
                        case _:
                            types.append('"String"')

                with result_files[0].with_suffix(".csvt").open(mode="w") as f:
                    f.write(",".join(types))

                raven_join = processing.run('qgis:joinattributestable',
                                            {"INPUT": inp_vector.currentLayer(),
                                             "FIELD": vector_fields[0],
                                             "INPUT_2": str(result_files[0]),
                                             "FIELD_2": vector_fields[0],
                                             "METHOD": 1,
                                             "DISCARD_NONMATCHING": False,
                                             "PREFIX": "",
                                             "OUTPUT": "memory"})
                raven_layer = QgsVectorLayer(raven_join['OUTPUT'], "RaVeN Result", "ogr")
                QgsProject.instance().addMapLayer(raven_layer)

                # first add the layer without showing it
                QgsProject.instance().addMapLayer(raven_layer, False)
                # obtain the layer tree of the top-level group in the project
                layerTree = self.iface.layerTreeCanvasBridge().rootGroup()
                # the position is a number starting from 0, with -1 an alias for the end
                layerTree.insertChildNode(0, QgsLayerTreeLayer(raven_layer))

                first_agg_name = next(filter(lambda n: (inp_raster_agg.checkedItems()[0].lower()) in (n.lower()),
                                             list(result.columns)))

                renderer = QgsGraduatedSymbolRenderer.createRenderer(raven_layer, first_agg_name, 10,
                                                                     QgsGraduatedSymbolRenderer.Mode.EqualInterval,
                                                                     QgsSymbol.defaultSymbol(
                                                                         raven_layer.geometryType()),
                                                                     QgsStyle().defaultStyle().colorRamp('Blues'))

                # Apply the renderer to the layer
                raven_layer.setRenderer(renderer)

                raven_layer.triggerRepaint()
                self.iface.messageBar().clearWidgets()

                if True:
                    db_path: Path = runs[0].host_params.db_path
                    db_path_tmp = Path("/tmp/results.tmp.db")
                    shutil.copy(str(db_path), str(db_path_tmp))

                    graph_cmd = ["Rscript", "--vanilla", str(PROJECT_ROOT.joinpath("graphs/stagegraph.R")),
                                 "-o", "/tmp/stagegraph.png",
                                 "-s", str(set_id),
                                 "-i", ",".join(interaction_params)]
                    print(f"plotting graph with: {' '.join(graph_cmd)}")

                    with open("/tmp/stagegraph.log", "w") as f:
                        subprocess.call(graph_cmd, stderr=f, stdout=f)

                    self.dialog_web = WebDialog(iface=self.iface, window_name="RaVeN Results")
                    self.dialog_web.ui.add_tab("Phase Breakdown",
                                               "/tmp/stagegraph.png")
                    self.dialog_web.show()

                cleanup_thread = threading.Thread(target=self.benchi.clean, name="Cleanup",
                                                  args=[
                                                      "/home/gereon/git/dima/benchi/config/controller_config.qgis.yaml"])
                cleanup_thread.start()

            main_thread = threading.Thread(target=run_bg, name="Main Run")
            main_thread.start()


class RaVeNDialog(QDialog):
    def __init__(self, iface, execute_benchmarks, parent=None):
        super().__init__(parent)
        self.iface = iface
        # Create an instance of the GUI
        self.ui = Ui_RaVeN()
        # Run the .setupUi() method to show the GUI
        self.ui.setupUi(self)
        self.execute_benchmarks = execute_benchmarks

    def accept(self):
        runs, iterations, vector_fields = self.ui.get_runs_from_ui()

        host_params = FileIO.get_host_params("/home/gereon/git/dima/benchi/config/controller_config.qgis.yaml")
        InitializeDuckDB(host_params.controller_db_connection, runs, "qgis.yaml")

        print(f"running {len(runs)} experiments")
        print([str(r.benchmark_params) for r in runs])
        self.hide()

        self.execute_benchmarks(runs, self.ui.inp_vector, vector_fields, self.ui.inp_raster, self.ui.inp_raster_agg,
                                self.ui.inp_iterations, self.ui.inp_warm_starts, self.ui.get_interactions())
