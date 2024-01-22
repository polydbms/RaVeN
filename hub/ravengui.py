import logging
import re

import pandas as pd
from PyQt5 import QtCore
from PyQt5.QtCore import QRect, Qt, QMetaObject, QCoreApplication, QVariant, QAbstractTableModel, QModelIndex
from PyQt5.QtGui import QStandardItemModel, QFont
from PyQt5.QtSql import QSqlTableModel
from PyQt5.QtWidgets import QAction, QFormLayout, QComboBox, QLabel, QLineEdit, QDialogButtonBox, QWidget, \
    QDialog, QCheckBox, QSpinBox, QTableView, QPushButton, QListWidget, QTextEdit, QHBoxLayout, QListWidgetItem, QFrame, \
    QSpacerItem, QSizePolicy
from pyqt_editable_list_widget import EditableListWidget

from hub.gui.browser_window import Ui_Web
from hub.gui.main_ui import Ui_RaVeN
# from hub.raven import Setup
# from hub.utils.fileio import FileIO
# from hub.utils.system import System
# from qgis.gui import QgsMapLayerComboBox, QgsFieldComboBox, QgsCheckableComboBox, QgsQueryBuilder
# from qgis.core import QgsMapLayerProxyModel, QgsFeatureRequest, QgsField, QgsProject, QgsVectorLayerJoinInfo, \
#     QgsVectorLayer, QgsGraduatedSymbolRenderer, QgsSymbol, QgsRendererRange, QgsSymbolLayerUtils, QgsLayerTreeLayer, \
#     QgsStyludo
# import processing

formatter = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=formatter)


class Raven:
    def __init__(self, iface):
        self.iface = iface
        # self.benchi = Setup()

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
        self.dialog = RaVeNDialog(iface=self.iface)
        self.dialog.exec()

    def run_results(self):
        self.dialog = WebDialog(iface=self.iface)
        self.dialog.exec()

class WebDialog(QDialog):
    def __init__(self, iface, parent=None):
        super().__init__(parent)
        self.iface = iface
        # Create an instance of the GUI
        self.ui = Ui_Web()
        # Run the .setupUi() method to show the GUI
        self.ui.setupUi(self)

class RaVeNDialog(QDialog):
    def __init__(self, iface, parent=None):
        super().__init__(parent)
        self.iface = iface
        # Create an instance of the GUI
        self.ui = Ui_RaVeN()
        # Run the .setupUi() method to show the GUI
        self.ui.setupUi(self)

    # def accept(self):
    #     if self.ui.inp_vector.currentLayer().storageType() == "ESRI Shapefile":
    #         pattern = re.compile(r'(.+?)\|layername=(.+)')
    #         match = pattern.match(self.ui.inp_vector.currentLayer().source())
    #         vector_file = f"{match.group(1)}/{match.group(2)}.shp"
    #     else:
    #         vector_file = self.ui.inp_vector.currentLayer().source()
    #
    #     raster_file = self.ui.inp_raster.currentLayer().source()
    #
    #     vector_fields = [self.ui.inp_vector_field.currentField()]
    #
    #     parameters = {}
    #
    #     runs, iterations = FileIO.create_configs({"raster": raster_file, "vector": vector_file},
    #                                              {}, "qgis.yaml", self.ui.inp_system.currentText(),
    #                                              [System('postgis', 25432),
    #                                               System('omnisci', 6274),
    #                                               System('sedona', 80),
    #                                               System('beast', 80),
    #                                               System('rasdaman', 8080)],
    #                                              {'get': {'vector': vector_fields, 'raster': [
    #                                                  {'sval': {
    #                                                      'aggregations': self.ui.inp_raster_agg.checkedItems()}}]},
    #                                               'join': {'table1': 'vector',
    #                                                        'table2': 'raster',
    #                                                        'condition': 'intersect(raster, vector)'},
    #                                               'group': {'vector': vector_fields},
    #                                               'order': {'vector': vector_fields}}
    #
    #                                              , "/home/gereon/git/dima/benchi/controller_config.qgis.yaml",
    #                                              parameters)
    #     print(f"running {len(runs)} experiments")
    #     print([str(r.benchmark_params) for r in runs])
    #
    #     runs[0].host_params.controller_db_connection.initialize_benchmark_set("qgis.yaml")
    #
    #     setup = Setup()
    #     result_files = []
    #
    #     if len(runs) > 0:
    #         run = runs[0]
    #         # run = next(iter(runs))
    #         print(str(run))
    #         result_files.extend(setup.run_tasks(run))
    #
    #         raven_result_layer = self.ui.inp_vector.currentLayer().materialize(
    #             QgsFeatureRequest().setFilterFids(self.ui.inp_vector.currentLayer().allFeatureIds()))
    #         raven_result_layer.setName("RaVeN Result")
    #         raven_result_layer.startEditing()
    #
    #         result = pd.read_csv(str(result_files[0]))
    #         types = []
    #         for colname, type in result.dtypes.items():
    #             match type:
    #                 case "int64":
    #                     types.append('"Integer"')
    #                 case "float64":
    #                     types.append('"Real"')
    #                 case _:
    #                     types.append('"String"')
    #
    #         with result_files[0].with_suffix(".csvt").open(mode="w") as f:
    #             f.write(",".join(types))
    #
    #         raven_join = processing.run('qgis:joinattributestable',
    #                                     {"INPUT": self.ui.inp_vector.currentLayer(),
    #                                      "FIELD": vector_fields[0],
    #                                      "INPUT_2": str(result_files[0]),
    #                                      "FIELD_2": vector_fields[0],
    #                                      "METHOD": 1,
    #                                      "DISCARD_NONMATCHING": False,
    #                                      "PREFIX": "",
    #                                      "OUTPUT": "memory"})
    #         raven_layer = QgsVectorLayer(raven_join['OUTPUT'], "RaVeN Result", "ogr")
    #         QgsProject.instance().addMapLayer(raven_layer)
    #
    #         # first add the layer without showing it
    #         QgsProject.instance().addMapLayer(raven_layer, False)
    #         # obtain the layer tree of the top-level group in the project
    #         layerTree = self.iface.layerTreeCanvasBridge().rootGroup()
    #         # the position is a number starting from 0, with -1 an alias for the end
    #         layerTree.insertChildNode(0, QgsLayerTreeLayer(raven_layer))
    #
    #         first_agg_name = next(filter(lambda n: (self.ui.inp_raster_agg.checkedItems()[0].lower()) in (n.lower()),
    #                                      list(result.columns)))
    #
    #         renderer = QgsGraduatedSymbolRenderer.createRenderer(raven_layer, first_agg_name, 10,
    #                                                              QgsGraduatedSymbolRenderer.Mode.EqualInterval,
    #                                                              QgsSymbol.defaultSymbol(raven_layer.geometryType()),
    #                                                              QgsStyle().defaultStyle().colorRamp('Blues'))
    #
    #         # Apply the renderer to the layer
    #         raven_layer.setRenderer(renderer)
    #
    #         raven_layer.triggerRepaint()
    #
    #     setup.clean("/home/gereon/git/dima/benchi/controller_config.qgis.yaml")
    #     self.hide()
