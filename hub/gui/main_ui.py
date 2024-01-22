from PyQt5.QtCore import Qt, QRect, QMetaObject, QCoreApplication
from PyQt5.QtGui import QFont
from PyQt5.QtWidgets import QLabel, QFrame, QFormLayout, QDialogButtonBox, QWidget, QCheckBox, QPushButton, QSpacerItem, \
    QSizePolicy, QTableView, QHBoxLayout, QListWidgetItem, QSpinBox
from pyqt_editable_list_widget import EditableListWidget

from hub.gui.tiletablemodel import EditTileTableModel
from qgis.core import QgsMapLayerProxyModel
from qgis.gui import QgsCheckableComboBox, QgsMapLayerComboBox, QgsFieldComboBox, QgsQueryBuilder

class Ui_RaVeN(object):
    def build_ccb(self, name, items):
        label = QLabel(self.formLayoutWidget)
        label.setObjectName(u"lbl_" + name)

        inp = QgsCheckableComboBox(self.formLayoutWidget)
        inp.setObjectName(u"inp_" + name)
        for (item, checked) in items:
            inp.addItemWithCheckState(item, Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked)

        return label, inp
    #
    def build_divider(self, RaVeN, row):
        font = QFont()
        font.setBold(True)
        font.setPointSize(12)

        label = QLabel(self.formLayoutWidget)
        label.setObjectName(f"lbl_title_{row}")
        label.setFont(font)

        lineLayers = QFrame(RaVeN)
        lineLayers.setObjectName(f"line{row}")
        lineLayers.setFrameShape(QFrame.HLine)
        lineLayers.setFrameShadow(QFrame.Sunken)

        self.formLayout.setWidget(row, QFormLayout.LabelRole, label)
        self.formLayout.setWidget(row, QFormLayout.FieldRole, lineLayers)

        return label

    def setupUi(self, RaVeN):
        if not RaVeN.objectName():
            RaVeN.setObjectName(u"RaVeN")

        height = 750
        RaVeN.resize(632, height + 50)
        self.buttonBox = QDialogButtonBox(RaVeN)
        self.buttonBox.setObjectName(u"buttonBox")
        self.buttonBox.setGeometry(QRect(270, height + 0, 341, 32))
        self.buttonBox.setOrientation(Qt.Horizontal)
        self.buttonBox.setStandardButtons(QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        self.formLayoutWidget = QWidget(RaVeN)
        self.formLayoutWidget.setObjectName(u"formLayoutWidget")
        self.formLayoutWidget.setGeometry(QRect(10, 10, 611, height - 30))
        self.formLayout = QFormLayout(self.formLayoutWidget)
        self.formLayout.setObjectName(u"formLayout")
        self.formLayout.setContentsMargins(0, 0, 0, 0)

        self.show_advanced = QCheckBox(RaVeN)
        self.show_advanced.setObjectName(u"show_advanced")
        self.show_advanced.setGeometry(QRect(50, height, 341, 32))
        self.show_advanced.setText("Show advanced parameters")

        ########
        # LAYERS
        ########

        self.lbl_title_layers = self.build_divider(RaVeN, 0)

        self.lbl_vector = QLabel(self.formLayoutWidget)
        self.lbl_vector.setObjectName(u"lbl_vector")


        self.inp_vector = QgsMapLayerComboBox(self.formLayoutWidget)
        self.inp_vector.setObjectName(u"inp_vector")
        self.inp_vector.setFilters(QgsMapLayerProxyModel.VectorLayer)

        self.formLayout.setWidget(1, QFormLayout.LabelRole, self.lbl_vector)
        self.formLayout.setWidget(1, QFormLayout.FieldRole, self.inp_vector)

        self.lbl_vector_field = QLabel(self.formLayoutWidget)
        self.lbl_vector_field.setObjectName(u"lbl_vector_field")

        self.inp_vector_field = QgsFieldComboBox(self.formLayoutWidget)
        self.inp_vector_field.setObjectName(u"inp_vector_field")

        if self.inp_vector.currentLayer():
            self.inp_vector_field.setLayer(self.inp_vector.currentLayer())

        self.inp_vector.layerChanged.connect(self.inp_vector_field.setLayer)

        self.formLayout.setWidget(2, QFormLayout.LabelRole, self.lbl_vector_field)
        self.formLayout.setWidget(2, QFormLayout.FieldRole, self.inp_vector_field)

        # self.lbl_where = QLabel(self.formLayoutWidget)
        # self.lbl_where.setObjectName(u"lbl_where")
        #
        # self.mdl_where = QgsQueryBuilder(self.inp_vector.currentLayer(), self.formLayoutWidget)
        #
        # self.inp_where = QPushButton(self.formLayoutWidget)
        # self.inp_where.clicked.connect(self.mdl_where.show)
        # self.inp_where.setObjectName(u"inp_where")
        #
        #
        # def update_where():
        #     self.inp_where.setText(self.mdl_where.sql())
        #
        # update_where()
        #
        # self.mdl_where.accepted.connect(update_where)
        #
        # self.formLayout.setWidget(3, QFormLayout.LabelRole, self.lbl_where)
        # self.formLayout.setWidget(3, QFormLayout.FieldRole, self.inp_where)

        self.lbl_raster = QLabel(self.formLayoutWidget)
        self.lbl_raster.setObjectName(u"lbl_raster")

        self.inp_raster = QgsMapLayerComboBox(self.formLayoutWidget)
        self.inp_raster.setObjectName(u"inp_raster")
        self.inp_raster.setFilters(QgsMapLayerProxyModel.RasterLayer)

        self.formLayout.setWidget(4, QFormLayout.LabelRole, self.lbl_raster)
        self.formLayout.setWidget(4, QFormLayout.FieldRole, self.inp_raster)

        self.lbl_system, self.inp_system = self.build_ccb("system",
                                                          [("Beast", False), ("PostGIS", True), ("Omnisci", False),
                                                           ("RasDaMan", False), ("Sedona", False)])
        self.formLayout.setWidget(5, QFormLayout.LabelRole, self.lbl_system)
        self.formLayout.setWidget(5, QFormLayout.FieldRole, self.inp_system)

        self.lbl_raster_agg, self.inp_raster_agg = self.build_ccb("raster_agg",
                                                                  [("count", False), ("sum", False), ("min", False),
                                                                   ("max", False), ("avg", True)])
        self.formLayout.setWidget(6, QFormLayout.LabelRole, self.lbl_raster_agg)
        self.formLayout.setWidget(6, QFormLayout.FieldRole, self.inp_raster_agg)

        ########
        # Benchmark properties
        ########
        self.formLayout.setItem(9, QFormLayout.LabelRole, QSpacerItem(40, 40, QSizePolicy.Minimum, QSizePolicy.Minimum))
        self.lbl_title_bench = self.build_divider(RaVeN, 10)

        self.lbl_vec_type, self.inp_vec_type = self.build_ccb("vec_type", [("polygons", False), ("points", True)])
        self.formLayout.setWidget(11, QFormLayout.LabelRole, self.lbl_vec_type)
        self.formLayout.setWidget(11, QFormLayout.FieldRole, self.inp_vec_type)

        self.lbl_raster_clip, self.inp_raster_clip = self.build_ccb("raster_clip", [("Yes", True), ("No", False)])

        self.formLayout.setWidget(12, QFormLayout.LabelRole, self.lbl_raster_clip)
        self.formLayout.setWidget(12, QFormLayout.FieldRole, self.inp_raster_clip)

        self.lbl_vector_filter_stage, self.inp_vector_filter_stage = self.build_ccb("vector_filter_stage",
                                                                                    [("Preprocess", True),
                                                                                     ("Execution", False)])

        self.formLayout.setWidget(13, QFormLayout.FieldRole, self.inp_vector_filter_stage)
        self.formLayout.setWidget(13, QFormLayout.LabelRole, self.lbl_vector_filter_stage)

        self.lbl_align_crs_at, self.inp_align_crs_at = self.build_ccb("align_crs_at",
                                                                      [("Preprocess", True), ("Execution", False)])

        self.formLayout.setWidget(14, QFormLayout.LabelRole, self.lbl_align_crs_at)
        self.formLayout.setWidget(14, QFormLayout.FieldRole, self.inp_align_crs_at)

        self.lbl_raster_tile_size = QLabel(self.formLayoutWidget)
        self.lbl_raster_tile_size.setObjectName(u"lbl_raster_tile_size")

        self.mdl_raster_tile_size = EditTileTableModel()

        self.inp_raster_tile_size = QTableView(self.formLayoutWidget)
        self.inp_raster_tile_size.setObjectName(u"inp_raster_tile_size")
        self.inp_raster_tile_size.setShowGrid(True)
        self.inp_raster_tile_size.setModel(self.mdl_raster_tile_size)
        self.inp_raster_tile_size.horizontalHeader().setVisible(True)
        self.inp_raster_tile_size.setMaximumHeight(100)
        self.inp_raster_tile_size.setMinimumHeight(100)

        self.inp_add_tile_col = QPushButton(self.formLayoutWidget)
        self.inp_add_tile_col.setObjectName(u"inp_add_tile_col")
        self.inp_add_tile_col.clicked.connect(lambda _: self.mdl_raster_tile_size.insertRow(0))
        self.inp_add_tile_col.setText("Add")

        tilebox = QHBoxLayout()
        tilebox.addWidget(self.inp_raster_tile_size)
        tilebox.addWidget(self.inp_add_tile_col)

        self.formLayout.setWidget(15, QFormLayout.LabelRole, self.lbl_raster_tile_size)
        self.formLayout.setLayout(15, QFormLayout.FieldRole, tilebox)

        self.lbl_vector_simplify = QLabel(self.formLayoutWidget)
        self.lbl_vector_simplify.setObjectName(u"lbl_vector_simplify")

        self.inp_vector_simplify = EditableListWidget()
        self.inp_vector_simplify.addItem(QListWidgetItem("1"))

        def add_simplify_vec():
            self.inp_vector_simplify.closeIfPersistentEditorStillOpen()  # You have to call this.
            item = QListWidgetItem('')
            self.inp_vector_simplify.addItem(item)

        self.inp_add_vector_simplify = QPushButton()
        self.inp_add_vector_simplify.setObjectName(u"inp_add_vector_simplify")
        self.inp_add_vector_simplify.clicked.connect(add_simplify_vec)
        self.inp_add_vector_simplify.setText("Add")

        vecsimplifybox = QHBoxLayout()
        vecsimplifybox.addWidget(self.inp_vector_simplify)
        vecsimplifybox.addWidget(self.inp_add_vector_simplify)

        self.wig_vecsimplify = QWidget(self.formLayoutWidget)
        self.wig_vecsimplify.setLayout(vecsimplifybox)

        self.formLayout.setWidget(16, QFormLayout.LabelRole, self.lbl_vector_simplify)
        self.formLayout.setWidget(16, QFormLayout.FieldRole, self.wig_vecsimplify)

        self.lbl_raster_downsample = QLabel(self.formLayoutWidget)
        self.lbl_raster_downsample.setObjectName(u"lbl_raster_downsample")

        self.inp_raster_downsample = EditableListWidget()
        self.inp_raster_downsample.addItem(QListWidgetItem("0"))

        def add_downsample_ras():
            self.inp_raster_downsample.closeIfPersistentEditorStillOpen()  # You have to call this.
            item = QListWidgetItem('')
            self.inp_raster_downsample.addItem(item)

        self.inp_add_raster_downsample = QPushButton()
        self.inp_add_raster_downsample.setObjectName(u"inp_add_raster_downsample")
        self.inp_add_raster_downsample.clicked.connect(add_downsample_ras)
        self.inp_add_raster_downsample.setText("Add")

        rasdownsamplebox = QHBoxLayout()
        rasdownsamplebox.addWidget(self.inp_raster_downsample)
        rasdownsamplebox.addWidget(self.inp_add_raster_downsample)

        self.wig_rasdownsample = QWidget(self.formLayoutWidget)
        self.wig_rasdownsample.setLayout(rasdownsamplebox)

        self.formLayout.setWidget(17, QFormLayout.LabelRole, self.lbl_raster_downsample)
        self.formLayout.setWidget(17, QFormLayout.FieldRole, self.wig_rasdownsample)

        ########
        # General Properties
        ########

        self.formLayout.setItem(19, QFormLayout.LabelRole, QSpacerItem(40, 40, QSizePolicy.Minimum, QSizePolicy.Minimum))
        self.lbl_title_general = self.build_divider(RaVeN, 20)

        self.lbl_iterations = QLabel(self.formLayoutWidget)
        self.lbl_iterations.setObjectName(u"lbl_iterations")

        self.inp_iterations = QSpinBox(self.formLayoutWidget)
        self.inp_iterations.setObjectName(u"inp_iterations")
        self.inp_iterations.setMinimum(1)
        self.inp_iterations.setValue(1)

        self.formLayout.setWidget(21, QFormLayout.LabelRole, self.lbl_iterations)
        self.formLayout.setWidget(21, QFormLayout.FieldRole, self.inp_iterations)

        self.lbl_warm_starts = QLabel(self.formLayoutWidget)
        self.lbl_warm_starts.setObjectName(u"lbl_warm_starts")

        self.inp_warm_starts = QSpinBox(self.formLayoutWidget)
        self.inp_warm_starts.setObjectName(u"inp_warm_starts")
        self.inp_warm_starts.setMinimum(0)
        self.inp_warm_starts.setValue(0)

        self.formLayout.setWidget(22, QFormLayout.LabelRole, self.lbl_warm_starts)
        self.formLayout.setWidget(22, QFormLayout.FieldRole, self.inp_warm_starts)

        self.lbl_timeout = QLabel(self.formLayoutWidget)
        self.lbl_timeout.setObjectName(u"lbl_timeout")

        self.inp_timeout = QSpinBox(self.formLayoutWidget)
        self.inp_timeout.setObjectName(u"inp_timeout")
        self.inp_timeout.setMinimum(10)
        self.inp_timeout.setMaximum(2147483647)
        self.inp_timeout.setValue(10000)
        self.inp_timeout.setSingleStep(100)

        self.formLayout.setWidget(23, QFormLayout.LabelRole, self.lbl_timeout)
        self.formLayout.setWidget(23, QFormLayout.FieldRole, self.inp_timeout)
        self.retranslateUi(RaVeN)

        self.buttonBox.accepted.connect(RaVeN.accept)
        self.buttonBox.rejected.connect(RaVeN.reject)

        def show_advanced():
            checked = self.show_advanced.checkState()
            self.wig_vecsimplify.setVisible(checked)
            self.wig_rasdownsample.setVisible(checked)
            self.lbl_raster_downsample.setVisible(checked)
            self.lbl_vector_simplify.setVisible(checked)

        self.show_advanced.stateChanged.connect(show_advanced)

        show_advanced()

        QMetaObject.connectSlotsByName(RaVeN)

    # setupUi

    def retranslateUi(self, RaVeN):
        RaVeN.setWindowTitle(QCoreApplication.translate("RaVeN", u"RaVeN", None))
        self.lbl_vector.setText(QCoreApplication.translate("RaVeN", u"Vector", None))
        self.lbl_vector_field.setText(QCoreApplication.translate("RaVeN", u"Vector Field", None))
        # self.lbl_where.setText(QCoreApplication.translate("RaVeN", u"Filter predicates", None))
        self.lbl_raster.setText(QCoreApplication.translate("RaVeN", u"Raster", None))
        self.lbl_raster_agg.setText(QCoreApplication.translate("RaVeN", u"Aggregations", None))
        self.lbl_system.setText(QCoreApplication.translate("RaVeN", u"System", None))

        self.lbl_vec_type.setText(QCoreApplication.translate("RaVeN", u"Vectorization Type", None))
        self.lbl_vector_filter_stage.setText(
            QCoreApplication.translate("RaVeN", u"Apply Vector Selection at Stage", None))
        self.lbl_vector_simplify.setText(QCoreApplication.translate("RaVeN", u"Simplify Vector"))
        self.lbl_raster_clip.setText(QCoreApplication.translate("RaVeN", u"Clip Raster to Vector?", None))
        self.lbl_raster_tile_size.setText(QCoreApplication.translate("RaVeN", u"Raster Tile Size", None))
        self.lbl_raster_downsample.setText(QCoreApplication.translate("RaVeN", u"Downsample Raster"))
        self.lbl_align_crs_at.setText(QCoreApplication.translate("RaVeN", u"Align CRS at Stage", None))
        self.lbl_iterations.setText(QCoreApplication.translate("RaVeN", u"Iterations", None))
        self.lbl_warm_starts.setText(QCoreApplication.translate("RaVeN", u"Warm Starts", None))
        self.lbl_timeout.setText(QCoreApplication.translate("RaVeN", u"Timeout", None))

        self.lbl_title_layers.setText(QCoreApplication.translate("RaVeN", u"Layers", None))
        self.lbl_title_bench.setText(QCoreApplication.translate("RaVeN", u"Processing Parameters", None))
        self.lbl_title_general.setText(QCoreApplication.translate("RaVeN", u"General Parameters", None))
        # retranslateUi
