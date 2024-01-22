# -*- coding: utf-8 -*-

################################################################################
## Form generated from reading UI file 'designerbiuduK.ui'
##
## Created by: Qt User Interface Compiler version 5.15.8
##
## WARNING! All changes made in this file will be lost when recompiling UI file!
################################################################################

from PyQt5.QtCore import *  # type: ignore
from PyQt5.QtGui import *  # type: ignore
from PyQt5.QtWidgets import *  # type: ignore


class Ui_Web(object):

    def __init__(self):
        self.tabs = {}

    def setupUi(self, Dialog):
        if not Dialog.objectName():
            Dialog.setObjectName(u"RaVeN Viewer")
        Dialog.resize(647, 692)
        # self.buttonBox = QDialogButtonBox(Dialog)
        # self.buttonBox.setObjectName(u"buttonBox")
        # self.buttonBox.setGeometry(QRect(200, 530, 341, 32))
        # self.buttonBox.setOrientation(Qt.Horizontal)
        # self.buttonBox.setStandardButtons(QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        layout = QVBoxLayout(Dialog)
        self.tabWidget = QTabWidget()
        self.tabWidget.setObjectName(u"tabWidget")
        # self.tabWidget.setMinimumWidth(400)
        # self.tabWidget.setMinimumHeight(400)
        self.tabWidget.setContentsMargins(0, 0, 0, 0)
        # self.tabWidget.setGeometry(QRect(10, 10, 521, 501))
        self.add_tab("End-to-End Runtime Overview", "/home/gereon/git/dima/vldb-benchi/figures/introplot-usg.png")
        self.add_tab("Phase Breakdown", "/home/gereon/git/dima/vldb-benchi/figures/breakdown_amtrak.png")

        layout.addWidget(self.tabWidget)
        self.retranslateUi(Dialog)
        # self.buttonBox.accepted.connect(Dialog.accept)
        # self.buttonBox.rejected.connect(Dialog.reject)
        #
        # QMetaObject.connectSlotsByName(Dialog)

    # setupUi

    def retranslateUi(self, Dialog):
        Dialog.setWindowTitle(QCoreApplication.translate("RaVeN Viewer", u"RaVeN Viewer", None))

    # retranslateUi

    def add_tab(self, title, img_url):
        tab = ImageTab(title, img_url, self.tabWidget)
        tab.setContentsMargins(0, 0, 0, 0)

        self.tabWidget.addTab(tab, "")
        self.tabWidget.setTabText(self.tabWidget.indexOf(tab),
                                  QCoreApplication.translate("RaVeN Viewer", title, None))
        self.tabs[title] = tab


class ImageTab(QWidget):
    def __init__(self, title, img_url, parent=None):
        QWidget.__init__(self, parent)

        self.setObjectName(f"tab_{title}")

        layout = QVBoxLayout()
        self.setLayout(layout)

        self.label = QLabel(self)
        self.label.setObjectName(f"lab_{title}")
        self.label.setContentsMargins(0, 0, 0, 0)

        # loading image
        self.pixmap = QPixmap(img_url)

        # adding image to label
        self.label.setPixmap(self.pixmap.scaled(parent.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation))
        layout.addWidget(self.label)
        self.setLayout(layout)
        self.label.setScaledContents(True)
        self.label.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Ignored   )

        # Optional, resize label to image size
        # self.label.resize(parent.width(), parent.height())
