from PyQt5.QtWidgets import QDialog

from hub.gui.browser_window import Ui_Web


class WebDialog(QDialog):
    def __init__(self, iface, window_name=None, parent=None):
        super().__init__(parent)
        self.iface = iface
        # Create an instance of the GUI
        self.ui = Ui_Web(window_name)
        # Run the .setupUi() method to show the GUI
        self.setModal(False)
        self.ui.setupUi(self)
