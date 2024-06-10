from PyQt5 import QtCore
from PyQt5.QtCore import QAbstractTableModel, Qt, QModelIndex

from hub.benchmarkrun.tilesize import TileSize


class EditTileTableModel(QAbstractTableModel):
    data: list[TileSize]

    def __init__(self):
        QAbstractTableModel.__init__(self)
        self.data = []
        self.headers = ["Width", "Height"]
        self.default_width = 100
        self.default_height = 100

    def flags(self, index):
        return QtCore.Qt.ItemIsEnabled | QtCore.Qt.ItemIsSelectable | QtCore.Qt.ItemIsEditable

    # def headerData(self, section, orientation, role=...):
    #     if orientation == Qt.Horizontal:
    #         return QVariant(self.headers[section])
    #     return None
    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self.headers[section])
            elif orientation == Qt.Vertical:
                return str(section + 1)
        return super().headerData(section, orientation, role)

    def rowCount(self, parent):
        return len(self.data)

    def columnCount(self, parent):
        return 2

    def data(self, index, role):
        row = self.data[index.row()]
        return row.width if index.column() == 0 else row.height

    def setData(self, index, value, role=QtCore.Qt.EditRole):
        if index.isValid():
            row = self.data[index.row()]
            if index.column() == 0:
                row.width = value
            else:
                row.height = value

            return True
        return False

    def insertRows(self, row, count, parent):
        # self.beginInsertRows(QModelIndex(), row, row + count)
        # self.data[row:row + count - 1].extend([TileSize(0, 0) for _ in range(count)])
        self.beginInsertRows(QModelIndex(), len(self.data), len(self.data))
        self.data.append(TileSize(self.default_width, self.default_height))
        self.endInsertRows()
        return True

    def all_data(self):
        return self.data
