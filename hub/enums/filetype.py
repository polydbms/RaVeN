from enum import Enum


class FileType(Enum):
    """
    the type of file or folder a path has
    """
    FILE = 1
    FOLDER = 2
    ZIP_ARCHIVE = 3
