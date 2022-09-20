from enum import Enum
from pathlib import Path
import zipfile


class DataType(Enum):
    RASTER = 1
    VECTOR = 2


class FileType(Enum):
    FILE = 1
    FOLDER = 2
    ZIP_ARCHIVE = 3


class DataLocation:
    _file: Path
    _data_type: DataType
    _name: str
    _preprocessed: bool
    _controller_location: Path

    def __init__(self, path: str, data_type: DataType, name: str = None) -> None:
        self._controller_location = Path(path).expanduser()

        if not self._controller_location.exists():
            raise FileNotFoundError(f"Path {path} to input for {data_type} data does not exist")

        if self._controller_location.is_dir():
            self.type = FileType.FOLDER
        elif zipfile.is_zipfile(self._controller_location):
            self.type = FileType.ZIP_ARCHIVE
        else:
            self.type = FileType.FILE

        self._name = self._controller_location.stem if name is None else name
        self._preprocessed = False
        self._data_type = data_type
        self._file = self._find_file("*.shp" if self._data_type == DataType.VECTOR else "*.tif*")

    def _find_file(self, ending: str) -> Path:
        if self.type == FileType.FILE:
            raise NotImplementedError("Single Files are currently not supported")
        elif self.type == FileType.FOLDER:
            return Path([f for f in self._controller_location.glob(ending)][0].name)
        elif self.type == FileType.ZIP_ARCHIVE:
            return Path([f for f in zipfile.Path(self._controller_location).iterdir() if Path(f.name).match(ending)][0].name)
        else:
            raise FileNotFoundError(f"Could not find file with ending {ending} at/in {self}")

    @property
    def docker_dir(self) -> Path:
        return Path("/data").joinpath(Path(self._name))

    @property
    def docker_file(self) -> Path:
        return self.docker_dir.joinpath(self._file)

    @property
    def host_dir(self) -> Path:
        return Path("~/data").joinpath(Path(self._name))

    @property
    def host_file(self) -> Path:
        return self.host_dir.joinpath(self._file)

    @property
    def name(self) -> str:
        return self._name

    @property
    def controller_location(self) -> str:
        return str(self._controller_location)

    def set_preprocessed(self):
        self._preprocessed = True

    @property
    def preprocessed(self) -> bool:
        return self._preprocessed

    def __str__(self):
        return ",".join(
            [str(self._controller_location),
             str(self._data_type),
             str(self._file),
             self._name,
             str(self._preprocessed)]
        )
