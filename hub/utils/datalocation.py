from enum import Enum
from pathlib import Path
import zipfile

from hub.utils.system import System


class DataType(Enum):
    RASTER = 1
    VECTOR = 2


class FileType(Enum):
    FILE = 1
    FOLDER = 2
    ZIP_ARCHIVE = 3


class DataLocation:
    _system: System
    _host_base_dir: Path
    _file: Path
    _data_type: DataType
    _name: str
    _preprocessed: bool
    _controller_location: Path

    def __init__(self, path: str, data_type: DataType, host_base_dir: Path, system: System, name: str = None) -> None:
        self._controller_location = Path(path).expanduser()
        self._host_base_dir = host_base_dir

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
        self._file = self._find_file(["*.shp"] if self._data_type == DataType.VECTOR else ["*.tif*", "*.jp2"])
        self._system = system

        self._host_dir = self._host_base_dir.joinpath("data").joinpath(self._name)
        self._docker_dir = Path("/data").joinpath(Path(self._name))

    def _find_file(self, ending: [str]) -> Path:
        if self.type == FileType.FILE:
            raise NotImplementedError("Single Files are currently not supported")
        elif self.type == FileType.FOLDER:
            for e in ending:
                files = [f for f in self._controller_location.glob(e)]
                if len(files) > 0:
                    return Path(files[0].name)
        elif self.type == FileType.ZIP_ARCHIVE:
            return Path(
                [f for f in zipfile.Path(self._controller_location).iterdir() if Path(f.name).match(ending)][0].name)
        else:
            raise FileNotFoundError(f"Could not find file with ending {ending} at/in {self}")

    @property
    def docker_dir(self) -> Path:
        return self._docker_dir if not self._preprocessed else self.docker_dir_preprocessed

    @property
    def docker_file(self) -> Path:
        return self.docker_dir.joinpath(self._file)

    @property
    def docker_file_preprocessed(self) -> Path:
        return self.docker_dir_preprocessed.joinpath(self._file)

    @property
    def docker_wkt(self) -> Path:
        return self.docker_file_preprocessed.with_suffix(".json")

    @property
    def docker_dir_preprocessed(self) -> Path:
        return self._docker_dir.joinpath(f"preprocessed_{self._system.name}")

    @property
    def host_dir(self) -> Path:
        return self._host_dir if not self._preprocessed else self.host_dir_preprocessed

    @property
    def host_file(self) -> Path:
        return self.host_dir.joinpath(self._file)

    @property
    def host_file_preprocessed(self) -> Path:
        return self.host_dir_preprocessed.joinpath(self._file)

    @property
    def host_wkt(self) -> Path:
        return self.host_file_preprocessed.with_suffix(".json")

    @property
    def host_dir_preprocessed(self) -> Path:
        return self._host_dir.joinpath(f"preprocessed_{self._system.name}")

    @property
    def name(self) -> str:
        return self._name

    @property
    def controller_location(self) -> Path:
        return self._controller_location

    @property
    def controller_file(self) -> Path:
        return self._controller_location.joinpath(self._file)

    @property
    def controller_wkt(self) -> Path:
        return self.controller_location.with_suffix(".json")

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
