from datetime import datetime
from pathlib import Path


class HostParameters:
    _host_base_path: Path
    _controller_result_folder: Path
    _ssh_connection: str
    _public_key_path: Path
    _run_folder: str

    def __init__(self,
                 ssh_connection: str,
                 public_key_path: str,
                 host_base_path: Path,
                 controller_result_folder: str):
        self._run_folder = f"run_{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        self._public_key_path = Path(public_key_path)
        self._ssh_connection = ssh_connection
        self._host_base_path = host_base_path
        self._controller_result_base_folder = Path(controller_result_folder).expanduser()
        self._controller_result_folder = self._controller_result_base_folder\
            .joinpath(self._run_folder)

    @property
    def public_key_path(self):
        return self._public_key_path

    @property
    def ssh_connection(self):
        return self._ssh_connection

    @property
    def host_base_path(self) -> Path:
        return self._host_base_path

    @property
    def controller_result_base_folder(self) -> Path:
        return self._controller_result_base_folder

    @property
    def controller_result_folder(self) -> Path:
        return self._controller_result_folder

    @property
    def run_folder(self):
        return self._run_folder

    def __str__(self):
        return ", ".join([
            str(self._public_key_path),
            str(self._ssh_connection),
            str(self._host_base_path),
            str(self._controller_result_folder)
        ])

