from datetime import datetime
from pathlib import Path

from hub.duckdb.submit_data import DuckDBConnector


class HostParameters:
    """
    The Host Parameters, vommon for all benchmark runs
    """
    _host_base_path: Path
    _controller_result_folder: Path
    _controller_db_connection: DuckDBConnector
    _ssh_connection: str
    _public_key_path: Path
    _run_folder: str

    def __init__(self,
                 ssh_connection: str,
                 public_key_path: str,
                 host_base_path: Path,
                 controller_result_folder: str,
                 controller_result_db: Path):
        """

        :param ssh_connection: the ssh connection base string
        :param public_key_path: the path tho the public ssh key
        :param host_base_path: the path where all files shall reside on the host
        :param controller_result_folder: the path where result files shall be put on the controller, later extended by a run-specific folder to group results
        :param controller_result_db: the path where the results database is located
        """
        self._run_folder = f"run_{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        self._public_key_path = Path(public_key_path)
        self._ssh_connection = ssh_connection
        self._host_base_path = host_base_path
        self._controller_result_base_folder = Path(controller_result_folder).expanduser()
        self._controller_result_folder = self._controller_result_base_folder \
            .joinpath(self._run_folder)
        self._controller_db_connection = DuckDBConnector(db_filename=controller_result_db)

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
        """

        :return: the folder where all result-folders are located
        """
        return self._controller_result_base_folder

    @property
    def controller_result_folder(self) -> Path:
        """

        :return: the run-specific result folder
        """
        return self._controller_result_folder

    @property
    def run_folder(self):
        """

        :return: the run-specific output folder
        """
        return self._run_folder

    @property
    def controller_db_connection(self):
        return self._controller_db_connection

    def __str__(self):
        return ", ".join([
            str(self._public_key_path),
            str(self._ssh_connection),
            str(self._host_base_path),
            str(self._controller_result_folder)
        ])
