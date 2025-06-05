from datetime import datetime
from pathlib import Path

from hub.zsresultsdb.submit_data import DuckDBConnector


class ControllerParameters:
    """
    The Host Parameters, vommon for all benchmark runs
    """
    _controller_result_folder: Path
    _controller_db_connection: DuckDBConnector | None
    _run_folder: str

    def __init__(self,
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
        self._controller_result_base_folder = Path(controller_result_folder).expanduser()
        self._controller_result_folder = self._controller_result_base_folder \
            .joinpath(self._run_folder)
        self._controller_result_db = Path(controller_result_db)
        self._controller_db_connection = None

    def connect_db(self):
        self._controller_db_connection = DuckDBConnector(db_filename=self._controller_result_db)

    @property
    def db_path(self) -> Path:
        return Path(self._controller_result_db)

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

    def close_db(self):
        if self._controller_db_connection is not None:
            self._controller_db_connection.close_connection()

    @property
    def controller_db_connection(self):
        if self._controller_db_connection is None:
            self.connect_db()
        return self._controller_db_connection

    def __str__(self):
        return ", ".join([
            str(self._controller_result_folder)
        ])
