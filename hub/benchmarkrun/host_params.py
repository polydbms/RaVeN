from datetime import datetime
from pathlib import Path

from hub.benchmarkrun.controller_params import ControllerParameters
from hub.zsresultsdb.submit_data import DuckDBConnector


class HostParameters:
    """
    The Host Parameters, vommon for all benchmark runs
    """
    _host_base_path: Path
    _ssh_connection: str
    _ssh_config_path: Path
    _run_folder: str

    def __init__(self,
                 ssh_connection: str,
                 ssh_config_path: str,
                 host_base_path: Path,
                 controller_params: ControllerParameters):
        """

        :param ssh_connection: the ssh connection base string
        :param public_key_path: the path tho the public ssh key
        :param host_base_path: the path where all files shall reside on the host
        :param controller_result_folder: the path where result files shall be put on the controller, later extended by a run-specific folder to group results
        :param controller_result_db: the path where the results database is located
        """
        self.controller_params = controller_params
        self._run_folder = controller_params.run_folder
        self._ssh_config_path = Path(ssh_config_path)
        self._ssh_connection = ssh_connection
        self._host_base_path = host_base_path

    @property
    def ssh_config_path(self):
        return self._ssh_config_path

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
        return self.controller_params._controller_result_base_folder

    @property
    def controller_result_folder(self) -> Path:
        """

        :return: the run-specific result folder
        """
        return self.controller_params._controller_result_folder

    @property
    def run_folder(self):
        """

        :return: the run-specific output folder
        """
        return self._run_folder

    # FIXME eventually remove duplicated code with ControllerParameters

    def close_db(self):
        if self.controller_params.controller_db_connection is not None:
            self.controller_params.controller_db_connection.close_connection()

    @property
    def controller_db_connection(self):
        if self.controller_params.controller_db_connection is None:
            self.controller_params.connect_db()
        return self.controller_params.controller_db_connection

    def __str__(self):
        return ", ".join([
            str(self._ssh_config_path),
            str(self._ssh_connection),
            str(self._host_base_path),
            str(self.controller_params),
        ])
