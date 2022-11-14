from pathlib import Path
from typing import Tuple, Any

from duckdb import DuckDBPyConnection

from hub.duckdb.submit_data import DuckDBConnector


class System:
    _results_db_conn: DuckDBConnector
    _controller_result_folder: Path
    _ssh_connection: str
    _public_key_path: Path
    _name: str
    _host_base_path: Path

    def __init__(self, name: str, public_key_path: str, ssh_connection: str, host_base_path: str,
                 controller_result_folder: str, results_db_conn: DuckDBConnector):
        self._name = name
        self._public_key_path = Path(public_key_path)
        self._ssh_connection = ssh_connection
        self._host_base_path = Path(host_base_path)
        self._controller_result_folder = Path(controller_result_folder).expanduser()
        self._results_db_conn = results_db_conn

    @property
    def name(self):
        return self._name

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
    def controller_result_folder(self) -> Path:
        return self._controller_result_folder

    @property
    def db_connector(self) -> DuckDBConnector:
        return self._results_db_conn

    # @property
    # def connection(self) -> DuckDBPyConnection:
    #     return self._results_db_conn
    #
    # @property
    # def cursor(self) -> DuckDBPyConnection:
    #     return self._results_db_conn.cursor()
    #
    # def close_db(self):
    #     self._results_db_conn.close()

    def __str__(self):
        return self._name
