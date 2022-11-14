from pathlib import Path

import duckdb
from duckdb import DuckDBPyConnection


class DuckDBConnector:
    _connection: DuckDBPyConnection

    def __init__(self, db_filename: Path):
        # self._connection = duckdb.connect(database=str(db_filename), read_only=False)
        self._exp_id = 0
        self._is_initialized = False

    def get_cursor(self) -> DuckDBPyConnection:
        return self._connection.cursor()

    def initialize_run(self):

        self._is_initialized = True
        pass

    def write_timings_marker(self, marker: str):
        if not self._is_initialized:
            raise Exception("experiment has not been initialized")
        pass

    # def __del__(self):
    #     self._connection.close()
