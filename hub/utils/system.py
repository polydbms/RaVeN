from pathlib import Path
from typing import Tuple, Any


class System:
    _ssh_connection: str
    _public_key_path: Path
    _name: str

    def __init__(self, name: str, public_key_path: str, ssh_connection: str):
        self._name = name
        self._public_key_path = Path(public_key_path)
        self._ssh_connection = ssh_connection

    @property
    def name(self):
        return self._name

    @property
    def public_key_path(self):
        return self._public_key_path

    @property
    def ssh_connection(self):
        return self._ssh_connection

    def __str__(self):
        return self._name
