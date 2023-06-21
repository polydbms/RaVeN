class System:
    """
    data class representing a system
    """
    _name: str
    _port: int

    def __init__(self, name: str, port: int):
        self._name = name
        self._port = port

    @property
    def name(self):
        return self._name

    @property
    def port(self):
        return self._port

    def __str__(self):
        return self._name
