from enum import Enum


class Stage(Enum):
    PREPROCESS = "preprocess"
    INGEST = "ingest"
    EXECUTE = "execute"

    @staticmethod
    def get_by_value(stage):
        return {s.value: s for s in list(Stage)}.get(stage)
