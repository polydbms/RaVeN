from enum import Enum


class System(Enum):
    POSTGIS = "postgis"
    SEDONA = "sedona"
    SEDONA_VEC = "sedona-vec"
    BEAST = "beast"
    RASDAMAN = "rasdaman"
    HEAVYAI = "omnisci"
    POSTGIS_VEC = "postgis-vec"

    @property
    def name(self):
        return self.value

    def __str__(self):
        return self.value

    @staticmethod
    def get_by_value(value: str):
        return {v.value: v for v in list(System)}.get(value.lower())
