from enum import Enum


class System(Enum):
    POSTGIS = "postgis"
    SEDONA = "sedona"
    SEDONA_VEC = "sedona-vec"
    BEAST = "beast"
    RASDAMAN = "rasdaman"
    HEAVYAI = "omnisci"

    @property
    def name(self):
        return self.value

    def __str__(self):
        return self.value
