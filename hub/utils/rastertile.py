from dataclasses import dataclass

from shapely import Polygon


@dataclass
class Tile:
    name: str
    extent: Polygon
    base_id: str
    is_relevant: bool = True
    is_preprocessed: bool = False
    is_ingested: bool = False
    is_merged: bool = False
