class TileSize:
    width: int
    height: int

    def __init__(self, width, height):
        self.width = width
        self.height = height

    @property
    def postgis_str(self):
        return f"{self.width}x{self.height}" if self.width > 0 and self.height > 0 else "auto"

    def __str__(self):
        return self.postgis_str
