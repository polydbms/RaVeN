class TileSize:
    """
    Contains the tile size
    """
    width: int
    height: int

    def __init__(self, width, height):
        """

        :param width: the width
        :param height: the height
        """
        self.width = width
        self.height = height

    @property
    def postgis_str(self):
        """
        the string used for the postgis ingestion stage. if the tile size is 0x0, 'auto' is returned
        :return:
        """
        return f"{self.width}x{self.height}" if self.width > 0 and self.height > 0 else "auto"

    def __str__(self):
        return self.postgis_str
