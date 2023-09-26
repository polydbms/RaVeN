# Quirks and Limitations

During implementation, a set of non-straight-forward design choices have been made.

## Rasdaman and Rasterization

Rasdaman is the only raster-based system currently implemented. Due to its query structure though, it does not strictly require rasterization of the vector files in order to compute zonal statistics. Instead, the connector controlling Rasdaman uses single queries, in which the `ST_Clip` operation selects a limited set of pixels based on a vector-based WKT string. Thus, the preprocessing utility does not support rasterization of vector files at the moment. Instead, it creates, transfers and uses a JSON-file containing WKT strings, which is not fully in line with the remaining processes.

## Beast and Tile Sizes

Looking at its source code, Beast employs some tile size to organize the raster files internally. Unfortunately, an API for manipulating the tile size is not exposed. It is therefore not possible to set the tile size of beast using Benchi at the moment.


## Beast, Points and Pixels

The interpretation of raster pixels in Beast depends on the type of the vector features (Point, LineString, Polygon). This is not exactly mirrored in the database of Benchi, as Beast always is registered as using a `TO_POINTS` interpretation of the pixels. However, as the interpretation is fixed to the vector type, the user is advised to ignore this database entry, or only use it knowing it may not be entirely correct.


## Raster Bands

Currently, Benchi does not support join operations on multiple raster bands. If a raster image contains more than a single band, Benchi should always chopose the first one for any operation.

## Resource Utilization Runaways

Benchi logs the resource utilization of the different containers by running `docker stats` in a forever loop. This loop is interrupted once a benchmark run has been completed. If benchi does not exit gracefully, the docker stats-loop may therefore still run in the background, even if everything else has already been stopped. It is therefore advised to check the host for any running docker stats-loops from time to time, and especially when developing either Benchi or a workload definition. 