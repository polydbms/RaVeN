# Capabilities

Not all parameters set in the [Workload Definition](workload-definition.md) may be applicable to every system defined that can be controlled by Benchi. Packaged within the utility is therefore a `capabilites.yaml` file that defines limits of the different systems that may be tested. Each capability is defined as a key, with systems being assigned to a capability using a list.

The `capabilities.yaml`:

```yaml
systems:
  rasterize:               # systems designed to only use raster data
    - rasdaman
  vectorize:               # systems designed to only use vector data
    - omnisci
    - sedona
  same_crs:                # whether the CRS of the datasets need to be aligned explicitly
    - postgis
    - sedona
  no_st_transform:         # systems that do not support st_transform
    - omnisci
  variable_tile_size:      # systems that support manually setting the internal tile size of the raster data
    - postgis
  ingest_raster_tiff_only: # systems that require raster data to be applied as a GeoTIFF file
    - beast
  require_geotiff_ending:  # systems that require the file endings of GeoTIFFs to be `.geotiff` 
    - beast
  pixels_as_points:        # systems that always assume pixels are points
    - beast
    - postgis
  pixels_as_polygons:      # systems that always assume pixels as polygons
    - rasdaman
```

The factory also takes into account the different quirks and requirements the systems need in order to function properly. For example, HeavyAI and Sedona both require vectorization of the raster data prior to execution. This detail is noted in the capabilities definition of Benchi, which is queried by the factory when loading the parameters. This capabilities document also holds information for other parts of the improvements.


