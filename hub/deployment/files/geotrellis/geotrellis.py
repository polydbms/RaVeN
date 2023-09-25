import json
from functools import partial

import geopyspark as gps
from pyspark import SparkContext

from shapely.geometry import Polygon, shape, Point
from shapely.ops import transform
import pyproj

# Create the GeoPyContext
conf = gps.geopyspark_conf(master="local[*]", appName="geotrellis-bench")
pysc = SparkContext(conf=conf)

# Read in the NLCD tif that has been saved locally.
# This tif represents the state of Pennsylvania.
raster_rdd = gps.geotiff.get(layer_type=gps.LayerType.SPATIAL,
                             uri='file:///data/PRISM_ppt_stable_4kmD2_20210901/PRISM_ppt_stable_4kmD2_20210901.tiff')

tiled_rdd = raster_rdd.to_tiled_layer()

# Reproject the reclassified TiledRasterRDD so that it is in WebMercator
# reprojected_rdd = tiled_rdd.reproject(3857, scheme=ZOOM).cache().repartition(150)

# We will do a polygonal summary of the north-west region of Philadelphia.
# with open('/tmp/area_of_interest.json') as f:
#     txt = json.load(f)
#
# geom = shape(txt['features'][0]['geometry'])

# We need to reporject the geometry to WebMercator so that it will intersect with
# the TiledRasterRDD.
# project = partial(
#     pyproj.transform,
#     pyproj.Proj(init='epsg:4326'),
#     pyproj.Proj(init='epsg:3857'))

# area_of_interest = transform(project, geom)

# Find the min and max of the values within the area of interest polygon.
min_val = tiled_rdd.polygonal_min(geometry=Polygon(shell=[(-86, 37), (-85, 34), (-83, 36)]), data_type=int)

print('The min value of the area of interest is:', min_val)
# print('The max value of the area of interest is:', max_val)
