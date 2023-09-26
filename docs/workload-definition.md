# Workload Definition

We designed the workload definition such that it can be divided into four parts: It first describes the location folders of the datasets, secondly contains the structure of the query, next the parameters to set or change, and last the systems to test for. All four parts are wrapped into a single `experiments` obejct.

## Dataset locations

Defining the dataset locations is straight forward: In the given field, simply supply one folder each for the raster and vector dataset. These folders then contain the dataset files themselves. While looking like an overhead for self-contained files like `.geojson` or `.geotiff`, this allows Benchi to easily handle multi-file datasets as they occur in *ESRI Shapefiles*.

```yaml
  data:
    raster: /data/glc/GlcShare_v10_Dominant
    vector: /data/us_state
```


## Query

As Benchi is capable to test a plethora of systems which do not support a common query language, we cannot simply define our query using SQL or a similar query language. Instead, we decided to define a benchmark-specific query language that can be translated effortlessly into other query languages. As the focus of Benchi is to only combine one vector and raster dataset each, our domain-specific query language only needs to support a subset of possible queries. The user therefore is limited to defining attributes for the following query parameters: `get` including aggregations, `where`, `group`, and `order`. Finally, for the join parameter the user has to define for the two input datasets, whether they contain vector or raster datasets. They also have to define a join condition, which uses a subset of spatial relations as possible conditions.

It is possible to aggregate on both raster and vector attributes. If an aggregation occurs, all non-aggregated attributes need to again occur in the `group` condition.

The value of a raster pixel is always called `sval`. Benchi currently can neither handle multiple raster values per pixel within one band, nor operations on multiple bands. 

The sample query part, wrapped inside the `workload` object:

```yaml
  workload:
    get: 
      vector:
        - AFFGEOID
      raster: 
        - sval
        - sval:
            aggregations:
              - count
    join:
      table1: vector
      table2: raster
      condition: intersect(raster, vector)
    group:
      vector: 
        - AFFGEOID
      raster: 
        - sval
    order:
      vector: 
        - AFFGEOID
      raster: 
        - sval
```

Here, the query is kept rather simple: If the system does not support a special join operator, we use an intersect join between the raster and vector dataset. For the vector file we then project on the IDs of the different vector features, while for the raster values we project and group by the values of the pixels and then count their frequency within the vector feature. Finally, we order the gathered results.

### Spatial Relations for Joins

Benchi currently supports three different join conditions.

#### `intersect`

When choosing `intersect`, Benchi will perform the join based on the `ST_Intersects` predicate.

#### `contains`

When choosing `contains`, Benchi will perform the join based on the `ST_Contains` predicate.


#### `bestrasvecjoin`

When choosing `bestrasvecjoin`, Benchi will perform the join based on a set of chained spatial predicates. The goal is to create a match only if the interior of two spatial objects overlaps. This is done to avoid any inaccuracies where two spatial objects may only connect on their boundaries. The 9IM for this predicate is as follows:

$$
bestrasvecjoin = 

\begin{pmatrix}
T & * & * \\
* & * & * \\
* & * & *
\end{pmatrix}
$$

At the moment, this predicate can only be used with Sedona due to limitations within the systems or their interpretation of pixels.

## Parameters

To facilitate easy benchmarking, Benchi also supports setting parameters that are tested automatically. Most of these parameters change how the datasets are ingested and are therefore handled by the [preprocessor](preprocessor.md). Most of these parameters take in arrays. Benchi then automatically creates all possible combinations and executes them as benchmark runs within a benchmark set (see [database](results-db.md)).

All possible parameters as well as some sample values:

```YAML
  parameters:
    raster_format:
      - geotiff
      - jp2
    rasterize_format:
      - geotiff
      - jp2
    raster_target_crs:
      - epsg:4326
      - epsg:25833
    raster_tile_size:
      - 1x100
      - 10x10
      - 100x1
      - auto
    raster_depth:
      - 1
      - 12
    raster_resolution:
      - 1
      - 0.5

    vector_format:
      - shp
      - geojson
    vectorize_format:
      - geojson
      - shp
    vectorize_type:
      - polygons
      - points
    vector_target_crs:
      - epsg:4326
    vector_resolution:
      - 0.8
      - 0.32

    align_to_crs: vector | raster | both # align vector crs to raster crs or vice versa or do 2 runs
    align_crs_at_stage:
      - preprocess
      - execution

  iterations: 2
  warm_starts: 3
  timeout: 10800
```

| Parameter | Value Range | Default | Description |  In use?
|--|--|--|--|--|
 |   raster_format| {`geotiff`, `jp2`} | input file format | The format of the raster file at the point of ingestion | Yes |
 |   rasterize_format| {`geotiff`,`jp2`} | `geotiff` | The format a vector file shall be rasterized to | [*somewhat*](quirks.md) |
 |   raster_target_crs| a valid EPSG code | input file CRS | The CRS to target | Yes |
 |   raster_tile_size| {$x \in \N^+$ x $y \in \N^+$, `auto`} | auto | The tile size within a system | Yes |
 |   raster_depth| $d \in \N^+$  | Full depth | The depth of a raster file in bits | No  |
 |   raster_resolution| $r \in (0, 1]$ | $1.0$ | The resolution of the raster file as a factor | No |
 |   | | | | |
 |   vector_format| {`shp`, `geojson`} | input file format | The format of the vector file at the point of ingestion | Yes |
 |   vectorize_format| {`shp`, `geojson`} | `shp` | The format a raster file shall be vectorized to | No |
 |   vectorize_type| {`points`, `polygons`} | `points` | The interpretation of raster pixels if vectorization takes place | Yes |
 |   vector_target_crs| a valid EPSG code | input file CRS |  The CRS to target | Yes |
 |   vector_resolution| $r \in (0, 1]$ | $1.0$ |  The resolution of the vector file as a factor | No |
| | | | | |
 |   align_to_crs| {`raster`, `vector`,`both`} | None or ` vector` | Which CRS of which dataset shall be aligned | Yes |
 |   align_crs_at_stage| {`preprocess`, `execution`} | `preprocess`| When to perform a CRS alignment | Yes |
| | | | | |
| iterations | $i \in \N^+$ | 1 | The number of iterations for each complete benchmark set | Yes |
 | warm_starts | $w \in \N$ | 3 | The number of warm starts | Yes |
 | timeout | $t \in \N$ | 10800 sec | When a query shall time out during the execution phase |





## Benchmark Parameters

The user has the option to define a set of benchmark parameters. These parameters are used by the `BenchmarkRunFactory` to create consecutive benchmark runs that automatically test the specified parameter combinations. Modifiable parameters include those affecting file types the datasets should have prior to ingestion, when and how to modify the CRS of the datasets, selecting the type of vectorization the raster dataset should use if required by the system, and the tile size of the raster dataset. If a paramater is not defined explicitly, it will be set automatically using some default value.

For example, we can choose three different benchmark parameters: The `raster_tile_size` defines a list of tile sizes to test, `align_to_crs` defines that in this case the CRS of the vector dataset should be aligned to the CRS of the raster dataset, and `align_crs_at_stage` states, that the CRS should be aligned in the preprocess stage. Implicitly, Benchi will e.g. set the `vector_file_type` and `raster_file_type` parameters to the file types the datasets are provided in, in this case `SHP` and `GeoTIFF`.

The parameters can also define options like the target file format for both raster and vector files, information on raster tile sizes, and the `vectorization_type`, which is especially important for vector-based systems. There also exist the `raster_depth`, `raster_resolution`, and `vector_resolution` that can modify the raster and vector datasets directly. Initially we intended to base dataset classes on only one dataset and then reduce resolution and depth of the datasets on-the-fly using GDAL for the raster transformations and a suitable algorithm like the Ramer-Douglas-Peucker algorithm for reducing the vertex-to-feature ratio in vector datasets. Because we found enough real-world datasets to base our dataset classes on, these parameters serve no function in the current version of Benchi.

After the parameters have been loaded, they are checked for accuracy and consistency in the `FileIO` class to prevent any misconfigurations. Also, some final fixes are applied to shape the configs properly. Then, before the data is returned to the main routine, duplicates are eliminated to avoid running any benchmark twice without necessity.

Controlling, how the benchmarks are run is now the task of the controlling parameters. Currently, three of these parameters exist: `warm_starts` for defining, how many warm starts of the executions shall be performed per benchmark run with a default of 3, `iterations` for defining how often the complete benchmark run shall be repeated with a default of 1, and `timeout` for setting an upper limit on the execution duration with a default of 3 hours.


## Systems

Finally, the user needs to define a set of systems to run the workload on. Here, they define the name of the system and the port at which they can be accessed. Internally, the systems are treated as a special kind of benchmark parameter. This also enables us to use a less complex data structure.

```yaml
  systems:
    - name: postgis
      port: 25432
    - name: omnisci
      port: 6274
    - name: rasdaman
      port: 8080
    - name: sedona
    - name: beast
```
