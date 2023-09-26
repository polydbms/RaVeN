# Getting Started

Benchi is a benchmarking framework designed to benchmark systems capable of computing zonal statistics. Based on a workload definition file it takes one vector and raster dataset each and computes one zonal statistic per function and vector feature supplied based on the raster image. These results are returned as a CSV file. For analysis, Benchi also records and returns latency and resource utilization metrics.



## The benchmarking framework

In Benchi, the benchmark is run across a number of components usually residing on two different physical machines. The two machines are called *Controller* and *Host* from here on. In general, the controller takes care of the benchmarking process, while the host takes care of the compute-intensive workloads, i.e. preprocessing the data and running the queries.

| Component          | Run on              | Task |
|--------------------|---------------------|--|
| Benchi utility     | Controller          | Overseeing the benchmarking process |
| Preprocess utility | Docker on Host      | Preprocessing the datasets |
| System-under-test  | Docker on Host      | Run the queries |
| DuckDB (Database)  | Controller          | Record latency and resource utilization metrics |
| Evaluator | Controller          | Create relative accuracy metrics based on a user-defined function |


## Setting up Benchi

To function properly, Benchi requires the following minimal configuration files:

| File                                          | Description | Packaged into utility? | Required? |
|-----------------------------------------------|--|--|--|
| [Host Configuration](host-config.md)          | Defines workload-independent parameters like result locations | No | Yes |
| [SSH Configuration](ssh-config.md)            | Defines how the SSH server is accessed | No | Yes |
| [Capabilities](capabilities.md)               | Describes limitations of the systems | Yes | Yes |
| [Workload Definition](workload-definition.md) | Description of the Datasets and query to use, as well as parameter combinations | No | Yes |
| [Raster File](raster-file.md)                 | The raster dataset to be used in the workload | No | Yes |
| [Vector File](vector-file.md)                 | The vector dataset to be used in the workload | No | Yes |

Also, Benchi requires the following software to be installed:

+ ssh
+ scp
+ rsync
+ python
+ pip
+ gdal (needs to fit to the version of the python package)
+ docker

### Host Configuration

The host configuration acts as the static configuration for Benchi. you can find more info in the [Host Configuration](host-config.md) reference. The referenced directories and database file on the controller and the host are created automatically if they do not exist already. 

Minimal configuration as provided in `controller-config.sample.yaml`:

```yaml
config:
  controller:
    results_folder: /data/results         # Location of the results folder of the controller
    results_db: /data/results.db          # Location of the metrics database
  hosts:
    - host: "remote.server"               # URL of the server
      base_path: /data/benchipath         # Benchi root directory on the host
      public_key_path: ~/.ssh/id_rsa.pub  # Location of the SSH key to access the host
```

### SSH config

The SSH config file contains information on how the remote server shall be accessed. Its existence is required, but it does not need explicit information on how a host can be accessed. An exemplary configuration can be found under `ssh/config.default`. **It is necessary to copy this configuration to `ssh/config` before running Benchi.**

### Workload definition

The workload definition contains benchmark-run-specific information like the location of the datasets and the query to be run. It may also contain [further parameters](workload-definition.md) that define how the benchmark shall be executed. Examples of workload definitions can be found at **PATH TO RECIPE REPO**

Minimal workload definition:

```yaml
experiments:
  data:
    raster: /data/sentinel2a_mol_band3
    vector: /data/ALKIS_bezirk_MOL
  workload:
    get:
      vector:
        - oid
      raster:
        - sval:
            aggregations:
              - count
              - sum
    join:
      table1: vector
      table2: raster
      condition: intersect(raster, vector)
    group:
      vector:
        - oid
    order:
      vector:
        - oid

  systems:
    - name: postgis
      port: 25432

```

### Raster and Vector Files

For raster and vector datasets, a folder containing either a raster or a vector dataset needs to be provided ion the workload definition. These files are stored on the controller first and copied to the host automatically. Currently, Benchi supports ingesting raster files of format `.jp2` (JPEG 2000) and `.tif`, `.tiff` or `.geotiff`(GeoTIFF), and vector files of format `.shp` (ESRI Shapefile) and `.geojson` (GeoJSON).


## Running Benchi

Benchi can be run from the controller by first navigating into the directory containing the `benchi.py` file and then running the following command:

```bash
python benchi.py --config controller-config.yaml --experiment recipe.yaml start
```

An exhaustive reference of the `benchi` utility can be found in the [Benchi utility description](benchi-utility.md).

## Investigating results

After Benchi has completed a run, the results of the run can be found in the locations specified in the Host configuration. The results folder contains the results of each run as CSV as well as a folder containing a backup of the latency and resource utilization metrics collected in the meantime.  

## Cleaning up

Finally, Benchi performs an automatic cleanup. This cleanup can also be performed manually by running the following command from the directory where the `benchi.py` file is located:

```bash
python benchi.py --config controller-config.yaml --experiment recipe.yaml clean
```

## Docker compose

To make using Benchi a bit easier, Benchi also ships with a docker-compose file. Please note though that benchi does not store logs automatically. If keeping logs between restarts is required, it is therefore advised to store them by executing

```bash
docker-compose logs -t > run.log
```


