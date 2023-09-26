# Benchi Utility

The Benchi utility is the main interaction point with the benchmarking framework. Located in the root folder at `benchi.py` It is running on the controller and therefore managing the full benchmarking process. It exposes a bunch of options and arguments that can be used to control the benchmarking process.

## Arguments Valid for all Routines

Some arguments have to be supplied regardless of the routine executed.

### `--config` {file}

The `--config` argument specifies the location of the [host configuration](host-config.md) file. It shall be used at most once.

### `--experiment` {file}

The `--experiment` argument specifies the location of the [workload definition](workload-definition.md) files. It can be used multiple times to specify multiple workload definitions, which are then run sequentially.

Example for multiple supplied workload definitions:

```shell
python benchi.py --config controller-config.yaml --experiment recipe_1.yaml --experiment recipe_n.yaml start
```

## `start` Routine Specific Arguments

The `start` routine is the main routine of Benchi, as it initializes a benchmark run. By default, it calls the `cleanup` routine after each benchmark run and the `eval` routine after each benchmark set.

### `--system` {name}

If the `--system` argument is supplied with a name only benchmark sets for this system are run, regardless of how many systems are listed in the workload definition. The system still needs to be listed in the `systems` object of the workload definition. Can only be supplied once.

### `--singlerun` and `--no-singlerun`

The `--singlerun` and `--no-singlerun` arguments together form a toggle-switch. When `--singlerun` is active (thus: True), only the first benchmark run of each workload definition will be executed. This is useful when trying to debug Benchi or a workload definition. False by default.


### `--postcleanup` and `--no-postcleanup`

The `--postcleanup` and `--no-postcleanup` arguments together form a toggle-switch. When `--no-postcleanup` is active (thus: False) **and** `--singelrun` is active, the cleanup routine un after each benchmark run will not be executed. Without `--singlerun`, this argument has no effect. This argument is useful if a query that shall be run on a system shall be debugged. False by default.


### `--eval` and `--no-eval`

The `--eval` and `--no-eval` arguments together form a toggle-switch. When `--no-eval` is active (thus: False), the evaluation routine is not started after all runs of a set have been completed. useful if the evaluation is done at a later stage. False by default.

## `eval` Routine Specific Arguments

The `eval` routine can take results of Benchi runs and compile them into an accuracy metric based on a user-defined function implemented in Python.

An exemplary call could look like this:

```shell
python benchi.py eval --config /config/eval-config.yaml --experiment "" --evalbase postgis --evalfolder 24_state_GLC --resultsfile /data/results/run_20230404-164405/results_beast_tiff_4326_100x100_9223372036854775807_1-0_points_shp_4326_1-0_AlignTo-raster_AlignAt-preprocess.warm-1.csv /data/results/run_20230404-164405/results_postgis_tiff_4326_auto_9223372036854775807_1-0_points_shp_4326_1-0_AlignTo-raster_AlignAt-preprocess.warm-1.csv /data/results/run_20230404-164405/results_rasdaman_tiff_4326_100x100_9223372036854775807_1-0_polygons_tiff_4326_1-0_AlignTo-raster_AlignAt-preprocess.warm-1.csv
```

### `--resultsfile` {file_1} … {file_n}

The `--resultsfile` argument specifies a lsit of files the evaluator shall use as a base for its evaluation run. The filenames can be either fetched from the logs of Benchi itself, or from the [results database](results-db.md).

### `--evalbase`

The `--evalbase` argument consists of a string containing the baseline for the comparison done by the evaluator. This string can be as simple as being just the system's name like `postgis`, but could also contain information about other parameters like the *vectorization type*. To see, how this string is constructed in more advanced cases, have a look at `hub/evaluator/main.py#Evaluator.__get_base()`

### `--evalfolder` {directory}

The `--evalfolder` argument specifies the location where the results of an evaluation run are stored.

## `cleanup` Routine Specific Arguments

The `cleanup` routine removes any running containers and [metrics gathering commands](quirks.md) if necessary.
