# Preprocessor

The main task of the preprocess utility is to prepare datasets such that they can be used by the different systems. It therefore primarily takes parameters from the [workload definition](workload-definition.md) and applies them to the given datasets. Internally, the utility uses the chain of responsibility pattern to parse the datasets.

## Using the Preprocess Utility
The preprocess utility can be called using the following command:

```bash
python preprocess.py --system {system} \
	--vector_path {vector_dir} \
 	--vector_target_suffix {vector_target_format} \
 	--vector_output_folder {vector_output_folder}  \
 	--vector_target_crs {vector_target_crs}  \
 	--vectorization_type {vectorize_type}  \
 	--raster_path {raster_dir}  \
 	--raster_target_suffix {raster_target_format}  \
 	--raster_output_folder {raster_output_folder}  \
 	--raster_target_crs {raster_target_crs}
```


## Building the Preprocess Utility

The preprocess utility is packaged into a separate docker container. It can be built using the following command:

```bash
docker build . --target=preprocess -t preprocess
```

**Note: If a new version of the preprocessor shall be used, the reference to the container needs to be updated in each `preprocess.sh` script, which can be found at `hub/deployment/files/**/preprocess.sh`.** If Benchi is used in its dockerized version, the version also has to be updated there.
