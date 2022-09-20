#!/bin/bash

for i in "$@"
do
case $i in
    -r=*|--raster=*)
    raster="${i#*=}"
    shift
    ;;
    -v=*|--vector=*)
    vector="${i#*=}"
    shift
    ;;
    --default)
    DEFAULT=YES
    shift
    ;;
    *)
    ;;
esac
done

get_filename () {
    filename=$(basename -- "$1")
    echo "${filename%.*}"
}

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep omnisci)
echo "Ingesting data"
if [ ! -z ${raster+x} ]; then
    name=$(get_filename $raster | cut -d'.' -f1)
    echo "Raster ingestion is not supported. Using the vectorized raster equivalent."
    docker exec $DOCKER_CONTAINER bash -c "echo \"COPY $name FROM '$raster' WITH (source_type='geo_file');\" | /omnisci/bin/omnisql -p HyperInteractive"
fi

if [ ! -z ${vector+x} ]; then
    name="$(get_filename $vector)"
    docker exec $DOCKER_CONTAINER bash -c "echo \"COPY $name FROM '$vector' WITH (source_type='geo_file');\" | /omnisci/bin/omnisql -p HyperInteractive"
fi