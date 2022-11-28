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
    -n=*|--name=*)
    name="${i#*=}"
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

#get_filename () {
#    filename=$(basename -- "$1")
#    echo "${filename%.*}"
#}

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep omnisci)
echo "Ingesting data"
if [ ! -z ${raster+x} ]; then
    echo "Raster ingestion is not supported. Using the vectorized raster equivalent."
    echo "benchi_marker,$(date +%s.%N),start,ingestion,omnisci,raster,"
    docker exec $DOCKER_CONTAINER bash -c "echo \"COPY $name FROM '$raster' WITH (source_type='geo_file');\" | /opt/heavyai/bin/heavysql -p HyperInteractive"
#    docker exec $DOCKER_CONTAINER bash -c "echo \"COPY $name FROM '$raster' WITH (source_type='geo_file');\" | /omnisci/bin/omnisql -p HyperInteractive"
    echo "benchi_marker,$(date +%s.%N),end,ingestion,omnisci,raster,"
fi

if [ ! -z ${vector+x} ]; then
    echo "benchi_marker,$(date +%s.%N),start,ingestion,omnisci,vector,"
    docker exec $DOCKER_CONTAINER bash -c "echo \"COPY $name FROM '$vector' WITH (source_type='geo_file');\" | /opt/heavyai/bin/heavysql -p HyperInteractive"
#    docker exec $DOCKER_CONTAINER bash -c "echo \"COPY $name FROM '$vector' WITH (source_type='geo_file');\" | /omnisci/bin/omnisql -p HyperInteractive"
    echo "benchi_marker,$(date +%s.%N),end,ingestion,omnisci,vector,"
fi