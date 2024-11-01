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
    -s=*|--srid=*)
    srid="${i#*=}"
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
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep postgis)
echo "Ingesting data"
if [ ! -z ${raster+x} ]; then
    echo "benchi_marker,$(date +%s.%N),pre,ingestion,postgis,raster,"
    docker exec $DOCKER_CONTAINER bash -c "time shp2pgsql -s $srid -I $raster $name > raster.sql"
    echo "benchi_marker,$(date +%s.%N),start,ingestion,postgis,raster,"
    docker exec $DOCKER_CONTAINER bash -c 'time PGPASSWORD=${POSTGRES_PASS} psql -d gis -f raster.sql -h localhost -U ${POSTGRES_USER}'
    echo "benchi_marker,$(date +%s.%N),end,ingestion,postgis,raster,"
fi

if [ ! -z ${vector+x} ]; then
    echo "benchi_marker,$(date +%s.%N),pre,ingestion,postgis,vector,"
    docker exec $DOCKER_CONTAINER bash -c "time shp2pgsql -s $srid -I $vector $name > vector.sql"
    echo "benchi_marker,$(date +%s.%N),start,ingestion,postgis,vector,"
    docker exec $DOCKER_CONTAINER bash -c 'time PGPASSWORD=${POSTGRES_PASS} psql -d gis -f vector.sql -h localhost -U ${POSTGRES_USER}'
    echo "benchi_marker,$(date +%s.%N),end,ingestion,postgis,vector,"
fi