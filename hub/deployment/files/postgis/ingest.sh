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
    -s=*|--srid=*)
    srid="${i#*=}"
    shift
    ;;
    -t=*|--tile=*)
    tile="${i#*=}"
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
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}')
echo "Ingesting data"
if [ ! -z ${raster+x} ]; then
    name=$(get_filename $raster | cut -d'.' -f1)
    docker exec $DOCKER_CONTAINER bash -c "time raster2pgsql -s $srid -I -C -M $raster -F -t $tile $name > raster.sql"
    docker exec $DOCKER_CONTAINER bash -c 'time PGPASSWORD=${POSTGRES_PASS} psql -d gis -f raster.sql -h localhost -U ${POSTGRES_USER}'
fi

if [ ! -z ${vector+x} ]; then
    name=$(get_filename $vector | cut -d'.' -f1)
    docker exec $DOCKER_CONTAINER bash -c "time shp2pgsql -s $srid -I $vector $name > vector.sql"
    docker exec $DOCKER_CONTAINER bash -c 'time PGPASSWORD=${POSTGRES_PASS} psql -d gis -f vector.sql -h localhost -U ${POSTGRES_USER}'
fi