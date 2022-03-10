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
    docker exec $DOCKER_CONTAINER spatialite_tool -i -shp $raster -d spatial.db -t $name -c UTF-8 -s $srid
fi

if [ ! -z ${vector+x} ]; then
    name="$(get_filename $vector | cut -d'.' -f1)"
    docker exec $DOCKER_CONTAINER spatialite_tool -i -shp $vector -d spatial.db -t $name -c UTF-8 -s $srid
fi