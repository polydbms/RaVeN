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
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep rasdaman)
echo "Ingesting data"

echo "Ingesting via ingredients.json."
echo "benchi_marker,$(date +%s.%N),start,ingestion,rasdaman,raster,"
docker exec $DOCKER_CONTAINER bash -c "/opt/rasdaman/bin/wcst_import.sh /config/rasdaman/ingredients.json"
echo "benchi_marker,$(date +%s.%N),end,ingestion,rasdaman,raster,"

#if [ ! -z ${raster+x} ]; then
#    name=$(get_filename $raster | cut -d'.' -f1)
#fi


