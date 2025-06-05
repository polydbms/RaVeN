#!/bin/bash

for i in "$@"
do
case $i in
    -q=*|--query=*)
    table1="${i#*=}"
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


echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep postgis)
echo "Running query"
echo "benchi_marker,$(date +%s.%N),start,execution,postgis,,"
docker exec $DOCKER_CONTAINER bash -c 'time PGPASSWORD=${POSTGRES_PASS} psql -d gis -f /data/query.sql -h localhost -U ${POSTGRES_USER}'
echo "benchi_marker,$(date +%s.%N),end,execution,postgis,,"
