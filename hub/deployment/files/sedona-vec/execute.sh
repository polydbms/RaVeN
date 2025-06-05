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
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep sedona)
echo $DOCKER_CONTAINER
echo "Running query"
echo "benchi_marker,$(date +%s.%N),start,execution,sedona-vec,,outer"
docker exec $DOCKER_CONTAINER bash -c 'python /config/sedona/executor.py'
echo "benchi_marker,$(date +%s.%N),end,execution,sedona-vec,,outer"
