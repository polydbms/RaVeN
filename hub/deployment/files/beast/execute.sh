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
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep beast)
echo $DOCKER_CONTAINER
echo "Running query"
echo "benchi_marker,$(date +%s.%N),start,execution,beast,,outer"
docker exec $DOCKER_CONTAINER bash -c '$(find /opt/bitnami/spark/beast*/bin/beast) --class benchi.RaptorScala /config/beast/scala-beast/target/beast-bench-1.0-SNAPSHOT.jar'
echo "benchi_marker,$(date +%s.%N),end,execution,beast,,outer"

docker exec $DOCKER_CONTAINER bash -c 'find /data/beast_result -iname "*.csv" -exec mv {} /data/results/results_beast.csv \;'
docker exec $DOCKER_CONTAINER bash -c 'rm -r /data/beast_result'
#docker exec $DOCKER_CONTAINER bash -c 'rm -r /config/beast/scala-beast/target' # fixme add cleanup for jar to avoid weird bugs
