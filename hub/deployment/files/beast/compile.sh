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
    *)
    ;;
esac
done


echo $(dirname $0)
echo "compiling beast execution script"
docker pull maven:3.8.6-openjdk-18
echo "benchi_marker,$(date +%s.%N),start,ingestion,beast,,"
docker run -v $(dirname $0)/../../config/beast/scala-beast:/scala-beast --name "compile_beast" --rm maven:3.8.6-openjdk-18 bash -c "cd /scala-beast && mvn -T 1C -B package"
echo "benchi_marker,$(date +%s.%N),mid,ingestion,beast,compile_end,"
echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep namenode)
echo $DOCKER_CONTAINER

docker exec $DOCKER_CONTAINER bash -c "hdfs dfs -mkdir -p /data/beast_result"
docker exec $DOCKER_CONTAINER bash -c "hdfs dfs -mkdir -p $(dirname $vector)"
docker exec $DOCKER_CONTAINER bash -c "hdfs dfs -mkdir -p $(dirname $raster)"
docker exec $DOCKER_CONTAINER bash -c "hdfs dfs -put $vector $vector"
docker exec $DOCKER_CONTAINER bash -c "hdfs dfs -put $raster $raster"
echo "benchi_marker,$(date +%s.%N),end,ingestion,beast,,"
