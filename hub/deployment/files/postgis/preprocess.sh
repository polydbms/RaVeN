#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.2.3-7
echo "benchi_marker,$(date +%s.%N),start,preprocess,postgis,,"
docker run -v $(dirname $0)/../../data:/data --name "preprocess_postgis" --rm registry.gitlab.com/zergar/benchi/preprocess:0.2.3-7 python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,preprocess,postgis,,"

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep postgis)

echo "Config"
docker exec $DOCKER_CONTAINER bash -c "apt-get update"
docker exec $DOCKER_CONTAINER bash -c "apt-get install postgis -y"