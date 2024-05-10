#!/bin/bash

parameters=$(echo $1 | base64 -d)

eval "ARGS=($parameters)"

echo "Preprocessing data"
docker pull ghcr.io/polydbms/preprocess:0.6.2-1
echo "benchi_marker,$(date +%s.%N),start,preprocess,postgis,,"
docker run -e PYTHONUNBUFFERED=1 -v $(dirname $0)/../../data:/data --name "preprocess_postgis" --rm  ghcr.io/polydbms/preprocess:0.6.2-1 python preprocess.py "${ARGS[@]}"
echo "benchi_marker,$(date +%s.%N),end,preprocess,postgis,,"

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep postgis)

echo "Config"
docker exec $DOCKER_CONTAINER bash -c "apt-get update"
docker exec $DOCKER_CONTAINER bash -c "apt-get install postgis -y"