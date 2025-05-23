#!/bin/bash

parameters=$(echo $1 | base64 -d)

eval "ARGS=($parameters)"

echo "Preprocessing data"
docker pull ghcr.io/polydbms/preprocess:0.11.0-0
echo "benchi_marker,$(date +%s.%N),start,preprocess,postgis,,"
docker run -e PYTHONUNBUFFERED=1 -v $(dirname $0)/../../data:/data --name "preprocess_postgis" --rm  ghcr.io/polydbms/preprocess:0.11.0-0 python preprocess.py "${ARGS[@]}"
echo "benchi_marker,$(date +%s.%N),end,preprocess,postgis,,"

echo "Starting Container in background"
cd $(dirname $0) && docker compose up -d

#echo "Get docker container name"
#export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep postgis)
#
#echo "Config"
#docker exec -u root $DOCKER_CONTAINER chmod -R 777 /var/lib/apt/lists/partial
#docker exec -u root $DOCKER_CONTAINER apt-get update
#docker exec -u root $DOCKER_CONTAINER apt-get install postgis -y