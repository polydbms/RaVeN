#!/bin/bash

parameters=$(echo $1 | base64 -d)

eval "ARGS=($parameters)"

echo "Preprocessing data"
docker pull ghcr.io/polydbms/preprocess:0.11.0-0
echo "benchi_marker,$(date +%s.%N),start,preprocess,beast,,"
docker run -e PYTHONUNBUFFERED=1 -v $(dirname $0)/../../data:/data --name "preprocess_beast" --rm  ghcr.io/polydbms/preprocess:0.11.0-0 python preprocess.py "${ARGS[@]}"
echo "benchi_marker,$(date +%s.%N),end,preprocess,beast,,"

echo "Starting Container in background"
cd $(dirname $0) && docker compose up -d

sleep 5

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep beast)

docker exec $DOCKER_CONTAINER bash -c "mkdir -p /data/beasthistory"
docker exec $DOCKER_CONTAINER bash -c "./sbin/start-history-server.sh"