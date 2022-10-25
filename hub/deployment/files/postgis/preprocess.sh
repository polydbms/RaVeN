#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.1.0-6
docker run -v $(dirname $0)/../../data:/data registry.gitlab.com/zergar/benchi/preprocess:0.1.0-6 python preprocess.py $1

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep postgis)

echo "Config"
docker exec $DOCKER_CONTAINER bash -c "apt-get update"
docker exec $DOCKER_CONTAINER bash -c "apt-get install postgis -y"