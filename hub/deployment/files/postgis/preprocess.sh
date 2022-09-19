#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.1.0-2
docker run -v ~/data:/data registry.gitlab.com/zergar/benchi/preprocess:0.1.0-2 python preprocess.py $1

echo "Starting Container in background"
cd ~/config/postgis && docker-compose up -d

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}')

echo "Config"
docker exec $DOCKER_CONTAINER bash -c "apt update"
docker exec $DOCKER_CONTAINER bash -c "apt install postgis -y"