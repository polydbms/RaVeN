#!/bin/bash

echo "Preprocessing data"
docker pull laertnuhu/preprocessor
docker run -v ~/data:/data laertnuhu/preprocessor python preprocess.py $1

echo "Starting Container in background"
cd ~/config && docker-compose up -d

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}')

echo "Config"
docker exec $DOCKER_CONTAINER bash -c "apt update"
docker exec $DOCKER_CONTAINER bash -c "apt install postgis -y"