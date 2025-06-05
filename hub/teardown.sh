#!/bin/bash

BASE_DIR=$1

docker run  \
  -v $BASE_DIR/config:/config \
  -v $BASE_DIR/data:/data \
  ubuntu:latest \
  bash -c 'rm -rfv /config/beast/scala-beast/target'

docker run  \
  -v $BASE_DIR/config:/config \
  -v $BASE_DIR/data:/data \
  ubuntu:latest \
  bash -c 'find /data -iname "preprocessed_*" | while read r; do rm -rv $r; done'



kill $(ps aux | grep "docker stats" | awk {'print $2'})
docker stop $(docker ps -q)
docker rm $(docker ps -aq)
docker volume rm $(docker volume ls -q)