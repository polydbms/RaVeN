#!/bin/bash

docker exec $DOCKER_CONTAINER bash -c 'rm -rf /config/beast/scala-beast/target'

kill $(ps aux | grep "docker stats" | awk {'print $2'})
docker stop $(docker ps -q)
docker rm $(docker ps -aq)
docker volume rm $(docker volume ls -q)