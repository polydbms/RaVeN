#!/bin/bash

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep postgis)
echo "Running explain analyze query"
COMMAND='time PGPASSWORD=${POSTGRES_PASS} psql -d gis -f /data/query_ea.sql -h localhost -U ${POSTGRES_USER}'
docker exec $DOCKER_CONTAINER bash -c "${COMMAND} > $1"
