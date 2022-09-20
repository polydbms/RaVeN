#!/bin/bash

for i in "$@"
do
case $i in
    -q=*|--query=*)
    table1="${i#*=}"
    shift
    ;;
    --default)
    DEFAULT=YES
    shift
    ;;
    *)
    ;;
esac
done

echo "Get docker container name"
export DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep omnisci)
echo "Running query"
docker exec $DOCKER_CONTAINER bash -c "cat /data/query.sql | /omnisci/bin/omnisql -p HyperInteractive"