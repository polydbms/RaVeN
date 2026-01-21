#!/bin/bash

parameters=$(echo $1 | base64 -d)

eval "ARGS=($parameters)"

echo "Preprocessing data"
docker pull ghcr.io/polydbms/preprocess:0.11.5-0
echo "benchi_marker,$(date +%s.%N),start,preprocess,sedona-vec,,"
docker run -e PYTHONUNBUFFERED=1 -v $(dirname $0)/../../data:/data --name "preprocess_sedona" --rm  ghcr.io/polydbms/preprocess:0.11.5-0 python preprocess.py "${ARGS[@]}"
echo "benchi_marker,$(date +%s.%N),end,preprocess,sedona-vec,,"

echo "Starting Container in background"
cd $(dirname $0) && docker compose up -d