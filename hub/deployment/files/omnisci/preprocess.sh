#!/bin/bash

parameters=$(echo $1 | base64 -d)

eval "ARGS=($parameters)"

echo "Preprocessing data"
docker pull ghcr.io/polydbms/preprocess:0.9.1-5
echo "benchi_marker,$(date +%s.%N),start,preprocess,omnisci,,"
docker run -e PYTHONUNBUFFERED=1 -v $(dirname $0)/../../data:/data --name "preprocess_omnisci" --rm  ghcr.io/polydbms/preprocess:0.9.1-5 python preprocess.py "${ARGS[@]}"
echo "benchi_marker,$(date +%s.%N),end,preprocess,omnisci,,"

echo "Starting Container in background"
cd $(dirname $0) && docker compose up -d