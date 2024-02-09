#!/bin/bash

echo "Preprocessing data"
docker pull ghcr.io/polydbms/preprocess:0.5.0-1
echo "benchi_marker,$(date +%s.%N),start,preprocess,rasdaman,,"
docker run -e PYTHONUNBUFFERED=1 -v $(dirname $0)/../../data:/data --name "preprocess_rasdaman" --rm ghcr.io/polydbms/preprocess:0.5.0-1 python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,preprocess,rasdaman,,"

echo $(dirname $0)

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d
