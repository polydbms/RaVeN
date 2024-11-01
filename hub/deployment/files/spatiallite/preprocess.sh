#!/bin/bash

echo "Preprocessing data"
docker pull ghcr.io/polydbms/preprocess:0.8.0-1
echo "benchi_marker,$(date +%s.%N),start,ghcr.io/polydbms/preprocess:0.8.0-1,spatiallite,,"
docker run -v $(dirname $0)/../../data:/data --name "ghcr.io/polydbms/preprocess:0.8.0-1_spatiallite" --rm preprocess python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,ghcr.io/polydbms/preprocess:0.8.0-1,spatiallite,,"

echo "Starting Container in background"
cd ~/config && docker-compose up -d