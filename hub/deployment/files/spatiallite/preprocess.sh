#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.2.4-3
echo "benchi_marker,$(date +%s.%N),start,preprocess,spatiallite,,"
docker run -v $(dirname $0)/../../data:/data --name "preprocess_spatiallite" --rm registry.gitlab.com/zergar/benchi/preprocess:0.2.4-3 python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,preprocess,spatiallite,,"

echo "Starting Container in background"
cd ~/config && docker-compose up -d