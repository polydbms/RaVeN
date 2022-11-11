#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.1.0-12
echo "benchi_marker,$(date +%s.%N),start,preprocess,spatiallite,,"
docker run -v $(dirname $0)/../../data:/data --name "preprocess_spatiallite" registry.gitlab.com/zergar/benchi/preprocess:0.1.0-12 python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,preprocess,spatiallite,,"

echo "Starting Container in background"
cd ~/config && docker-compose up -d