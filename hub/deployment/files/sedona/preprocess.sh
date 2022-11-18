#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.2.3-3
echo "benchi_marker,$(date +%s.%N),start,preprocess,sedona,,"
docker run -v $(dirname $0)/../../data:/data --name "preprocess_sedona" --rm registry.gitlab.com/zergar/benchi/preprocess:0.2.3-3 python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,preprocess,sedona,,"

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d