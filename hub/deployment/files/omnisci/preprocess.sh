#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.2.3-7
echo "benchi_marker,$(date +%s.%N),start,preprocess,omnisci,,"
docker run -v $(dirname $0)/../../data:/data --name "preprocess_omnisci" --rm registry.gitlab.com/zergar/benchi/preprocess:0.2.3-7 python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,preprocess,omnisci,,"

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d