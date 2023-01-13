#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.2.7-1
echo "benchi_marker,$(date +%s.%N),start,preprocess,beast,,"
docker run -e PYTHONUNBUFFERED=1 -v $(dirname $0)/../../data:/data --name "preprocess_beast" --rm registry.gitlab.com/zergar/benchi/preprocess:0.2.7-1 python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,preprocess,beast,,"

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d