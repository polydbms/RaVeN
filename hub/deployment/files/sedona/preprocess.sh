#!/bin/bash

echo "Preprocessing data"
docker pull preprocess
echo "benchi_marker,$(date +%s.%N),start,preprocess,sedona,,"
docker run -e PYTHONUNBUFFERED=1 -v $(dirname $0)/../../data:/data --name "preprocess_sedona" --rm preprocess python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,preprocess,sedona,,"

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d