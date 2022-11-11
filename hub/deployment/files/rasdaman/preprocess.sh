#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.1.0-12
echo "benchi_marker,$(date +%s.%N),start,preprocess,rasdaman,,"
docker run -v $(dirname $0)/../../data:/data --name "preprocess_rasdaman" registry.gitlab.com/zergar/benchi/preprocess:0.1.0-12 python preprocess.py $1
echo "benchi_marker,$(date +%s.%N),end,preprocess,rasdaman,,"

echo $(dirname $0)

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d
