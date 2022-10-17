#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.1.0-4
docker run -v $(dirname $0)/../../data:/data registry.gitlab.com/zergar/benchi/preprocess:0.1.0-4 python preprocess.py $1

echo "Starting Container in background"
cd $(dirname $0) && docker-compose up -d