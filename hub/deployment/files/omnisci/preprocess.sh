#!/bin/bash

echo "Preprocessing data"
docker pull registry.gitlab.com/zergar/benchi/preprocess:0.1.0-3
docker run -v ~/data:/data registry.gitlab.com/zergar/benchi/preprocess:0.1.0-3 python preprocess.py $1

echo "Starting Container in background"
cd ~/config/omnisci && docker-compose up -d