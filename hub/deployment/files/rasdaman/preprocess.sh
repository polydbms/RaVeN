#!/bin/bash

echo "Preprocessing data"
docker pull laertnuhu/preprocessor
docker run -v ~/data:/data laertnuhu/preprocessor python preprocess.py $1