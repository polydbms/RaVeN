#!/bin/bash

echo $(dirname $0)
echo "compiling beast execution script"
docker pull maven:3.8.6-openjdk-18
echo "benchi_marker,$(date +%s.%N),start,ingestion,beast,,"
docker run -v $(dirname $0)/../../config/beast/scala-beast:/scala-beast --name "compile_beast" --rm maven:3.8.6-openjdk-18 bash -c "cd /scala-beast && mvn -T 1C -B package"
echo "benchi_marker,$(date +%s.%N),end,ingestion,beast,,"