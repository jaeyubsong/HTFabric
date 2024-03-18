#!/usr/bin/env bash
source base_parameters.sh

cd infolab/

npm run build
node dist/Main.js 0 1 $GOPATH/../infolab/benchmarks/inputs/indep-12/part-00000
