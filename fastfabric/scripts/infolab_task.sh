#!/bin/bash
CLIENT_ID=$1
CLIENT_COUNT=$2
TASKS_PER_CLIENT=$3
INPUT=$4
METHOD=$5

source ./base_parameters.sh

TASK_COUNT=$(($CLIENT_COUNT * $TASKS_PER_CLIENT))

./prepare.sh $INPUT
cd infolab/

for i in `seq $TASKS_PER_CLIENT`; do
  TASK_ID=$(($CLIENT_ID * $TASKS_PER_CLIENT + $i - 1))
  node --max-old-space-size=20000 dist/Main.js $TASK_ID $TASK_COUNT /home/jjoriping/Power-Fabric/inputs/$INPUT.sendx.bundled $METHOD &
done
wait
