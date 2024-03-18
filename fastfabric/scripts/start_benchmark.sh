#!/bin/bash
NAME=$1
CLIENT_COUNT=$2
TASKS_PER_CLIENT=$3
METHOD=$4

cd $GOPATH/src/github.com/hyperledger/fabric/fastfabric/scripts/

source ./base_parameters.sh

SCRIPT_DIR=$GOPATH/src/github.com/hyperledger/fabric/fastfabric/scripts

if [ -z "$NAME" ] | [ -z "$CLIENT_COUNT" ] | [ -z "$TASKS_PER_CLIENT" ]; then
  echo "[ERR] 올바른 NAME/CLIENT_COUNT/TASKS_PER_CLIENT를 입력해 주세요."
  exit 1
fi
for w in `seq $CLIENT_COUNT`; do
  CLIENT_ID=$(($w - 1))
  # CLIENT_ID=13
  echo "ssh client$CLIENT_ID.org1.jjo.kr -t bash -c"
  echo "hostname --ip-address; pwd; source /home/jjoriping/Power-Fabric/profile.sh; cd $SCRIPT_DIR; \
      ./infolab_task.sh $CLIENT_ID $CLIENT_COUNT $TASKS_PER_CLIENT $NAME $METHOD"
  ssh client$CLIENT_ID.org1.jjo.kr -t bash -c \
    "hostname --ip-address; pwd; source /home/jjoriping/Power-Fabric/profile.sh; cd $SCRIPT_DIR; \
      ./infolab_task.sh $CLIENT_ID $CLIENT_COUNT $TASKS_PER_CLIENT $NAME $METHOD" &
done
wait
echo "[INF] 완료!"
