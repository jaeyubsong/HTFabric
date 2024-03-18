#!/usr/bin/env bash
source base_parameters.sh

peer=$1
query=$2

export CORE_PEER_ADDRESS=${peer}:7051
export CORE_PEER_MSPCONFIGPATH="./crypto-config/peerOrganizations/$PEER_DOMAIN/users/Admin@$PEER_DOMAIN/msp"

echo peer chaincode invoke -C ${CHANNEL} -n $CHAINCODE -c '{"Args":["send2","0", "1", "1"]}'

peer chaincode invoke -C ${CHANNEL} -n ${CHAINCODE} -c '{"Args":["send2","0", "1", "1"]}'

echo peer chaincode invoke -C ${CHANNEL} -n $CHAINCODE -c '{"Args":["send8","0", "1", "2", "3", "4", "5", "6", "7", "1"]}'

peer chaincode invoke -C ${CHANNEL} -n ${CHAINCODE} -c '{"Args":["send8","0", "1", "2", "3", "4", "5", "6", "7", "1"]}'

echo peer chaincode invoke -C ${CHANNEL} -n $CHAINCODE -c '{"Args":["readwrite4","0", "1", "2", "3", "4", "5", "1", "4"]}'

peer chaincode invoke -C ${CHANNEL} -n $CHAINCODE -c '{"Args":["readwrite4","0", "1", "2", "3", "4", "5", "1", "4"]}'

if [ "$query" == "yes" ]; then
  sleep 3

  a="'{\"Args\":[\"query\",\"0\"]}'"
  echo peer chaincode query -C ${CHANNEL}  -n ${CHAINCODE} -c $a | bash

  a="'{\"Args\":[\"query\",\"1\"]}'"
  echo peer chaincode query -C ${CHANNEL}  -n ${CHAINCODE} -c $a | bash
fi
