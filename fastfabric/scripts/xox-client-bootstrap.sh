#!/bin/bash

cd $GOPATH/src/github.com/hyperledger/fabric/peer
go install
cd $GOPATH/src/github.com/hyperledger/fabric/fastfabric/scripts/

source ./base_parameters.sh

./channel_setup.sh
sleep 3
./chaincode_setup.sh
sleep 3
./test_chaincode.sh fast.org1.jjo.kr yes
if [ $? -ne 0 ]; then
  exit 1
fi
for i in ${ENDORSER_ADDRESS[@]}; do
  echo "[INF] 테스트: $i"
  # for j in 0 1 2 3 4 5 6 7; do
    ./test_chaincode.sh $i.org1.jjo.kr yes
    if [ $? -ne 0 ]; then
      exit 1
    fi
  # done
done

# exit 2