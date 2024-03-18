#!/bin/bash
echo "chaincode setup start"
source base_parameters.sh

export CORE_PEER_MSPCONFIGPATH=./crypto-config/peerOrganizations/${PEER_DOMAIN}/users/Admin@${PEER_DOMAIN}/msp

endorsers=(${FAST_PEER_ADDRESS})

if [[ ! -z ${ENDORSER_ADDRESS[@]} ]]
then
    endorsers=(${endorsers[@]} ${ENDORSER_ADDRESS[@]})
fi

for i in ${endorsers[@]}
do
    export CORE_PEER_ADDRESS=$(get_correct_peer_address ${i}):7051
    echo "체인코드 설치: $CORE_PEER_ADDRESS"
    peer chaincode install -l golang -n ${CHAINCODE} -v 2.4 -o $(get_correct_orderer_address):7050 -p "github.com/hyperledger/fabric/fastfabric/chaincode"
done

export CORE_PEER_ADDRESS=$(get_correct_peer_address ${endorsers[0]}):7051
echo "체인코드 인스턴스화: $CORE_PEER_ADDRESS"
a="'{\"Args\":[]}'"
echo peer chaincode instantiate -o $(get_correct_orderer_address):7050 -C ${CHANNEL} -n $CHAINCODE -v 2.4 -l golang -c ${a} | bash

# sleep 5

echo "start account setup"
bash chaincode_account_setup.sh 0 3000 1
bash chaincode_account_setup.sh 0 200005 1

