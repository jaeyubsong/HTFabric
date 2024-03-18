#!/bin/bash
source base_parameters.sh
docker ps -aq | xargs docker stop | xargs docker rm

export FABRIC_LOGGING_SPEC=WARN
export CORE_PEER_MSPCONFIGPATH=${FABRIC_CFG_PATH}/crypto-config/peerOrganizations/${PEER_DOMAIN}/peers/${FAST_PEER_ADDRESS}.${PEER_DOMAIN}/msp

p_addr=$(get_correct_peer_address $FAST_PEER_ADDRESS)
export CORE_PEER_ID=${p_addr}
export CORE_PEER_ADDRESS=${p_addr}:7051
export CORE_PEER_CHAINCODEADDRESS=${p_addr}:7052
export CORE_PEER_GOSSIP_EXTERNALENDPOINT=${p_addr}:7051
export CORE_PEER_GOSSIP_USELEADERELECTION=false
export CORE_PEER_GOSSIP_ORGLEADER=true


rm /home/jjoriping/Power-Fabric/production -r # clean up data from previous runs
rm cpu_profile.log
(cd ${FABRIC_ROOT}/peer/ && go install)

# peer node start can be run without the storageAddr. In that case those modules will not be decoupled to different nodes
s=""
if [[ ! -z ${STORAGE_ADDRESS} ]]
then
    echo "Starting with decoupled storage server ${STORAGE_ADDRESS}"
    s="--storageAddr $(get_correct_peer_address ${STORAGE_ADDRESS}):10000"
fi

# bash run_fastpeer_profiler.sh &
bash run_orderer_profiler.sh peer &

# if [ "$MODEL" = "my-xox-fabric" ]; then
    peer node start ${s}
# else
#     peer node start
# fi