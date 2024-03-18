#!/bin/bash
source base_parameters.sh
docker ps -aq | xargs docker stop | xargs docker rm

p_addr=$(get_correct_peer_address $(hostname))

export FABRIC_LOGGING_SPEC=WARN
export CORE_PEER_MSPCONFIGPATH=${FABRIC_CFG_PATH}/crypto-config/peerOrganizations/${PEER_DOMAIN}/peers/$p_addr/msp

export CORE_METRICS_STATSD_PREFIX=$(echo $p_addr | awk -F '.' '{print $1}')

export CORE_PEER_ID=${p_addr}
export CORE_PEER_ADDRESS=${p_addr}:7051
export CORE_PEER_GOSSIP_EXTERNALENDPOINT=${p_addr}:7051
export CORE_PEER_CHAINCODEADDRESS=${p_addr}:7052
export CORE_PEER_GOSSIP_USELEADERELECTION=false
export CORE_PEER_GOSSIP_ORGLEADER=false

rm /home/jjoriping/Power-Fabric/production -r # clean up data from previous runs
rm cpu_profile.log
(cd ${FABRIC_ROOT}/peer/ && go install)

if [ "$p_addr" = "mars02.org1.jjo.kr" ]; then
  bash run_orderer_profiler.sh peer &
fi

# peer node start -e
# if [ "$MODEL" = "my-xox-fabric" ]; then
  peer node start -e --storageAddr $(get_correct_peer_address ${STORAGE_ADDRESS}):10000
# else
#   export CORE_METRICS_PROVIDER=disabled
#   peer node start
# fi