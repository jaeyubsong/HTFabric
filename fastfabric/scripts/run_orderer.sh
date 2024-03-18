#!/bin/bash
source base_parameters.sh
docker ps -aq | xargs docker stop | xargs docker rm

export FABRIC_LOGGING_SPEC=WARN
export ORDERER_GENERAL_LOCALMSPDIR=${FABRIC_CFG_PATH}/crypto-config/ordererOrganizations/${ORDERER_DOMAIN}/orderers/${ORDERER_ADDRESS}.${ORDERER_DOMAIN}/msp

rm /home/jjoriping/Power-Fabric/production -r # clean up data from previous runs
rm cpu_profile.log

(cd ${FABRIC_ROOT}/orderer/ && go install)
bash run_orderer_profiler.sh orderer &
orderer start
