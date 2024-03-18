#!/bin/bash

TT=
T=

SENDX_SUM=$(($SENDX2 + $SENDX4 + $SENDX6))

echo "IS_XOX: $IS_XOX"
if [ "$IS_XOX" == "true" ]; then
  export FABRIC_TLS=false
  export FABRIC_ROOT=$GOPATH/src/github.com/hyperledger/fabric
  export FABRIC_CFG_PATH=$FABRIC_ROOT/fastfabric/scripts
  source $FABRIC_CFG_PATH/base_parameters.sh
  export CORE_PEER_MSPCONFIGPATH=./crypto-config/peerOrganizations/${PEER_DOMAIN}/users/Admin@${PEER_DOMAIN}/msp
fi

function timerStart {
  TT=0
  T=$(date +%s%N)
}
function timerTick {
  U=$(date +%s%N)
  GAP=$((($U - $T) / 1000))
  TT=$(($TT + $GAP))
  
  echo "[INF] $GAP ㎲ 경과 (총 $TT ㎲)"
  T=$U
}
function timerStop {
  R=$1

  timerTick
  echo "[INF] 벤치마킹 종료"
  echo "[INF] 총 $R tx 전송. 초당 평균 $(($R * 1000000 / $TT)) tx"
  # docker run --rm --net=FABRIC redis sh -c "redis-cli -h redis.jjo.kr -p 16379 -n 0 SET benchmark-result-$CLIENT_ID-n $R"
  # docker run --rm --net=FABRIC redis sh -c "redis-cli -h redis.jjo.kr -p 16379 -n 0 SET benchmark-result-$CLIENT_ID-t $TT"
}
function dice {
  SENDX=
  DICE=$(($RANDOM % $SENDX_SUM))

  if [ $DICE -lt $SENDX2 ]; then
    SENDX=2
  elif [ $DICE -lt $(( $SENDX2 + $SENDX4 )) ]; then
    SENDX=4
  else
    SENDX=6
  fi
}