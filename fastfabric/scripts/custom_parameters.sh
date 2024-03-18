#!/usr/bin/env bash


export PEER_DOMAIN="org1.jjo.kr"           # can be anything if running on localhost
export ORDERER_DOMAIN="jjo.kr"        # can be anything if running on localhost

# fill in the addresses without domain suffix and without ports
export FAST_PEER_ADDRESS="fast"
export ORDERER_ADDRESS="orderer0"

# leave endorser address and storage address blank if you want to run on a single server
export ENDORSER_ADDRESS=(mars02 mars03 mars04 mars05)
export STORAGE_ADDRESS="data"

export CHANNEL="jjoriping"               # the name of the created channel of the network
export CHAINCODE="InfoCC"             # the name of the chaincode used on the channel