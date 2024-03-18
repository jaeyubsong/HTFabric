#!/bin/bash

source ../base_parameters.sh

npm run build
rm -rf wallet/
node dist/addToWallet.js
