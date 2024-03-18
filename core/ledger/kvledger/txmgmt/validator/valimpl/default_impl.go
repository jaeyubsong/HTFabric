/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package valimpl

import (
	"fmt"
	"runtime"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/fastfabric/dependency/deptype"
	"github.com/hyperledger/fabric/fastfabric/validator/internalVal"
	"github.com/hyperledger/fabric/fastfabric/validator/statebasedval"
	"github.com/hyperledger/fabric/infolab"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

var logger = flogging.MustGetLogger("valimpl")

// DefaultImpl implements the interface validator.Validator
// This performs the common tasks that are independent of a particular scheme of validation
// and for actual validation of the public rwset, it encloses an internal validator (that implements interface
// internal.InternalValidator) such as statebased validator
type DefaultImpl struct {
	txmgr             txmgr.TxMgr
	db                privacyenabledstate.DB
	internalValidator internalVal.Validator
}

// NewStatebasedValidator constructs a validator that internally manages statebased validator and in addition
// handles the tasks that are agnostic to a particular validation scheme such as parsing the block and handling the pvt data
func NewStatebasedValidator(txmgr txmgr.TxMgr, db privacyenabledstate.DB) validator.Validator {
	return &DefaultImpl{txmgr, db, statebasedval.NewValidator(db)}
}

// ValidateAndPrepareBatch implements the function in interface validator.Validator
func (impl *DefaultImpl) ValidateAndPrepareBatch(blockAndPvtdata *ledger.BlockAndPvtData,
	doMVCCValidation bool, committedTxs chan<- *deptype.Transaction) (*privacyenabledstate.UpdateBatch, []*txmgr.TxStatInfo, error) {
	if infolab.DebugMode && infolab.ValidateAndPrepare {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(core/ledger/kvledger/txmgmt/validator/valimpl/default_impl.go) ValidateAndPrepareBatch from %s#%d\n", file, no)
		}
	}
	block := blockAndPvtdata.Block
	logger.Debugf("ValidateAndPrepareBatch() for block number = [%d]", block.Header.Number)
	var internalBlock *internalVal.Block
	var txsStatInfo []*txmgr.TxStatInfo
	var pubAndHashUpdates *internalVal.PubAndHashUpdates
	var pvtUpdates *privacyenabledstate.PvtUpdateBatch
	var reexecutedTxIndices, badTxIndices []uint64
	var err error

	logger.Debug("preprocessing ProtoBlock...")
	if internalBlock, txsStatInfo, err = preprocessProtoBlock(impl.txmgr, impl.db.ValidateKeyValue, block, blockAndPvtdata.UnblockedTxs, doMVCCValidation); err != nil {
		return nil, nil, err
	}

	if pubAndHashUpdates, reexecutedTxIndices, badTxIndices, err = impl.internalValidator.ValidateAndPrepareBatch(internalBlock, doMVCCValidation, committedTxs); err != nil {
		return nil, nil, err
	}
	logger.Debug("validating rwset...")
	if pvtUpdates, err = validateAndPreparePvtBatch(internalBlock, impl.db, pubAndHashUpdates, blockAndPvtdata.PvtData); err != nil {
		return nil, nil, err
	}
	logger.Debug("postprocessing ProtoBlock...")
	postprocessProtoBlock(block, internalBlock)
	logger.Debug("ValidateAndPrepareBatch() complete")
	if infolab.DebugMode && infolab.CheckMetric {
		fmt.Printf("txsStatInfo length is %d\n", len(txsStatInfo))
	}
	txsFilter := util.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	for i := range txsFilter {
		txsStatInfo[i].ValidationCode = txsFilter.Flag(i)
	}
	logger.Infof("블록 %d에서: (재시뮬레이션 %d, 실패 %d)", block.Header.Number, len(reexecutedTxIndices), len(badTxIndices))
	for _, v := range reexecutedTxIndices {
		txsStatInfo[v].ValidationCode = peer.TxValidationCode_RESIMULATED
	}
	for _, v := range badTxIndices {
		txsStatInfo[v].ValidationCode = peer.TxValidationCode_BAD_RWSET
	}
	return &privacyenabledstate.UpdateBatch{
		PubUpdates:  pubAndHashUpdates.PubUpdates,
		HashUpdates: pubAndHashUpdates.HashUpdates,
		PvtUpdates:  pvtUpdates,
	}, txsStatInfo, nil
}
