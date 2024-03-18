/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package multichannel

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	newchannelconfig "github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/crypto"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/infolab"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

type blockWriterSupport interface {
	crypto.LocalSigner
	blockledger.ReadWriter
	configtx.Validator
	Update(*newchannelconfig.Bundle)
	CreateBundle(channelID string, config *cb.Config) (*newchannelconfig.Bundle, error)
}

// BlockWriter efficiently writes the blockchain to disk.
// To safely use BlockWriter, only one thread should interact with it.
// BlockWriter will spawn additional committing go routines and handle locking
// so that these other go routines safely interact with the calling one.
type BlockWriter struct {
	support            blockWriterSupport
	registrar          *Registrar
	lastConfigBlockNum uint64
	lastConfigSeq      uint64
	lastBlock          *cb.Block
	committingBlock    sync.Mutex
}

func newBlockWriter(lastBlock *cb.Block, r *Registrar, support blockWriterSupport) *BlockWriter {
	bw := &BlockWriter{
		support:       support,
		lastConfigSeq: support.Sequence(),
		lastBlock:     lastBlock,
		registrar:     r,
	}

	// If this is the genesis block, the lastconfig field may be empty, and, the last config block is necessarily block 0
	// so no need to initialize lastConfig
	if lastBlock.Header.Number != 0 {
		var err error
		bw.lastConfigBlockNum, err = utils.GetLastConfigIndexFromBlock(lastBlock)
		if err != nil {
			logger.Panicf("[channel: %s] Error extracting last config block from block metadata: %s", support.ChainID(), err)
		}
	}

	logger.Debugf("[channel: %s] Creating block writer for tip of chain (blockNumber=%d, lastConfigBlockNum=%d, lastConfigSeq=%d)", support.ChainID(), lastBlock.Header.Number, bw.lastConfigBlockNum, bw.lastConfigSeq)
	return bw
}

var createNextBlockCallCount int64
var createNextBlockTotalTime int64

// CreateNextBlock creates a new block with the next block number, and the given contents.
func (bw *BlockWriter) CreateNextBlock(messages []*cb.Envelope) *cb.Block {
	createNextBlockCallCount++
	startTime := time.Now()
	if infolab.DebugMode && infolab.TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(orderer/common/multichannel/blockwriter.go) CreateNextBlock() called from %s#%d\n", file, no)
		}
	}
	previousBlockHash := bw.lastBlock.Header.Hash()

	data := &cb.BlockData{
		Data: make([][]byte, len(messages)),
	}

	var err error
	for i, msg := range messages {
		data.Data[i], err = proto.Marshal(msg)
		if err != nil {
			logger.Panicf("Could not marshal envelope: %s", err)
		}
	}

	block := cb.NewBlock(bw.lastBlock.Header.Number+1, previousBlockHash)
	block.Header.DataHash = data.Hash()

	// createNextBlock_step1 := time.Since(startTime).Nanoseconds()
	if infolab.PartialFunctionOrdering == false || infolab.Partitions > 1 {
		block.Metadata.Metadata[cb.BlockMetadataIndex_PARTITIONS] = infolab.PartitionBatch(messages)
	}
	block.Data = data
	// createNextBlock_step2 := (time.Since(startTime) - time.Duration(createNextBlock_step1)).Nanoseconds()
	takenTime := time.Since(startTime)
	createNextBlockTotalTime += takenTime.Nanoseconds()

	if createNextBlockCallCount > 770 {
		// fmt.Printf("** Thousandth msg ordered duration average: %d\n", thousandthMsgOrderedDurationAverage/3000)
		infolab.Log.Printf("CreateNextBlockTime:%d\n", createNextBlockTotalTime/createNextBlockCallCount)
	}

	return block
}

// CreateNextBlockParallel creates a new block with the next block number, and the given contents.
func (bw *BlockWriter) CreateNextBlockParallel(messages []*cb.Envelope, partition_info []byte, prevBlockHash []byte, prevBlockNum uint64) *cb.Block {
	// startTime := time.Now()
	if infolab.DebugMode && infolab.TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(orderer/common/multichannel/blockwriter.go) CreateNextBlock() called from %s#%d\n", file, no)
		}
	}

	// fmt.Printf("Checkpoint 2\n")
	var previousBlockHeaderNumber uint64
	var previousBlockHash []byte
	if prevBlockHash == nil {
		previousBlockHash = bw.lastBlock.Header.Hash()
	} else {
		previousBlockHash = prevBlockHash
	}
	// fmt.Printf("Checkpoint 3\n")
	if prevBlockNum == 0 {
		previousBlockHeaderNumber = bw.lastBlock.Header.Number
		// fmt.Printf("(Not found) PreviousBlockNumber: %d\n", previousBlockHeaderNumber)
	} else {
		previousBlockHeaderNumber = prevBlockNum
		// fmt.Printf("(found) PreviousBlockNumber: %d\n", previousBlockHeaderNumber)
	}

	data := &cb.BlockData{
		Data: make([][]byte, len(messages)),
	}

	var err error
	for i, msg := range messages {
		data.Data[i], err = proto.Marshal(msg)
		if err != nil {
			logger.Panicf("Could not marshal envelope: %s", err)
		}
	}

	block := cb.NewBlock(previousBlockHeaderNumber+1, previousBlockHash)
	block.Header.DataHash = data.Hash()

	// createNextBlock_step1 := time.Since(startTime).Nanoseconds()
	block.Metadata.Metadata[cb.BlockMetadataIndex_PARTITIONS] = partition_info
	block.Data = data
	// createNextBlock_step2 := (time.Since(startTime) - time.Duration(createNextBlock_step1)).Nanoseconds()

	return block
}

// WriteConfigBlock should be invoked for blocks which contain a config transaction.
// This call will block until the new config has taken effect, then will return
// while the block is written asynchronously to disk.
func (bw *BlockWriter) WriteConfigBlock(block *cb.Block, encodedMetadataValue []byte) {
	ctx, err := utils.ExtractEnvelope(block, 0)
	if err != nil {
		logger.Panicf("Told to write a config block, but could not get configtx: %s", err)
	}

	payload, err := utils.UnmarshalPayload(ctx.Payload)
	if err != nil {
		logger.Panicf("Told to write a config block, but configtx payload is invalid: %s", err)
	}

	if payload.Header == nil {
		logger.Panicf("Told to write a config block, but configtx payload header is missing")
	}

	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		logger.Panicf("Told to write a config block with an invalid channel header: %s", err)
	}

	switch chdr.Type {
	case int32(cb.HeaderType_ORDERER_TRANSACTION):
		newChannelConfig, err := utils.UnmarshalEnvelope(payload.Data)
		if err != nil {
			logger.Panicf("Told to write a config block with new channel, but did not have config update embedded: %s", err)
		}
		bw.registrar.newChain(newChannelConfig)
	case int32(cb.HeaderType_CONFIG):
		configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
		if err != nil {
			logger.Panicf("Told to write a config block with new channel, but did not have config envelope encoded: %s", err)
		}

		err = bw.support.Validate(configEnvelope)
		if err != nil {
			logger.Panicf("Told to write a config block with new config, but could not apply it: %s", err)
		}

		bundle, err := bw.support.CreateBundle(chdr.ChannelId, configEnvelope.Config)
		if err != nil {
			logger.Panicf("Told to write a config block with a new config, but could not convert it to a bundle: %s", err)
		}

		// Avoid Bundle update before the go-routine in WriteBlock() finished writing the previous block.
		// We do this (in particular) to prevent bw.support.Sequence() from advancing before the go-routine reads it.
		// In general, this prevents the StableBundle from changing before the go-routine in WriteBlock() finishes.
		bw.committingBlock.Lock()
		bw.committingBlock.Unlock()
		bw.support.Update(bundle)
	default:
		logger.Panicf("Told to write a config block with unknown header type: %v", chdr.Type)
	}

	bw.WriteBlock(block, encodedMetadataValue)
}

// WriteBlock should be invoked for blocks which contain normal transactions.
// It sets the target block as the pending next block, and returns before it is committed.
// Before returning, it acquires the committing lock, and spawns a go routine which will
// annotate the block with metadata and signatures, and write the block to the ledger
// then release the lock.  This allows the calling thread to begin assembling the next block
// before the commit phase is complete.
func (bw *BlockWriter) WriteBlock(block *cb.Block, encodedMetadataValue []byte) {
	bw.committingBlock.Lock()
	bw.lastBlock = block

	go func() {
		defer bw.committingBlock.Unlock()
		bw.commitBlock(encodedMetadataValue)
	}()
}

// WriteBlock should be invoked for blocks which contain normal transactions.
// It sets the target block as the pending next block, and returns before it is committed.
// Before returning, it acquires the committing lock, and spawns a go routine which will
// annotate the block with metadata and signatures, and write the block to the ledger
// then release the lock.  This allows the calling thread to begin assembling the next block
// before the commit phase is complete.
func (bw *BlockWriter) WriteBlockParallel(block *cb.Block, encodedMetadataValue []byte, lastBlock *cb.Block) {
	bw.committingBlock.Lock()
	bw.lastBlock = block

	go func() {
		defer bw.committingBlock.Unlock()
		bw.commitBlockParallel(encodedMetadataValue, lastBlock)
	}()
}

var Orderer_commitBlockCount int64
var Orderer_totalCommitBlocktime int64

// commitBlock should only ever be invoked with the bw.committingBlock held
// this ensures that the encoded config sequence numbers stay in sync
func (bw *BlockWriter) commitBlock(encodedMetadataValue []byte) {
	Orderer_commitBlockCount++
	startTime := time.Now()

	// Set the orderer-related metadata field
	if encodedMetadataValue != nil {
		bw.lastBlock.Metadata.Metadata[cb.BlockMetadataIndex_ORDERER] = utils.MarshalOrPanic(&cb.Metadata{Value: encodedMetadataValue})
	}

	bw.addLastConfigSignature(bw.lastBlock)
	bw.addBlockSignature(bw.lastBlock)

	err := bw.support.Append(bw.lastBlock)
	if infolab.DebugMode && infolab.TraceOrdererWriteBlock {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(orderer/common/multichannel/blockwriter.go) commitBlock called from %s#%d\n", file, no)
		}
	}
	if err != nil {
		logger.Panicf("[channel: %s] Could not append block: %s", bw.support.ChainID(), err)
	}
	logger.Debugf("[channel: %s] Wrote block [%d]", bw.support.ChainID(), bw.lastBlock.GetHeader().Number)

	takenTime := time.Since(startTime)
	if infolab.DebugMode && infolab.Orderer_commitBlockTime {
		fmt.Printf("[Orderer_commitBlock %d] Time taken: %d\n", Orderer_commitBlockCount, takenTime.Nanoseconds())
		if Orderer_commitBlockCount >= 301 {
			Orderer_totalCommitBlocktime += takenTime.Nanoseconds()
		}
		if Orderer_commitBlockCount == 3300 {
			avgTimeTaken := Orderer_totalCommitBlocktime / 3000
			fmt.Printf("** Orderer_commitBlock Average is: %d\n", avgTimeTaken)
		}
	}
}

func (bw *BlockWriter) commitBlockParallel(encodedMetadataValue []byte, lastBlock *cb.Block) {
	var err error
	Orderer_commitBlockCount++
	startTime := time.Now()

	// Set the orderer-related metadata field
	if encodedMetadataValue != nil {
		bw.lastBlock.Metadata.Metadata[cb.BlockMetadataIndex_ORDERER] = utils.MarshalOrPanic(&cb.Metadata{Value: encodedMetadataValue})
		if lastBlock != nil {
			lastBlock.Metadata.Metadata[cb.BlockMetadataIndex_ORDERER] = bw.lastBlock.Metadata.Metadata[cb.BlockMetadataIndex_ORDERER]
		}
	}

	if lastBlock == nil {
		bw.addLastConfigSignature(bw.lastBlock)
		bw.addBlockSignature(bw.lastBlock)
		err = bw.support.Append(bw.lastBlock)
	} else {
		bw.addLastConfigSignature(lastBlock)
		bw.addBlockSignature(lastBlock)
		err = bw.support.Append(lastBlock)

	}

	if infolab.DebugMode && infolab.TraceOrdererWriteBlock {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(orderer/common/multichannel/blockwriter.go) commitBlock called from %s#%d\n", file, no)
		}
	}
	if err != nil {
		logger.Panicf("[channel: %s] Could not append block: %s", bw.support.ChainID(), err)
	}
	logger.Debugf("[channel: %s] Wrote block [%d]", bw.support.ChainID(), bw.lastBlock.GetHeader().Number)

	takenTime := time.Since(startTime)
	if infolab.DebugMode && infolab.Orderer_commitBlockTime {
		fmt.Printf("[Orderer_commitBlock %d] Time taken: %d\n", Orderer_commitBlockCount, takenTime.Nanoseconds())
		if Orderer_commitBlockCount >= 301 {
			Orderer_totalCommitBlocktime += takenTime.Nanoseconds()
		}
		if Orderer_commitBlockCount == 3300 {
			avgTimeTaken := Orderer_totalCommitBlocktime / 3000
			fmt.Printf("** Orderer_commitBlock Average is: %d\n", avgTimeTaken)
		}
	}
}

func (bw *BlockWriter) addBlockSignature(block *cb.Block) {
	blockSignature := &cb.MetadataSignature{
		SignatureHeader: utils.MarshalOrPanic(utils.NewSignatureHeaderOrPanic(bw.support)),
	}

	blockSignatureValue := utils.MarshalOrPanic(&cb.OrdererBlockMetadata{
		LastConfig:        &cb.LastConfig{Index: bw.lastConfigBlockNum},
		ConsenterMetadata: bw.lastBlock.Metadata.Metadata[cb.BlockMetadataIndex_ORDERER],
	})

	blockSignature.Signature = utils.SignOrPanic(bw.support, util.ConcatenateBytes(blockSignatureValue, blockSignature.SignatureHeader, block.Header.Bytes()))

	block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = utils.MarshalOrPanic(&cb.Metadata{
		Value: blockSignatureValue,
		Signatures: []*cb.MetadataSignature{
			blockSignature,
		},
	})
}

func (bw *BlockWriter) addLastConfigSignature(block *cb.Block) {
	configSeq := bw.support.Sequence()
	if configSeq > bw.lastConfigSeq {
		logger.Debugf("[channel: %s] Detected lastConfigSeq transitioning from %d to %d, setting lastConfigBlockNum from %d to %d", bw.support.ChainID(), bw.lastConfigSeq, configSeq, bw.lastConfigBlockNum, block.Header.Number)
		bw.lastConfigBlockNum = block.Header.Number
		bw.lastConfigSeq = configSeq
	}

	lastConfigValue := utils.MarshalOrPanic(&cb.LastConfig{Index: bw.lastConfigBlockNum})
	logger.Debugf("[channel: %s] About to write block, setting its LAST_CONFIG to %d", bw.support.ChainID(), bw.lastConfigBlockNum)

	block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = utils.MarshalOrPanic(&cb.Metadata{
		Value: lastConfigValue,
	})
}
