/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockcutter

import (
	"fmt"
	"runtime"
	"time"

	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/infolab"
	cb "github.com/hyperledger/fabric/protos/common"
)

var logger = flogging.MustGetLogger("orderer.common.blockcutter")

type OrdererConfigFetcher interface {
	OrdererConfig() (channelconfig.Orderer, bool)
}

// Receiver defines a sink for the ordered broadcast messages
type Receiver interface {
	// Ordered should be invoked sequentially as messages are ordered
	// Each batch in `messageBatches` will be wrapped into a block.
	// `pending` indicates if there are still messages pending in the receiver.
	Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool)
	OrderedParallel(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool)

	// Cut returns the current batch and starts a new one
	Cut() []*cb.Envelope
	CutParallel() []*cb.Envelope

	GetPreviousPendingBatchStartTime() *time.Time
	SetPreviousPendingBatchStartTime(value *time.Time)
}

type receiver struct {
	sharedConfigFetcher   OrdererConfigFetcher
	pendingBatch          []*cb.Envelope
	pendingBatchSizeBytes uint32
	// pendingBatchWaitGroups        *sync.WaitGroup
	previousPendingBatchStartTime *time.Time

	PendingBatchStartTime time.Time
	ChannelID             string
	Metrics               *Metrics
}

// NewReceiverImpl creates a Receiver implementation based on the given configtxorderer manager
func NewReceiverImpl(channelID string, sharedConfigFetcher OrdererConfigFetcher, metrics *Metrics) Receiver {
	return &receiver{
		sharedConfigFetcher: sharedConfigFetcher,
		// pendingBs: &sync.WaitGroup{},
		Metrics:   metrics,
		ChannelID: channelID,
	}
}

// Ordered should be invoked sequentially as messages are ordered
//
// messageBatches length: 0, pending: false
//   - impossible, as we have just received a message
// messageBatches length: 0, pending: true
//   - no batch is cut and there are messages pending
// messageBatches length: 1, pending: false
//   - the message count reaches BatchSize.MaxMessageCount
// messageBatches length: 1, pending: true
//   - the current message will cause the pending batch size in bytes to exceed BatchSize.PreferredMaxBytes.
// messageBatches length: 2, pending: false
//   - the current message size in bytes exceeds BatchSize.PreferredMaxBytes, therefore isolated in its own batch.
// messageBatches length: 2, pending: true
//   - impossible
//
// Note that messageBatches can not be greater than 2.
func (r *receiver) Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool) {
	// if infolab.DebugMode && infolab.TraceOrderer {
	// 	_, file, no, ok := runtime.Caller(1)
	// 	if ok {
	// 		fmt.Printf("(orderer/common/blockcutter/blockcutter.go) Ordered() called from %s#%d\n", file, no)
	// 	}
	// }
	if len(r.pendingBatch) == 0 {
		// We are beginning a new batch, mark the time
		r.PendingBatchStartTime = time.Now()
	}

	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}

	batchSize := ordererConfig.BatchSize()

	// r.pendingBatchWaitGroups.Add(1)
	// go func() {
	// 	msg.Partition = []byte{infolab.PartitionOnce(msg)}
	// 	r.pendingBatchWaitGroups.Done()
	// }()

	messageSizeBytes := messageSizeBytes(msg)
	if messageSizeBytes > batchSize.PreferredMaxBytes {
		logger.Debugf("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, batchSize.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		if len(r.pendingBatch) > 0 {
			messageBatch := r.Cut()
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		messageBatches = append(messageBatches, []*cb.Envelope{msg})

		// Record that this batch took no time to fill
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(0)

		return
	}

	messageWillOverflowBatchSizeBytes := r.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes

	if messageWillOverflowBatchSizeBytes {
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, r.pendingBatchSizeBytes)
		logger.Debugf("Pending batch would overflow if current message is added, cutting batch now.")
		messageBatch := r.Cut()
		r.PendingBatchStartTime = time.Now()
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")
	r.pendingBatch = append(r.pendingBatch, msg)
	r.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	if uint32(len(r.pendingBatch)) >= batchSize.MaxMessageCount {
		if infolab.DebugMode && infolab.TraceOrderer {
			_, file, no, ok := runtime.Caller(1)
			if ok {
				fmt.Printf("(orderer/common/blockcutter/blockcutter.go) Ordered() called from %s#%d\n", file, no)
				// fmt.Printf("batchSize.MaxMessageCount is %d\n", batchSize.MaxMessageCount)
			}
		}
		logger.Debugf("Batch size met, cutting batch")
		messageBatch := r.Cut()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return
}

func (r *receiver) OrderedParallel(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool) {
	// if infolab.DebugMode && infolab.TraceOrderer {
	// 	_, file, no, ok := runtime.Caller(1)
	// 	if ok {
	// 		fmt.Printf("(orderer/common/blockcutter/blockcutter.go) Ordered() called from %s#%d\n", file, no)
	// 	}
	// }
	if len(r.pendingBatch) == 0 {
		// We are beginning a new batch, mark the time
		r.PendingBatchStartTime = time.Now()
	}

	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}

	batchSize := ordererConfig.BatchSize()

	// r.pendingBatchWaitGroups.Add(1)
	// go func() {
	// 	msg.Partition = []byte{infolab.PartitionOnce(msg)}
	// 	r.pendingBatchWaitGroups.Done()
	// }()

	messageSizeBytes := messageSizeBytes(msg)
	if messageSizeBytes > batchSize.PreferredMaxBytes {
		infolab.Log.Printf("messageSizeBytes > batchSize.PreferredMaxBytes")
		logger.Debugf("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, batchSize.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		if len(r.pendingBatch) > 0 {
			messageBatch := r.CutParallel()
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		messageBatches = append(messageBatches, []*cb.Envelope{msg})

		// Record that this batch took no time to fill
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(0)

		return
	}

	messageWillOverflowBatchSizeBytes := r.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes

	if messageWillOverflowBatchSizeBytes {
		infolab.Log.Printf("messageWillOverflowBatchSizeBytes")
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, r.pendingBatchSizeBytes)
		logger.Debugf("Pending batch would overflow if current message is added, cutting batch now.")
		messageBatch := r.CutParallel()
		r.PendingBatchStartTime = time.Now()
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")
	r.pendingBatch = append(r.pendingBatch, msg)
	r.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	if uint32(len(r.pendingBatch)) >= batchSize.MaxMessageCount {
		infolab.Log.Printf("Over message count")
		if infolab.DebugMode && infolab.TraceOrderer {
			_, file, no, ok := runtime.Caller(1)
			if ok {
				fmt.Printf("(orderer/common/blockcutter/blockcutter.go) Ordered() called from %s#%d\n", file, no)
				// fmt.Printf("batchSize.MaxMessageCount is %d\n", batchSize.MaxMessageCount)
			}
		}
		logger.Debugf("Batch size met, cutting batch")
		messageBatch := r.CutParallel()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return
}

// Cut returns the current batch and starts a new one
func (r *receiver) Cut() []*cb.Envelope {
	if infolab.DebugMode && infolab.TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(orderer/common/blockcutter/blockcutter.go) Cut() called from %s#%d\n", file, no)
		}
	}
	r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(time.Since(r.PendingBatchStartTime).Seconds())
	r.PendingBatchStartTime = time.Time{}
	batch := r.pendingBatch
	r.pendingBatch = nil
	r.pendingBatchSizeBytes = 0
	// r.pendingBatchWaitGroups.Wait()
	return batch
	// infolab.UpdateDegree(batch)

	// return infolab.OrderBatch(r.Metrics.BlockRearrangeDuration, batch)
}

func (r *receiver) CutParallel() []*cb.Envelope {
	if infolab.DebugMode && infolab.TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(orderer/common/blockcutter/blockcutter.go) Cut() called from %s#%d\n", file, no)
		}
	}
	r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(time.Since(r.PendingBatchStartTime).Seconds())
	r.PendingBatchStartTime = time.Time{}
	batch := r.pendingBatch
	r.pendingBatch = nil
	r.pendingBatchSizeBytes = 0
	// r.pendingBatchWaitGroups.Wait()
	return batch
	// infolab.UpdateDegree(batch)

	// return infolab.OrderBatch(r.Metrics.BlockRearrangeDuration, batch)
}

func messageSizeBytes(message *cb.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}

func (r *receiver) GetPreviousPendingBatchStartTime() *time.Time {
	return r.previousPendingBatchStartTime
}
func (r *receiver) SetPreviousPendingBatchStartTime(value *time.Time) {
	r.previousPendingBatchStartTime = value
}
