/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/infolab"

	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/gossip/util"
)

// PayloadsBuffer is used to store payloads into which used to
// support payloads with blocks reordering according to the
// sequence numbers. It also will provide the capability
// to signal whenever expected block has arrived.
type PayloadsBuffer interface {
	// Adds new block into the buffer
	Push(payload *cached.GossipPayload)

	// Returns next expected sequence number
	Next() uint64

	// Remove and return payload with given sequence number
	Pop() *cached.GossipPayload

	// Get current buffer size
	Size() int

	// Channel to indicate event when new payload pushed with sequence
	// number equal to the next expected value.
	Ready() chan struct{}

	Close()
}

// PayloadsBufferImpl structure to implement PayloadsBuffer interface
// store inner state of available payloads and sequence numbers
type PayloadsBufferImpl struct {
	next uint64

	buf map[uint64]*cached.GossipPayload

	readyChan chan struct{}

	mutex sync.RWMutex

	logger util.Logger
}

// NewPayloadsBuffer is factory function to create new payloads buffer
func NewPayloadsBuffer(next uint64) PayloadsBuffer {
	return &PayloadsBufferImpl{
		buf:       make(map[uint64]*cached.GossipPayload),
		readyChan: make(chan struct{}, 1),
		next:      next,
		logger:    util.GetLogger(util.StateLogger, ""),
	}
}

// Ready function returns the channel which indicates whenever expected
// next block has arrived and one could safely pop out
// next sequence of blocks
func (b *PayloadsBufferImpl) Ready() chan struct{} {
	return b.readyChan
}

// Push new payload into the buffer structure in case new arrived payload
// sequence number is below the expected next block number payload will be
// thrown away.
// TODO return bool to indicate if payload was added or not, so that caller can log result.
func (b *PayloadsBufferImpl) Push(payload *cached.GossipPayload) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	seqNum := payload.Data.Header.Number

	if seqNum < b.next || b.buf[seqNum] != nil {
		logger.Debugf("Payload with sequence number = %d has been already processed", seqNum)
		return
	}

	// if seqNum > 10 {
	// 	return
	// }
	b.buf[seqNum] = payload
	// fmt.Printf("Push: %d (대기: %d)\n", seqNum, len(b.buf))

	// Send notification that next sequence has arrived
	if seqNum == b.next && len(b.readyChan) == 0 {
		b.readyChan <- struct{}{}
	}
}

// Next function provides the number of the next expected block
func (b *PayloadsBufferImpl) Next() uint64 {
	// Atomically read the value of the top sequence number
	return atomic.LoadUint64(&b.next)
}

// Pop function extracts the payload according to the next expected block
// number, if no next block arrived yet, function returns nil.
func (b *PayloadsBufferImpl) Pop() *cached.GossipPayload {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	result := b.buf[b.Next()]

	if result != nil {
		// If there is such sequence in the buffer need to delete it
		delete(b.buf, b.Next())
		// Increment next expect block index
		atomic.AddUint64(&b.next, 1)

		b.drainReadChannel()

	}

	return result
}

// drainReadChannel empties ready channel in case last
// payload has been poped up and there are still awaiting
// notifications in the channel
func (b *PayloadsBufferImpl) drainReadChannel() {
	if len(b.buf) == 0 {
		for {
			if len(b.readyChan) > 0 {
				<-b.readyChan
			} else {
				break
			}
		}
	}
}

// Size returns current number of payloads stored within buffer
func (b *PayloadsBufferImpl) Size() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return len(b.buf)
}

// Close cleanups resources and channels in maintained
func (b *PayloadsBufferImpl) Close() {
	close(b.readyChan)
}

type metricsBuffer struct {
	PayloadsBuffer
	sizeMetrics metrics.Gauge
	chainID     string
}

func (mb *metricsBuffer) Push(payload *cached.GossipPayload) {
	if infolab.DebugMode && infolab.GossipPayload {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(gossip/state/payloads_buffer.go) Push called from %s#%d\n", file, no)
		}
	}
	mb.PayloadsBuffer.Push(payload)
	mb.reportSize()
}

func (mb *metricsBuffer) Pop() *cached.GossipPayload {
	pl := mb.PayloadsBuffer.Pop()
	mb.reportSize()
	return pl
}

func (mb *metricsBuffer) reportSize() {
	mb.sizeMetrics.With("channel", mb.chainID).Set(float64(mb.Size()))
}
