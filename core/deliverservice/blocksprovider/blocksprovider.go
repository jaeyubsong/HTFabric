/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/fastfabric/config"
	"github.com/hyperledger/fabric/fastfabric/gossip"
	"github.com/hyperledger/fabric/infolab"
	"github.com/hyperledger/fabric/infolab/simulating"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/gossip/api"
	gossipcommon "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/hyperledger/fabric/protos/common"
	gossip_proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/hyperledger/fabric/protos/orderer"

)

// LedgerInfo an adapter to provide the interface to query
// the ledger committer for current ledger height
type LedgerInfo interface {
	// LedgerHeight returns current local ledger height
	LedgerHeight() (uint64, error)
}

// GossipServiceAdapter serves to provide basic functionality
// required from gossip service by delivery service
type GossipServiceAdapter interface {
	// PeersOfChannel returns slice with members of specified channel
	PeersOfChannel(gossipcommon.ChainID) []discovery.NetworkMember

	// AddPayload adds payload to the local state sync buffer
	AddPayload(chainID string, payload *cached.GossipPayload) error

	// Gossip the message across the peers
	Gossip(msg *gossip_proto.GossipMessage)
}

// BlocksProvider used to read blocks from the ordering service
// for specified chain it subscribed to
type BlocksProvider interface {
	// DeliverBlocks starts delivering and disseminating blocks
	DeliverBlocks()

	// UpdateClientEndpoints update endpoints
	UpdateOrderingEndpoints(endpoints []string)

	// Stop shutdowns blocks provider and stops delivering new blocks
	Stop()
}

// BlocksDeliverer defines interface which actually helps
// to abstract the AtomicBroadcast_DeliverClient with only
// required method for blocks provider.
// This also decouples the production implementation of the gRPC stream
// from the code in order for the code to be more modular and testable.
type BlocksDeliverer interface {
	// Recv retrieves a response from the ordering service
	Recv() (*orderer.DeliverResponse, error)

	// Send sends an envelope to the ordering service
	Send(*common.Envelope) error
}

type streamClient interface {
	BlocksDeliverer

	// UpdateEndpoint update ordering service endpoints
	UpdateEndpoints(endpoints []string)

	// GetEndpoints
	GetEndpoints() []string

	// Close closes the stream and its underlying connection
	Close()

	// Disconnect disconnects from the remote node and disable reconnect to current endpoint for predefined period of time
	Disconnect(disableEndpoint bool)
}

// blocksProviderImpl the actual implementation for BlocksProvider interface
type blocksProviderImpl struct {
	chainID string

	client streamClient

	gossip GossipServiceAdapter

	mcs api.MessageCryptoService

	done int32

	wrongStatusThreshold int
}

const wrongStatusThreshold = 10

var maxRetryDelay = time.Second * 10
var logger = flogging.MustGetLogger("blocksProvider")

var LatestBlock uint64
var ValidatedChan chan bool



// NewBlocksProvider constructor function to create blocks deliverer instance
func NewBlocksProvider(chainID string, client streamClient, gossip GossipServiceAdapter, mcs api.MessageCryptoService) BlocksProvider {
	return &blocksProviderImpl{
		chainID:              chainID,
		client:               client,
		gossip:               gossip,
		mcs:                  mcs,
		wrongStatusThreshold: wrongStatusThreshold,
	}
}

// DeliverBlocks used to pull out blocks from the ordering service to
// distributed them across peers
func (b *blocksProviderImpl) DeliverBlocks() {

	if ValidatedChan == nil {
		fmt.Printf("Created ValidChan\n")
		ValidatedChan = make(chan bool, 100000)
		// for i := 0; i < infolab.DsQueueSize; i++ {
		// 	ValidatedChan <- true
		// }
		// ValidatedChan <- true
	}
	var bbHighest uint64 = 1
	var bbRequired uint64 = 2
	var blockBox = map[uint64]*gossip_proto.GossipMessage{}
	var lock = sync.Mutex{}

	errorStatusCounter := 0
	statusCounter := 0
	defer b.client.Close()
	for !b.isDone() {
		msg, err := b.client.Recv()
		if err != nil {
			logger.Warningf("[%s] Receive error: %s", b.chainID, err.Error())
			return
		}
		switch t := msg.Type.(type) {
		case *orderer.DeliverResponse_Status:
			if t.Status == common.Status_SUCCESS {
				logger.Warningf("[%s] ERROR! Received success for a seek that should never complete", b.chainID)
				return
			}
			if t.Status == common.Status_BAD_REQUEST || t.Status == common.Status_FORBIDDEN {
				logger.Errorf("[%s] Got error %v", b.chainID, t)
				errorStatusCounter++
				if errorStatusCounter > b.wrongStatusThreshold {
					logger.Criticalf("[%s] Wrong statuses threshold passed, stopping block provider", b.chainID)
					return
				}
			} else {
				errorStatusCounter = 0
				logger.Warningf("[%s] Got error %v", b.chainID, t)
			}
			maxDelay := float64(maxRetryDelay)
			currDelay := float64(time.Duration(math.Pow(2, float64(statusCounter))) * 100 * time.Millisecond)
			time.Sleep(time.Duration(math.Min(maxDelay, currDelay)))
			if currDelay < maxDelay {
				statusCounter++
			}
			if t.Status == common.Status_BAD_REQUEST {
				b.client.Disconnect(false)
			} else {
				b.client.Disconnect(true)
			}
			continue
		case *orderer.DeliverResponse_Block:
			errorStatusCounter = 0
			statusCounter = 0
			block := cached.WrapBlock(t.Block)
			blockNum := block.Header.Number
			if blockNum > 1 && (config.IsEndorser || config.IsStorage) {
				continue
			}
			fmt.Printf("Receive block from orderer: %5d\n", blockNum)
			if infolab.DebugMode && infolab.GossipPayload {
				fmt.Printf("Receive block from orderer: %5d\n", blockNum)
			}
			// fmt.Printf("오더러로부터 받음: %5d\n", blockNum)
			// logger.Infof("received block [%d]", blockNum)

			// time.Sleep(20 * time.Millisecond)

			// 여기서 valid 이후에 블록이 도착하면 바로 보낸다
			if simulating.FastWbChan == nil {
				simulating.FastWbChan = make(chan simulating.GossipMessageWithBlockNum, 10)
			}




			go func() {
				if infolab.FastWb {
					for fastWbMsg := range simulating.FastWbChan {
						simulating.GossipFastWb(b.gossip, fastWbMsg.BlockNum, fastWbMsg.GossipMsg)
					}
				}
			}()
			defer close(simulating.FastWbChan)




			if err := b.mcs.VerifyBlock(gossipcommon.ChainID(b.chainID), blockNum, block); err != nil {
				logger.Errorf("[%s] Error verifying block with sequnce number %d, due to %s", b.chainID, blockNum, err)
				continue
			}

			// numberOfPeers := len(b.gossip.PeersOfChannel(gossipcommon.ChainID(b.chainID)))
			// Create payload with a block received
			payload := createPayload(block)

			logger.Debugf("[%s] Adding payload to local buffer, blockNum = [%d]", b.chainID, blockNum)
			// Add payload to local state payloads buffer
			// JNOTE 원래는 여기서 블록이 큐에 들어가고 나중에 처리된다.
			if err := b.gossip.AddPayload(b.chainID, payload); err != nil {
				logger.Warningf("Block [%d] received from ordering service wasn't added to payload buffer: %v", blockNum, err)
			}
			if !(blockNum > 1 && (config.IsEndorser || config.IsStorage)) {
				gossipChan := gossip.GetQueue(blockNum)

				go func(c chan *gossip_proto.Payload, bn uint64) {
					// Gossip messages with other nodes
					// fmt.Printf("[%s] Gossiping block [%d], peers number [%d]\n", b.chainID, blockNum, numberOfPeers)
					// Use payload to create gossip message
					gossipMsg := createGossipMsg(b.chainID, <-c)
					if b.isDone() {
						return
					}
					lock.Lock()
					if bbRequired == bn {
						simulating.GossipOnCatchUp(b.gossip, bn, gossipMsg)
						bbRequired++
						for i := bbRequired; i <= bbHighest; i++ {
							if msg, ok := blockBox[i]; ok {
								simulating.GossipOnCatchUp(b.gossip, i, msg)
								blockBox[i] = nil
								delete(blockBox, i)
								bbRequired++
							} else {
								break
							}
						}
					} else {
						// queue := len(blockBox)
						if bbHighest < bn {
							bbHighest = bn
						}
						blockBox[bn] = gossipMsg
						// fmt.Printf("블록 대기: %d (%d, 길이 %d)\n", blockNum, bbRequired, len(blockBox))
						// if queue > 30000 {
						// 	panic("너무 많이 대기함...")
						// }
					}
					gossip.RemoveQueue(bn)
					lock.Unlock()
				}(gossipChan, blockNum)
			}

		default:
			logger.Warningf("[%s] Received unknown: %v", b.chainID, t)
			return
		}
	}
}

// Stop stops blocks delivery provider
func (b *blocksProviderImpl) Stop() {
	atomic.StoreInt32(&b.done, 1)
	b.client.Close()
}

// UpdateOrderingEndpoints update endpoints of ordering service
func (b *blocksProviderImpl) UpdateOrderingEndpoints(endpoints []string) {
	if !b.isEndpointsUpdated(endpoints) {
		// No new endpoints for ordering service were provided
		return
	}
	// We have got new set of endpoints, updating client
	logger.Debug("Updating endpoint, to %s", endpoints)
	b.client.UpdateEndpoints(endpoints)
	logger.Debug("Disconnecting so endpoints update will take effect")
	// We need to disconnect the client to make it reconnect back
	// to newly updated endpoints
	b.client.Disconnect(false)
}
func (b *blocksProviderImpl) isEndpointsUpdated(endpoints []string) bool {
	if len(endpoints) != len(b.client.GetEndpoints()) {
		return true
	}
	// Check that endpoints was actually updated
	for _, endpoint := range endpoints {
		if !util.Contains(endpoint, b.client.GetEndpoints()) {
			// Found new endpoint
			return true
		}
	}
	// Nothing has changed
	return false
}

// Check whenever provider is stopped
func (b *blocksProviderImpl) isDone() bool {
	return atomic.LoadInt32(&b.done) == 1
}

func createGossipMsg(chainID string, payload *gossip_proto.Payload) *gossip_proto.GossipMessage {
	gossipMsg := &gossip_proto.GossipMessage{
		Nonce:   0,
		Tag:     gossip_proto.GossipMessage_CHAN_AND_ORG,
		Channel: []byte(chainID),
		Content: &gossip_proto.GossipMessage_DataMsg{
			DataMsg: &gossip_proto.DataMessage{
				Payload: payload,
			},
		},
	}
	return gossipMsg
}

func createPayload(block *cached.Block) *cached.GossipPayload {
	return &cached.GossipPayload{
		Data: block,
	}
}
