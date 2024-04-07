/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/fastfabric/config"
	"github.com/hyperledger/fabric/infolab"
	"github.com/hyperledger/fabric/infolab/simulating"
	"golang.org/x/sync/semaphore"

	vsccErrors "github.com/hyperledger/fabric/common/errors"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/comm"
	common2 "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/metrics"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/hyperledger/fabric/protos/common"
	proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
)

var startTime time.Time
var lastTime time.Duration
var receivePeriod time.Duration
var receiveCount int64
var periodCount int64
var receivePeriodAverage int64

// GossipStateProvider is the interface to acquire sequences of the ledger blocks
// capable to full fill missing blocks by running state replication and
// sending request to get missing block to other nodes
type GossipStateProvider interface {
	AddPayload(payload *cached.GossipPayload) error

	// Stop terminates state transfer object
	Stop()

	// SendToFastPeer 함수는 주어진 메시지를 fast peer로 전송한다.
	SendToFastPeer(message *proto.GossipMessage) error
}

const (
	DefAntiEntropyInterval             = 5 * time.Second
	DefAntiEntropyStateResponseTimeout = 5 * time.Second
	DefAntiEntropyBatchSize            = 500

	DefChannelBufferSize     = 100
	DefAntiEntropyMaxRetries = 3

	DefMaxBlockDistance = 10

	Blocking    = true
	NonBlocking = false

	enqueueRetryInterval = time.Millisecond * 100
)

// Configuration keeps state transfer configuration parameters
type Configuration struct {
	AntiEntropyInterval             time.Duration
	AntiEntropyStateResponseTimeout time.Duration
	AntiEntropyBatchSize            uint64
	MaxBlockDistance                int
	AntiEntropyMaxRetries           int
	ChannelBufferSize               int
	EnableStateTransfer             bool
	BlockingMode                    bool
}

// GossipAdapter defines gossip/communication required interface for state provider
type GossipAdapter interface {
	// Send sends a message to remote peers
	Send(msg *proto.GossipMessage, peers ...*comm.RemotePeer)

	// Accept returns a dedicated read-only channel for messages sent by other nodes that match a certain predicate.
	// If passThrough is false, the messages are processed by the gossip layer beforehand.
	// If passThrough is true, the gossip layer doesn't intervene and the messages
	// can be used to send a reply back to the sender
	Accept(acceptor common2.MessageAcceptor, passThrough bool) (<-chan *proto.GossipMessage, <-chan proto.ReceivedMessage)

	// UpdateLedgerHeight updates the ledger height the peer
	// publishes to other peers in the channel
	UpdateLedgerHeight(height uint64, chainID common2.ChainID)

	// PeersOfChannel returns the NetworkMembers considered alive
	// and also subscribed to the channel given
	PeersOfChannel(common2.ChainID) []discovery.NetworkMember
}

// MCSAdapter adapter of message crypto service interface to bound
// specific APIs required by state transfer service
type MCSAdapter interface {
	// VerifyBlock returns nil if the block is properly signed, and the claimed seqNum is the
	// sequence number that the block's header contains.
	// else returns error
	VerifyBlock(chainID common2.ChainID, seqNum uint64, signedBlock *cached.Block) error

	// VerifyByChannel checks that signature is a valid signature of message
	// under a peer's verification key, but also in the context of a specific channel.
	// If the verification succeeded, Verify returns nil meaning no error occurred.
	// If peerIdentity is nil, then the verification fails.
	VerifyByChannel(chainID common2.ChainID, peerIdentity api.PeerIdentityType, signature, message []byte) error
}

// ledgerResources defines abilities that the ledger provides
type ledgerResources interface {
	// StoreBlock deliver new block with underlined private data
	// returns missing transaction ids
	StoreBlock(block *cached.Block, data util.PvtDataCollections) chan error

	// StorePvtData used to persist private date into transient store
	StorePvtData(txid string, privData *transientstore.TxPvtReadWriteSetWithConfigInfo, blckHeight uint64) error

	// GetPvtDataAndBlockByNum get block by number and returns also all related private data
	// the order of private data in slice of PvtDataCollections doesn't imply the order of
	// transactions in the block related to these private data, to get the correct placement
	// need to read TxPvtData.SeqInBlock field
	GetPvtDataAndBlockByNum(seqNum uint64, peerAuthInfo common.SignedData) (*cached.Block, util.PvtDataCollections, error)

	// Get recent block sequence number
	LedgerHeight() (uint64, error)

	// Close ledgerResources
	Close()
}

// ServicesMediator aggregated adapter to compound all mediator
// required by state transfer into single struct
type ServicesMediator struct {
	GossipAdapter
	MCSAdapter
}

// GossipStateProviderImpl the implementation of the GossipStateProvider interface
// the struct to handle in memory sliding window of
// new ledger block to be acquired by hyper ledger
type GossipStateProviderImpl struct {
	// Chain id
	chainID string

	mediator *ServicesMediator

	// Channel to read gossip messages from
	gossipChan <-chan *proto.GossipMessage

	commChan <-chan proto.ReceivedMessage

	// Queue of payloads which wasn't acquired yet
	payloads PayloadsBuffer

	ledger ledgerResources

	stateResponseCh chan proto.ReceivedMessage

	stateRequestCh chan proto.ReceivedMessage

	stopCh chan struct{}

	done sync.WaitGroup

	once sync.Once

	stateTransferActive int32

	requestValidator *stateRequestValidator

	config *Configuration

	stateMetrics *metrics.StateMetrics
}

var logger = util.GetLogger(util.StateLogger, "")

// stateRequestValidator facilitates validation of the state request messages
type stateRequestValidator struct {
}

// validate checks for RemoteStateRequest message validity
func (v *stateRequestValidator) validate(request *proto.RemoteStateRequest, batchSize uint64) error {
	if request.StartSeqNum > request.EndSeqNum {
		return errors.Errorf("Invalid sequence interval [%d...%d).", request.StartSeqNum, request.EndSeqNum)
	}

	if request.EndSeqNum > batchSize+request.StartSeqNum {
		return errors.Errorf("Requesting blocks range [%d-%d) greater than configured allowed"+
			" (%d) batching size for anti-entropy.", request.StartSeqNum, request.EndSeqNum, batchSize)
	}
	return nil
}

// NewGossipStateProvider creates state provider with coordinator instance
// to orchestrate arrival of private rwsets and blocks before committing them into the ledger.
func NewGossipStateProvider(chainID string, services *ServicesMediator, ledger ledgerResources, stateMetrics *metrics.StateMetrics, config *Configuration) GossipStateProvider {

	gossipChan, _ := services.Accept(func(message interface{}) bool {
		// Get only data messages
		return message.(*proto.GossipMessage).IsDataMsg() &&
			bytes.Equal(message.(*proto.GossipMessage).Channel, []byte(chainID))
	}, false)

	remoteStateMsgFilter := func(message interface{}) bool {
		receivedMsg := message.(proto.ReceivedMessage)
		msg := receivedMsg.GetGossipMessage()
		if !(msg.IsRemoteStateMessage() || msg.GetPrivateData() != nil) {
			return false
		}
		// Ensure we deal only with messages that belong to this channel
		if !bytes.Equal(msg.Channel, []byte(chainID)) {
			return false
		}
		connInfo := receivedMsg.GetConnectionInfo()
		authErr := services.VerifyByChannel(msg.Channel, connInfo.Identity, connInfo.Auth.Signature, connInfo.Auth.SignedData)
		if authErr != nil {
			logger.Warning("Got unauthorized request from", string(connInfo.Identity))
			return false
		}
		return true
	}

	// Filter message which are only relevant for nodeMetastate transfer
	_, commChan := services.Accept(remoteStateMsgFilter, true)

	height, err := ledger.LedgerHeight()
	if height == 0 {
		// Panic here since this is an indication of invalid situation which should not happen in normal
		// code path.
		logger.Panic("Committer height cannot be zero, ledger should include at least one block (genesis).")
	}

	if err != nil {
		logger.Error("Could not read ledger info to obtain current ledger height due to: ", errors.WithStack(err))
		// Exiting as without ledger it will be impossible
		// to deliver new blocks
		return nil
	}

	s := &GossipStateProviderImpl{
		// MessageCryptoService
		mediator: services,

		// Chain ID
		chainID: chainID,

		// Channel to read new messages from
		gossipChan: gossipChan,

		// Channel to read direct messages from other peers
		commChan: commChan,

		// Create a queue for payloads, wrapped in a metrics buffer
		payloads: &metricsBuffer{
			PayloadsBuffer: NewPayloadsBuffer(height),
			sizeMetrics:    stateMetrics.PayloadBufferSize,
			chainID:        chainID,
		},

		ledger: ledger,

		stateResponseCh: make(chan proto.ReceivedMessage, config.ChannelBufferSize),

		stateRequestCh: make(chan proto.ReceivedMessage, config.ChannelBufferSize),

		stopCh: make(chan struct{}, 1),

		stateTransferActive: 0,

		once: sync.Once{},

		requestValidator: &stateRequestValidator{},

		config: config,

		stateMetrics: stateMetrics,
	}

	logger.Infof("Updating metadata information, "+
		"current ledger sequence is at = %d, next expected block is = %d", height-1, s.payloads.Next())
	logger.Debug("Updating gossip ledger height to", height)
	services.UpdateLedgerHeight(height, common2.ChainID(s.chainID))

	s.done.Add(4)

	// Listen for incoming communication
	go s.listen()
	// Deliver in order messages into the incoming channel
	go s.deliverPayloads()
	if s.config.EnableStateTransfer {
		// Execute anti entropy to fill missing gaps
		go s.antiEntropy()
	}
	// Taking care of state request messages
	go s.processStateRequests()

	return s
}

func (s *GossipStateProviderImpl) listen() {
	if infolab.DebugMode && infolab.GossipPayload {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(gossip/state/state.go) listen called from %s#%d\n", file, no)
		}
	}
	defer s.done.Done()
	var stress = 0

	go func() {
		var i = 0

		for {
			i++
			stress++
			// fmt.Println(i, stress)
			time.Sleep(time.Second)
		}
	}()
	for {
		select {
		case msg := <-s.gossipChan:
			logger.Debug("Received new message via gossip channel")
			s.queueNewMessage(msg)
			// if stress > 5 && simulating.LatestReceivedBlockID > 100 {
			// 	panic("스트레스를 너무 많이 받았다...")
			// }
			stress = 0
		case msg := <-s.commChan:
			logger.Debug("Dispatching a message", msg)
			s.dispatch(msg)
		case <-s.stopCh:
			s.stopCh <- struct{}{}
			logger.Debug("Stop listening for new messages")
			return
		}
	}
}
func (s *GossipStateProviderImpl) dispatch(msg proto.ReceivedMessage) {
	// Check type of the message
	if msg.GetGossipMessage().IsRemoteStateMessage() {
		logger.Debug("Handling direct state transfer message")
		// Got state transfer request response
		s.directMessage(msg)
	} else if msg.GetGossipMessage().GetPrivateData() != nil {
		logger.Debug("Handling private data collection message")
		// Handling private data replication message
		s.privateDataMessage(msg)
	}

}
func (s *GossipStateProviderImpl) privateDataMessage(msg proto.ReceivedMessage) {
	if !bytes.Equal(msg.GetGossipMessage().Channel, []byte(s.chainID)) {
		logger.Warning("Received state transfer request for channel",
			string(msg.GetGossipMessage().Channel), "while expecting channel", s.chainID, "skipping request...")
		return
	}

	gossipMsg := msg.GetGossipMessage()
	pvtDataMsg := gossipMsg.GetPrivateData()

	if pvtDataMsg.Payload == nil {
		logger.Warning("Malformed private data message, no payload provided")
		return
	}

	collectionName := pvtDataMsg.Payload.CollectionName
	txID := pvtDataMsg.Payload.TxId
	pvtRwSet := pvtDataMsg.Payload.PrivateRwset

	if len(pvtRwSet) == 0 {
		logger.Warning("Malformed private data message, no rwset provided, collection name = ", collectionName)
		return
	}

	txPvtRwSet := &rwset.TxPvtReadWriteSet{
		DataModel: rwset.TxReadWriteSet_KV,
		NsPvtRwset: []*rwset.NsPvtReadWriteSet{{
			Namespace: pvtDataMsg.Payload.Namespace,
			CollectionPvtRwset: []*rwset.CollectionPvtReadWriteSet{{
				CollectionName: collectionName,
				Rwset:          pvtRwSet,
			}}},
		},
	}

	txPvtRwSetWithConfig := &transientstore.TxPvtReadWriteSetWithConfigInfo{
		PvtRwset: txPvtRwSet,
		CollectionConfigs: map[string]*common.CollectionConfigPackage{
			pvtDataMsg.Payload.Namespace: pvtDataMsg.Payload.CollectionConfigs,
		},
	}

	if err := s.ledger.StorePvtData(txID, txPvtRwSetWithConfig, pvtDataMsg.Payload.PrivateSimHeight); err != nil {
		logger.Errorf("Wasn't able to persist private data for collection %s, due to %s", collectionName, err)
		msg.Ack(err) // Sending NACK to indicate failure of storing collection
	}

	msg.Ack(nil)
	logger.Debug("Private data for collection", collectionName, "has been stored")
}

func (s *GossipStateProviderImpl) directMessage(msg proto.ReceivedMessage) {
	logger.Debug("[ENTER] -> directMessage")
	defer logger.Debug("[EXIT] ->  directMessage")

	if msg == nil {
		logger.Error("Got nil message via end-to-end channel, should not happen!")
		return
	}

	if !bytes.Equal(msg.GetGossipMessage().Channel, []byte(s.chainID)) {
		logger.Warning("Received state transfer request for channel",
			string(msg.GetGossipMessage().Channel), "while expecting channel", s.chainID, "skipping request...")
		return
	}

	incoming := msg.GetGossipMessage()

	if incoming.GetStateRequest() != nil {
		if len(s.stateRequestCh) < s.config.ChannelBufferSize {
			// Forward state request to the channel, if there are too
			// many message of state request ignore to avoid flooding.
			s.stateRequestCh <- msg
		}
	} else if incoming.GetStateResponse() != nil {
		// If no state transfer procedure activate there is
		// no reason to process the message
		if atomic.LoadInt32(&s.stateTransferActive) == 1 {
			// Send signal of state response message
			s.stateResponseCh <- msg
		}
	}
}

func (s *GossipStateProviderImpl) processStateRequests() {
	defer s.done.Done()

	for {
		select {
		case msg := <-s.stateRequestCh:
			s.handleStateRequest(msg)
		case <-s.stopCh:
			s.stopCh <- struct{}{}
			return
		}
	}
}

// handleStateRequest handles state request message, validate batch size, reads current leader state to
// obtain required blocks, builds response message and send it back
func (s *GossipStateProviderImpl) handleStateRequest(msg proto.ReceivedMessage) {
	if msg == nil {
		return
	}
	request := msg.GetGossipMessage().GetStateRequest()

	if err := s.requestValidator.validate(request, s.config.AntiEntropyBatchSize); err != nil {
		logger.Errorf("State request validation failed, %s. Ignoring request...", err)
		return
	}

	currentHeight, err := s.ledger.LedgerHeight()
	if err != nil {
		logger.Errorf("Cannot access to current ledger height, due to %+v", errors.WithStack(err))
		return
	}
	if currentHeight < request.EndSeqNum {
		logger.Warningf("Received state request to transfer blocks with sequence numbers higher  [%d...%d] "+
			"than available in ledger (%d)", request.StartSeqNum, request.StartSeqNum, currentHeight)
	}

	endSeqNum := min(currentHeight, request.EndSeqNum)

	response := &proto.RemoteStateResponse{Payloads: make([]*proto.Payload, 0)}
	for seqNum := request.StartSeqNum; seqNum <= endSeqNum; seqNum++ {
		logger.Debug("Reading block ", seqNum, " with private data from the coordinator service")
		connInfo := msg.GetConnectionInfo()
		peerAuthInfo := common.SignedData{
			Data:      connInfo.Auth.SignedData,
			Signature: connInfo.Auth.Signature,
			Identity:  connInfo.Identity,
		}
		block, pvtData, err := s.ledger.GetPvtDataAndBlockByNum(seqNum, peerAuthInfo)

		if err != nil {
			logger.Errorf("cannot read block number %d from ledger, because %+v, skipping...", seqNum, err)
			continue
		}

		if block == nil {
			logger.Errorf("Wasn't able to read block with sequence number %d from ledger, skipping....", seqNum)
			continue
		}

		var pvtBytes [][]byte
		if pvtData != nil {
			// Marshal private data
			pvtBytes, err = pvtData.Marshal()
			if err != nil {
				logger.Errorf("Failed to marshal private rwset for block %d due to %+v", seqNum, errors.WithStack(err))
				continue
			}
		}

		// Appending result to the response
		response.Payloads = append(response.Payloads, &proto.Payload{
			Data:        block.Block,
			PrivateData: pvtBytes,
		})
	}
	// Sending back response with missing blocks
	msg.Respond(&proto.GossipMessage{
		// Copy nonce field from the request, so it will be possible to match response
		Nonce:   msg.GetGossipMessage().Nonce,
		Tag:     proto.GossipMessage_CHAN_OR_ORG,
		Channel: []byte(s.chainID),
		Content: &proto.GossipMessage_StateResponse{StateResponse: response},
	})
}

func (s *GossipStateProviderImpl) handleStateResponse(msg proto.ReceivedMessage) (uint64, error) {
	max := uint64(0)
	// Send signal that response for given nonce has been received
	response := msg.GetGossipMessage().GetStateResponse()
	// Extract payloads, verify and push into buffer
	if len(response.GetPayloads()) == 0 {
		return uint64(0), errors.New("Received state transfer response without payload")
	}
	for _, payload := range response.GetPayloads() {
		seqNum := payload.Data.Header.Number
		logger.Debugf("Received payload with sequence number %d.", seqNum)
		block := cached.WrapBlock(payload.Data)
		if !(config.IsEndorser || config.IsStorage) {
			if err := s.mediator.VerifyBlock(common2.ChainID(s.chainID), seqNum, block); err != nil {
				err = errors.WithStack(err)
				logger.Warningf("Error verifying block with sequence number %d, due to %+v", seqNum, err)
				return uint64(0), err
			}
		}
		if max < seqNum {
			max = seqNum
		}

		err := s.addPayload(&cached.GossipPayload{Data: block, PrivateData: payload.PrivateData}, Blocking)
		if err != nil {
			logger.Warningf("Block [%d] received from block transfer wasn't added to payload buffer: %v", seqNum, err)
		}
	}
	return max, nil
}

// Stop function sends halting signal to all go routines
func (s *GossipStateProviderImpl) Stop() {
	// Make sure stop won't be executed twice
	// and stop channel won't be used again
	s.once.Do(func() {
		s.stopCh <- struct{}{}
		// Make sure all go-routines has finished
		s.done.Wait()
		// Close all resources
		s.ledger.Close()
		close(s.stateRequestCh)
		close(s.stateResponseCh)
		close(s.stopCh)
	})
}

// queueNewMessage makes new message notification/handler
func (s *GossipStateProviderImpl) queueNewMessage(msg *proto.GossipMessage) {
	if infolab.DebugMode && infolab.GossipPayload {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(gossip/state/state.go) queueNewMessage called from %s#%d\n", file, no)
		}
	}

	if !bytes.Equal(msg.Channel, []byte(s.chainID)) {
		logger.Warning("Received enqueue for channel",
			string(msg.Channel), "while expecting channel", s.chainID, "ignoring enqueue")
		return
	}

	dataMsg := msg.GetDataMsg()
	if dataMsg != nil {
		// if config.IsEndorser || config.IsStorage {
		// 	fmt.Printf("블록 수신: %5d\n", dataMsg.Payload.Data.Header.Number)
		// }
		plRaw := dataMsg.GetPayload()
		if err := s.addPayload(&cached.GossipPayload{
			Data:        cached.WrapBlock(plRaw.Data),
			PrivateData: plRaw.PrivateData,
		}, NonBlocking); err != nil {
			logger.Warningf("Block [%d] received from gossip wasn't added to payload buffer: %v", dataMsg.Payload.Data.Header.Number, err)
			return
		}
	} else {
		logger.Debug("Gossip message received is not of data message type, usually this should not happen.")
	}
}

func (s *GossipStateProviderImpl) deliverPayloads() {
	if infolab.DebugMode && infolab.HashPut {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(gossip/state/state.go) deliverPayloads called from %s#%d\n", file, no)
		}
	}
	defer s.done.Done()

	// state machine 구현해야함
	go func() {
		simulating.DeferredMsgChan = make(chan simulating.DeferredMsg, 1000)

		for msg := range simulating.DeferredMsgChan {
			if simulating.DeferredState == simulating.ExecuteState && msg.M_VER < simulating.MinDeferredBlockNum {
				continue
			}
			if msg.M_TYPE == simulating.PeerType {
				fmt.Printf("[MSG Peer] msg.M_VER: %d, O_VAL: %d, P_VAL: %d, VerGap: %d\n", msg.M_VER, simulating.O_VAL, simulating.P_VAL, infolab.DeferredVersion)
				simulating.P_VAL = msg.M_VER
				infolab.Log.Printf("[MSG Peer] msg.M_VER: %d, O_VAL: %d, P_VAL: %d, VerGap: %d\n", msg.M_VER, simulating.O_VAL, simulating.P_VAL, infolab.DeferredVersion)
			} else {
				fmt.Printf("[MSG Orderer] msg.M_VER: %d, O_VAL: %d, P_VAL: %d, VerGap: %d\n", msg.M_VER, simulating.O_VAL, simulating.P_VAL, infolab.DeferredVersion)
				infolab.Log.Printf("[MSG Orderer] msg.M_VER: %d, O_VAL: %d, P_VAL: %d, VerGap: %d\n", msg.M_VER, simulating.O_VAL, simulating.P_VAL, infolab.DeferredVersion)
				simulating.O_VAL = msg.M_VER
			}
			if simulating.DeferredState == simulating.ExecuteState {
				if msg.M_TYPE == simulating.OrdererType && msg.M_VER > simulating.P_VAL + infolab.DeferredVersion {
					simulating.O_VAL = msg.M_VER
					simulating.DeferredMu.Lock()
					simulating.DeferredState = simulating.DeferState
					simulating.DeferredCond.Broadcast()
					simulating.DeferredMu.Unlock()
				}
			} else if simulating.DeferredState == simulating.DeferState {
				if msg.M_TYPE == simulating.PeerType && msg.M_VER + infolab.DeferredVersion >= simulating.O_VAL {
					fmt.Printf("[Unlock] msg.M_VER: %d, O_VAL: %d, P_VAL: %d, VerGap: %d\n %d + %d <= %d \n", msg.M_VER, simulating.O_VAL, simulating.P_VAL, infolab.DeferredVersion, msg.M_VER, infolab.DeferredVersion, simulating.O_VAL)
					infolab.Log.Printf("[Unlock] msg.M_VER: %d, O_VAL: %d, P_VAL: %d, VerGap: %d\n %d + %d <= %d \n", msg.M_VER, simulating.O_VAL, simulating.P_VAL, infolab.DeferredVersion, msg.M_VER, infolab.DeferredVersion, simulating.O_VAL)
					simulating.P_VAL = msg.M_VER
					simulating.DeferredMu.Lock()
					simulating.DeferredState = simulating.ExecuteState
					simulating.DeferredCond.Broadcast()
					simulating.DeferredMu.Unlock()
				}
			}
		}
	} ()


	// Orderer block 번호 받는 부분
	go func() {
		fmt.Printf("Listen from orderer\n")
		l, err := net.Listen("tcp", ":8001")
		
		if nil != err {
			log.Println(err)
		}
		defer l.Close()

		for {
			fmt.Printf("Wait for accept\n")
			conn, err := l.Accept()
			fmt.Printf("Received connection\n")
			if nil != err {
				log.Println(err)
				continue
			}
			defer conn.Close()
			recvBuf := make([]byte, 4096)
			for {
				n, err := conn.Read(recvBuf)
				if nil != err {
					log.Println(err)
					return
				}
				if 0 < n {
					// M_TYPE: Orderer
					data := recvBuf[:n]
					stringData := strings.Split(string(data), ",")
					for i := 0; i < len(stringData) - 1; i++ {
						ordererBlock, _ := strconv.Atoi(stringData[i])
						simulating.DeferredMsgChan <- simulating.DeferredMsg{simulating.OrdererType, ordererBlock}
						fmt.Printf("[Receive from orderer]: %d\n", ordererBlock)
					}
				}
			}
		}
	}()

	startTime = time.Now()
	errChan := make(chan error, 10)
	sem := semaphore.NewWeighted(int64(runtime.NumCPU()))
	ctx := context.Background()
	for {
		select {
		// Wait for notification that next seq has arrived
		case <-s.payloads.Ready():
			if infolab.DebugMode && infolab.FastPayloadRecvTime {
				receiveCount++
				receivePeriod = time.Since(startTime) - lastTime
				lastTime = time.Since(startTime)
				if receiveCount >= 301 {
					receivePeriodAverage += receivePeriod.Nanoseconds()
					fmt.Printf("Fast peer payload ready is %d\n", receivePeriod.Nanoseconds())
				}
				if receiveCount == 3300 {
					receivePeriodAverage = receivePeriodAverage / 3000
					fmt.Printf("**Fast peer payload ready receive average is %d\n", receivePeriodAverage)
				}
			}
			if infolab.DebugMode && infolab.GossipPayload {
				fmt.Printf("(gossip/state/state.go) Received payloads.ready()\n")
			}
			logger.Debugf("[%s] Ready to transfer payloads (blocks) to the ledger, next block number is = [%d]", s.chainID, s.payloads.Next())
			// Collect all subsequent payloads
			for payload := s.payloads.Pop(); payload != nil; payload = s.payloads.Pop() {
				block := payload.Data
				logger.Debugf("[%s] Transferring block [%d] with %d transaction(s) to the ledger", s.chainID, block.Header.Number, len(block.Data.Data))

				// fmt.Printf("블록 풀기 시작: %5d\n", block.Header.Number)

				// ▼ 메모리 누수 의심 구간
				// Read all private data into slice
				var p util.PvtDataCollections
				if payload.PrivateData != nil {
					// if false && payload.PrivateData != nil {
					err := p.Unmarshal(payload.PrivateData)
					if err != nil {
						logger.Errorf("Wasn't able to unmarshal private data for block seqNum = %d due to (%+v)...dropping block", block.Header.Number, errors.WithStack(err))
						continue
					}
				}
				// if block.Header.Number > 10 {
				// 	continue
				// }
				if err := sem.Acquire(ctx, 1); err != nil {
					logger.Fatal("Couldn't acquire semaphore lock")
				}
				// ▲ 메모리 누수 의심 구간
				errIn := s.commitBlock(block, p)
				go func() {
					sem.Release(1)
					err := <-errIn
					errChan <- err
				}()

				select {
				case err := <-errChan:
					if err != nil {
						if executionErr, isExecutionErr := err.(*vsccErrors.VSCCExecutionFailureError); isExecutionErr {
							logger.Errorf("Failed executing VSCC due to %v. Aborting chain processing", executionErr)
							return
						}
						logger.Panicf("Cannot commit block to the ledger due to %+v", errors.WithStack(err))
					}
				default:
				}

			}
		case <-s.stopCh:
			s.stopCh <- struct{}{}
			logger.Debug("State provider has been stopped, finishing to push new blocks.")
			return
		}
	}
}

func (s *GossipStateProviderImpl) antiEntropy() {
	defer s.done.Done()
	defer logger.Debug("State Provider stopped, stopping anti entropy procedure.")

	for {
		select {
		case <-s.stopCh:
			s.stopCh <- struct{}{}
			return
		case <-time.After(s.config.AntiEntropyInterval):
			ourHeight, err := s.ledger.LedgerHeight()
			if err != nil {
				// Unable to read from ledger continue to the next round
				logger.Errorf("Cannot obtain ledger height, due to %+v", errors.WithStack(err))
				continue
			}
			if ourHeight == 0 {
				logger.Error("Ledger reported block height of 0 but this should be impossible")
				continue
			}
			maxHeight := s.maxAvailableLedgerHeight()
			if ourHeight >= maxHeight {
				continue
			}
			if simulating.CatchingUp {
				continue
			}
			s.requestBlocksInRange(uint64(ourHeight), uint64(maxHeight)-1)
		}
	}
}

// maxAvailableLedgerHeight iterates over all available peers and checks advertised meta state to
// find maximum available ledger height across peers
func (s *GossipStateProviderImpl) maxAvailableLedgerHeight() uint64 {
	max := uint64(0)
	for _, p := range s.mediator.PeersOfChannel(common2.ChainID(s.chainID)) {
		if p.Properties == nil {
			logger.Debug("Peer", p.PreferredEndpoint(), "doesn't have properties, skipping it")
			continue
		}
		peerHeight := p.Properties.LedgerHeight
		if max < peerHeight {
			max = peerHeight
		}
	}
	return max
}

// requestBlocksInRange capable to acquire blocks with sequence
// numbers in the range [start...end).
func (s *GossipStateProviderImpl) requestBlocksInRange(start uint64, end uint64) {
	atomic.StoreInt32(&s.stateTransferActive, 1)
	defer atomic.StoreInt32(&s.stateTransferActive, 0)

	for prev := start; prev <= end; {
		next := min(end, prev+s.config.AntiEntropyBatchSize)

		gossipMsg := s.stateRequestMessage(prev, next)

		responseReceived := false
		tryCounts := 0

		for !responseReceived {
			if tryCounts > s.config.AntiEntropyMaxRetries {
				logger.Warningf("Wasn't  able to get blocks in range [%d...%d), after %d retries",
					prev, next, tryCounts)
				return
			}
			// Select peers to ask for blocks
			peer, err := s.selectPeerToRequestFrom(next)
			if err != nil {
				logger.Warningf("Cannot send state request for blocks in range [%d...%d), due to %+v",
					prev, next, errors.WithStack(err))
				return
			}

			logger.Debugf("State transfer, with peer %s, requesting blocks in range [%d...%d), "+
				"for chainID %s", peer.Endpoint, prev, next, s.chainID)

			fmt.Printf("requestBlocksInRange (%5d ~ %5d) #%d <- %s\n", prev, next, tryCounts, peer.Endpoint)
			s.mediator.Send(gossipMsg, peer)
			tryCounts++

			// Wait until timeout or response arrival
			select {
			case msg := <-s.stateResponseCh:
				// if msg.GetGossipMessage().Nonce != gossipMsg.Nonce {
				// 	fmt.Println("Nonce")
				// 	continue
				// }
				// Got corresponding response for state request, can continue
				// fmt.Printf("handleStateResponse %5d %5d {\n", prev, next)
				index, err := s.handleStateResponse(msg)
				// fmt.Printf("handleStateResponse %5d %5d }\n", prev, next)
				if err != nil {
					logger.Warningf("Wasn't able to process state response for "+
						"blocks [%d...%d], due to %+v", prev, next, errors.WithStack(err))
					continue
				}
				prev = index + 1
				responseReceived = true
			case <-time.After(s.config.AntiEntropyStateResponseTimeout):
				fmt.Println("Timeout")
			case <-s.stopCh:
				s.stopCh <- struct{}{}
				return
			}
		}
	}
}

// stateRequestMessage generates state request message for given blocks in range [beginSeq...endSeq]
func (s *GossipStateProviderImpl) stateRequestMessage(beginSeq uint64, endSeq uint64) *proto.GossipMessage {
	return &proto.GossipMessage{
		Nonce:   util.RandomUInt64(),
		Tag:     proto.GossipMessage_CHAN_OR_ORG,
		Channel: []byte(s.chainID),
		Content: &proto.GossipMessage_StateRequest{
			StateRequest: &proto.RemoteStateRequest{
				StartSeqNum: beginSeq,
				EndSeqNum:   endSeq,
			},
		},
	}
}

// selectPeerToRequestFrom selects peer which has required blocks to ask missing blocks from
func (s *GossipStateProviderImpl) selectPeerToRequestFrom(height uint64) (*comm.RemotePeer, error) {
	// Filter peers which posses required range of missing blocks
	peers := s.filterPeers(s.hasRequiredHeight(height))

	n := len(peers)
	if n == 0 {
		return nil, errors.New("there are no peers to ask for missing blocks from")
	}

	// Select peer to ask for blocks
	return peers[util.RandomInt(n)], nil
}

// filterPeers returns list of peers which aligns the predicate provided
func (s *GossipStateProviderImpl) filterPeers(predicate func(peer discovery.NetworkMember) bool) []*comm.RemotePeer {
	var peers []*comm.RemotePeer

	for _, member := range s.mediator.PeersOfChannel(common2.ChainID(s.chainID)) {
		if predicate(member) {
			peers = append(peers, &comm.RemotePeer{Endpoint: member.PreferredEndpoint(), PKIID: member.PKIid})
		}
	}

	return peers
}

// hasRequiredHeight returns predicate which is capable to filter peers with ledger height above than indicated
// by provided input parameter
func (s *GossipStateProviderImpl) hasRequiredHeight(height uint64) func(peer discovery.NetworkMember) bool {
	return func(peer discovery.NetworkMember) bool {
		// return peer.Endpoint == "fast.org1.jjo.kr:7051"

		if peer.Properties != nil {
			return peer.Properties.LedgerHeight >= height
		}
		logger.Debug(peer.PreferredEndpoint(), "doesn't have properties")
		return false
	}
}

func (s *GossipStateProviderImpl) SendToFastPeer(message *proto.GossipMessage) error {
	var fastPeer *comm.RemotePeer

	for _, member := range s.mediator.PeersOfChannel(common2.ChainID(s.chainID)) {
		if member.PreferredEndpoint() == "fast.org1.jjo.kr:7051" {
			fastPeer = &comm.RemotePeer{
				Endpoint: "fast.org1.jjo.kr:7051",
				PKIID:    member.PKIid,
			}
			break
		}
	}
	if fastPeer == nil {
		return errors.New("fast peer를 찾을 수 없음")
	}
	s.mediator.Send(message, fastPeer)
	return nil
}

// AddPayload adds new payload into state.
func (s *GossipStateProviderImpl) AddPayload(payload *cached.GossipPayload) error {
	if infolab.DebugMode && infolab.GossipPayload {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(gossip/state/state.go) AddPayload called from %s#%d\n", file, no)
		}
	}
	return s.addPayload(payload, s.config.BlockingMode)
}

// addPayload adds new payload into state. It may (or may not) block according to the
// given parameter. If it gets a block while in blocking mode - it would wait until
// the block is sent into the payloads buffer.
// Else - it may drop the block, if the payload buffer is too full.
func (s *GossipStateProviderImpl) addPayload(payload *cached.GossipPayload, blockingMode bool) error {
	if infolab.DebugMode && infolab.GossipPayload {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(gossip/state/state.go) addPayload called from %s#%d\n", file, no)
		}
	}
	if payload == nil {
		return errors.New("Given payload is nil")
	}
	blockNum := payload.Data.Header.Number
	logger.Debugf("[%s] Adding payload to local buffer, blockNum = [%d]", s.chainID, blockNum)
	height, err := s.ledger.LedgerHeight()
	if err != nil {
		return errors.Wrap(err, "Failed obtaining ledger height")
	}

	if !blockingMode && blockNum-height >= uint64(s.config.MaxBlockDistance) {
		return errors.Errorf("Ledger height is at %d, cannot enqueue block with sequence of %d", height, blockNum)
	}

	for blockingMode && s.payloads.Size() > s.config.MaxBlockDistance*2 {
		time.Sleep(enqueueRetryInterval)
	}

	s.payloads.Push(payload)
	logger.Debugf("Blocks payloads buffer size for channel [%s] is %d blocks", s.chainID, s.payloads.Size())
	return nil
}

func (s *GossipStateProviderImpl) commitBlock(block *cached.Block, pvtData util.PvtDataCollections) chan error {
	if infolab.DebugMode && (infolab.HashPut || infolab.ValidateAndPrepare) {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(gossip/state/state.go) commitBlock called from %s#%d\n", file, no)
		}
	}
	t1 := time.Now()

	// Commit block with available private transactions
	errIn := s.ledger.StoreBlock(block, pvtData)
	errOut := make(chan error, 1)
	go func() {
		err := <-errIn
		if err != nil {
			logger.Errorf("Got error while committing(%+v)", errors.WithStack(err))
			errOut <- err
			return
		}

		sinceT1 := time.Since(t1)
		s.stateMetrics.CommitDuration.With("channel", s.chainID).Observe(sinceT1.Seconds())

		// Update ledger height
		s.mediator.UpdateLedgerHeight(block.Header.Number+1, common2.ChainID(s.chainID))
		logger.Debugf("[%s] Committed block [%d] with %d transaction(s)",
			s.chainID, block.Header.Number, len(block.Data.Data))
		s.stateMetrics.Height.With("channel", s.chainID).Set(float64(block.Header.Number + 1))
		errOut <- nil

		infolab.LatestReceivedBlockID = block.Header.Number
		if block.Header.Number%infolab.FlushInterval == 0 {
			infolab.Flush()
		}
		caliperLatency := infolab.ResolvePendingBatchStartTime(block.Header.Number)
		s.stateMetrics.CaliperLatency.With("channel", s.chainID).Observe(caliperLatency.Seconds())
	}()
	return errOut
}

func min(a uint64, b uint64) uint64 {
	return b ^ ((a ^ b) & (-(uint64(a-b) >> 63)))
}
