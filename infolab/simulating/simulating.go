package simulating

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/infolab"
	gossip "github.com/hyperledger/fabric/protos/gossip"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
)
const (
	ExecuteState int = 0
	DeferState = 1
)

const (
	OrdererType = 0
	PeerType = 1
)
// Race condition을 제거하기 위해서 무조건 channel에 업데이트 값을 넣어준 후 값을 변경한다
var DeferredState int
var O_VAL int
var P_VAL int
var MinDeferredBlockNum int = 40

type DeferredMsg struct {
	M_TYPE int
	M_VER int
}

var DeferredMsgChan chan DeferredMsg

var DeferredMu = new(sync.Mutex)
var DeferredCond = sync.NewCond(DeferredMu)



type iGossipServiceInstance interface {
	// SendToFastPeer 함수는 주어진 메시지를 fast peer로 전송한다.
	SendToFastPeer(channelID string, message *gossip.GossipMessage) error
}
type keyListItem struct {
	key        string
	prev, next *keyListItem
}

const maxQueueLength = 1000000
const maxInnerQueueLength = 10000
const maxKeyListLength = 10000

var GossipServiceInstance iGossipServiceInstance

var RememberLock = sync.RWMutex{}

var LatestReceivedBlockID uint64 = 0

// SimulatedProposals 변수는 (키, []*fabric.core.endorser.simulationParameter) 쌍의 딕셔너리이다.
// endorsement peer만 이 딕셔너리를 활용한다.
var SimulatedProposals = map[string][]*ccprovider.SimulationParameter{}

// EarlyResimulationResults 변수는 (트랜잭션 식별자, *ResimulationResult) 쌍의 딕셔너리이다.
// fast peer만 이 딕셔너리를 활용한다.
var EarlyResimulationResults = map[string]*gossip.ResimulationResult{}

var keyListItemHead *keyListItem = &keyListItem{key: "ROOT"}
var keyListItemTail *keyListItem
var recentlySimulatedKeyList = map[string]*keyListItem{}

// RememberSimulation 함수는 첫 시뮬레이션 결과를 딕셔너리에 기록한다.
// 딕셔너리에 너무 많은 결과가 보관되는 경우 가장 먼저 기록된 결과부터 차례대로 지운다.
func RememberSimulation(name string, baby *ccprovider.SimulationParameter) {
	if !infolab.EarlyResimulationEnabled {
		return
	}
	var list []*ccprovider.SimulationParameter

	RememberLock.Lock()
	defer RememberLock.Unlock()

	if node, ok := SimulatedProposals[name]; ok {
		list = node
		// fmt.Println("innerQueue", len(list), maxInnerQueueLength)
		// if len(list) == maxInnerQueueLength {
		// return
		// list = append(list[1:], baby)
		// }
		item := recentlySimulatedKeyList[name]
		if item.prev.key != "ROOT" {
			if item.next == nil {
				keyListItemTail = item.prev
			} else {
				item.next.prev = item.prev
			}
			item.prev.next = item.next
			item.prev = keyListItemHead
			item.next = keyListItemHead.next
			keyListItemHead.next.prev = item
			keyListItemHead.next = item
		}
	} else {
		var item = &keyListItem{
			key:  name,
			prev: keyListItemHead,
			next: keyListItemHead.next,
		}
		// fmt.Println("outerQueue", len(recentlySimulatedKeyList), maxKeyListLength)
		// for len(recentlySimulatedKeyList) >= maxKeyListLength {
		// fmt.Printf("ㅠㅠ %s\n", keyListItemTail.key)
		// 	delete(recentlySimulatedKeyList, keyListItemTail.key)
		// 	delete(SimulatedProposals, keyListItemTail.key)
		// 	keyListItemTail = keyListItemTail.prev
		// 	keyListItemTail.next = nil
		// }
		if keyListItemTail == nil {
			keyListItemTail = item
		}
		if keyListItemHead.next != nil {
			keyListItemHead.next.prev = item
		}
		keyListItemHead.next = item
		recentlySimulatedKeyList[name] = item
		list = append(make([]*ccprovider.SimulationParameter, 0, maxInnerQueueLength), baby)
	}
	SimulatedProposals[name] = list
}

// RememberResimulation 함수는 조기 재시뮬레이션 결과를 딕셔너리에 기록한다.
// 딕셔너리에 너무 많은 결과가 보관되는 경우 가장 먼저 기록된 결과부터 차례대로 지운다.
func RememberResimulation(result *gossip.ResimulationResult) {
	if !infolab.EarlyResimulationEnabled {
		return
	}
	// if len(EarlyResimulationResults) == maxQueueLength {
	// 	return
	// }
	RememberLock.Lock()
	defer RememberLock.Unlock()

	EarlyResimulationResults[result.TransactionId] = result
}

// Resimulate 함수는 주어진 인자를 가지고 Invoke2를 재시뮬레이션한 결과를 fast peer로 보낸다.
func Resimulate(parameter *ccprovider.SimulationParameter, latest []*peer.KeyMetadata) bool {
	if !infolab.EarlyResimulationEnabled {
		return false
	}
	var payload = &peer.ChaincodeProposalPayload{}
	var input = &peer.ChaincodeInvocationSpec{}
	var args [][]byte
	var baseR = map[string]*peer.KeyMetadata{}
	var baseW = map[string]*peer.KeyMetadata{}
	var set = map[string]int64{}
	var amount int64

	if err := proto.Unmarshal(parameter.TxParams.Proposal.Payload, payload); err != nil {
		panic(err)
	}
	if err := proto.Unmarshal(payload.Input, input); err != nil {
		panic(err)
	}
	args = input.ChaincodeSpec.Input.Args

	for _, v := range parameter.KeyMetadataList {
		if v.Writing {
			set[string(v.Name)] = v.Value
		} else {
			baseR[string(v.Name)] = v
		}
	}
	for _, v := range latest {
		if v.Writing {
			set[string(v.Name)] = v.Value
		} else {
			baseR[string(v.Name)] = v
		}
	}
	switch string(args[0]) {
	case "send2":
		// args[1] -> args[2]로 args[3]원 전송
		amount, _ = strconv.ParseInt(string(args[3]), 10, 64)
		baseW[string(args[1])] = &peer.KeyMetadata{
			Name:    args[1],
			Value:   set[string(args[1])] - amount,
			Writing: true,
		}
		baseW[string(args[2])] = &peer.KeyMetadata{
			Name:    args[2],
			Value:   set[string(args[2])] + amount,
			Writing: true,
		}
	case "send4":
		// args[1] -> args[2], args[3], args[4]로 args[5]원 전송
		amount, _ = strconv.ParseInt(string(args[5]), 10, 64)
		baseW[string(args[1])] = &peer.KeyMetadata{
			Name:    args[1],
			Value:   set[string(args[1])] - 3*amount,
			Writing: true,
		}
		for i := 2; i <= 4; i++ {
			baseW[string(args[i])] = &peer.KeyMetadata{
				Name:    args[i],
				Value:   set[string(args[i])] + amount,
				Writing: true,
			}
		}
	case "send6":
		// args[1] -> args[2], args[3], args[4], args[5], args[6]으로 args[7]원 전송
		amount, _ = strconv.ParseInt(string(args[7]), 10, 64)
		baseW[string(args[1])] = &peer.KeyMetadata{
			Name:    args[1],
			Value:   set[string(args[1])] - 5*amount,
			Writing: true,
		}
		for i := 2; i <= 6; i++ {
			baseW[string(args[i])] = &peer.KeyMetadata{
				Name:    args[i],
				Value:   set[string(args[i])] + amount,
				Writing: true,
			}
		}
	default:
		return false
	}
	GossipServiceInstance.SendToFastPeer("jjoriping", &gossip.GossipMessage{
		Nonce:   0,
		Tag:     gossip.GossipMessage_CHAN_OR_ORG,
		Channel: []byte("jjoriping"),
		Content: &gossip.GossipMessage_ResimulationResult{
			ResimulationResult: getResimulationResult(parameter.TxParams.TxID, baseR, baseW),
		},
	})
	return true
}

func getResimulationResult(transactionID string, reads, writes map[string]*peer.KeyMetadata) *gossip.ResimulationResult {
	var keyMetadataList = make([]*peer.KeyMetadata, len(reads)+len(writes))
	var kvReads = make([]*kvrwset.KVRead, 0, len(reads))
	var kvWrites = make([]*kvrwset.KVWrite, 0, len(writes))

	for _, v := range reads {
		keyMetadataList = append(keyMetadataList, v)
		kvReads = append(kvReads, &kvrwset.KVRead{
			Key: string(v.Name),
			Version: &kvrwset.Version{
				BlockNum: v.Version,
				TxNum:    0,
			},
		})
	}
	for _, v := range writes {
		keyMetadataList = append(keyMetadataList, v)
		kvWrites = append(kvWrites, &kvrwset.KVWrite{
			Key:   string(v.Name),
			Value: []byte(fmt.Sprint(v.Value)),
		})
	}
	return &gossip.ResimulationResult{
		TransactionId: transactionID,
		RwSet: &rwset.TxReadWriteSet{
			NsRwset: []*rwset.NsReadWriteSet{
				{
					Namespace: "InfoCC",
					Rwset: utils.MarshalOrPanic(&kvrwset.KVRWSet{
						Reads:  kvReads,
						Writes: kvWrites,
					}),
				},
			},
		},
		KeyMetadataList: keyMetadataList,
	}
}
