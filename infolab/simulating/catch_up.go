package simulating

import (
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	// "math/rand"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/fastfabric/validator/internalVal"
	"github.com/hyperledger/fabric/infolab"

	"github.com/hyperledger/fabric/gossip/util"
	"github.com/hyperledger/fabric/protos/gossip"
)

// var Log *log.Logger

type iGossipServiceAdapter interface {
	Gossip(msg *gossip.GossipMessage)
}
type iVersionedDB interface {
	ApplyPrivacyAwareUpdates(updates *privacyenabledstate.UpdateBatch, height *version.Height) error
}

// const CatchUpDuration = 10 * time.Hour

// CatchingUp 변수는 현재 버전 차이 극복 여부를 나타낸다.
var CatchingUp bool

// CatchingUpCount 변수는 버전 차이 극복을 위해 fast peer가 특수한 방법으로 블록을 전달한 횟수를 나타낸다.
var CatchingUpCount uint

var RecentlyUpdatedVersionedDB iVersionedDB

var catchUpMarking = map[uint64]*gossip.GossipMessage{}
var fastWb = map[uint64]*gossip.GossipMessage{}
// var fastWb = map[uint64]*gossip.GossipMessage{}
var FastWbChan chan GossipMessageWithBlockNum
var catchUpLock = sync.Mutex{}

var startTime time.Time
var lastTime time.Duration
var sendPeriod time.Duration
var sendCount int64
var periodCount int64
var sendPeriodAverage int64

var receiveStartTime time.Duration
var startMeasureReceive bool
var endMeasureReceive bool
var startMeasureReceiveBlockNum int64

type GossipMessageWithBlockNum struct {
	GossipMsg *gossip.GossipMessage
	BlockNum uint64
}


func init() {
	sendCount = 0
	startTime = time.Now()
	lastTime = time.Since(startTime)
	infolab.BootstrapCatchUp = func(by uint64) {
		if !CatchingUp && infolab.CatchUpGossipingEnabled {
			if infolab.DebugMode {
				_, file, no, ok := runtime.Caller(1)
				if ok {
					fmt.Printf("(infolab/simulating/catch_up.go) Start catchup from %s#%d\n", file, no)
				}
			}
			CatchingUp = true
			// go func() {
			// 	time.Sleep(CatchUpDuration)
			// 	CatchingUp = false
			// }()
			GossipServiceInstance.SendToFastPeer("jjoriping", &gossip.GossipMessage{
				Nonce:   0,
				Tag:     gossip.GossipMessage_CHAN_OR_ORG,
				Channel: []byte("jjoriping"),
				Content: &gossip.GossipMessage_CatchUpSignal{
					CatchUpSignal: &gossip.CatchUpSignal{
						Type:    gossip.CatchUpSignal_START,
						BlockId: by,
					},
				},
			})
		}
		fmt.Printf("[%5d] 너무 늦었어! (%d)\n", by, infolab.LatestReceivedBlockID)
	}
}

// StartCatchUp 함수는 버전 차이를 극복하기 위해 fast peer의 트랜잭션 전달 방식을 바꾼다.
// 바뀐 전달 방식은 1분이 지나면 되돌아온다.
// 이 함수는 fast peer에서만 호출되어야 한다.
func StartCatchUp() {
	if !infolab.CatchUpGossipingEnabled {
		return
	}
	if CatchingUp {
		return
	}
	fmt.Printf("캐치업 시작\n")
	CatchingUp = true
	CatchingUpCount = 0
	// time.Sleep(CatchUpDuration)
	// fmt.Printf("캐치업 끝: %d\n", CatchingUpCount)
	// CatchingUp = false
}

func GossipFastWb(adapter iGossipServiceAdapter, blockNum uint64, msg *gossip.GossipMessage) {
	fmt.Printf("GossipFastWb: %d\n", blockNum)
	if CatchingUp {
		catchUpLock.Lock()
		defer catchUpLock.Unlock()

		if catchUpMessage, ok := fastWb[blockNum]; ok {
			adapter.Gossip(catchUpMessage)
			delete(fastWb, blockNum)
		}

	}
}

// GossipOnCatchUp 함수는 fast peer가 블록을 통째로 담은 메시지를 보낼 상황이 될 때는 그대로,
// 버전 차이 극복이 필요할 때는 따로 처리한다.
// 이 함수는 fast peer에서만 호출되어야 한다.
func GossipOnCatchUp(adapter iGossipServiceAdapter, blockNum uint64, msg *gossip.GossipMessage) {
	// 실험
	// adapter.Gossip(msg)
	// return

	// fmt.Printf("블록 송신: %d\n", blockNum)
	if CatchingUp {
		catchUpLock.Lock()
		defer catchUpLock.Unlock()

		if infolab.DebugMode && infolab.MsgSendTime {
			sendCount++
			sendPeriod = time.Since(startTime) - lastTime
			lastTime = time.Since(startTime)
			if sendCount >= 301 {
				sendPeriodAverage += sendPeriod.Nanoseconds()
				// fmt.Printf("Block send period is %d\n", sendPeriod.Nanoseconds())
			}
			if sendCount == 1000 {
				sendPeriodAverage = sendPeriodAverage / 3000
				fmt.Printf("Catchup send average is %d\n", sendPeriodAverage)
			}
		}

		if catchUpMessage, ok := catchUpMarking[blockNum]; ok {
			// fmt.Printf("GossipOnCatchUp: %d\n", blockNum)

			adapter.Gossip(catchUpMessage)
			// go func(){
			// 	time.Sleep(6400 * time.Millisecond)
			// 	adapter.Gossip(catchUpMessage)
			// }()
			delete(catchUpMarking, blockNum)
			if infolab.DebugMode && infolab.MsgSendTime {
				fmt.Printf("[Block %d]Send catchup message\n", blockNum)
			}
		} else {
			if infolab.DebugMode && infolab.MsgSendTime {
				fmt.Printf("[Block %d]Send ordinary message\n", blockNum)
			}
			adapter.Gossip(msg)
		}
	} else {
		if infolab.DebugMode && infolab.MsgSendTime {
			sendCount++
			sendPeriod = time.Since(startTime) - lastTime
			lastTime = time.Since(startTime)
			if sendCount >= 301 {
				sendPeriodAverage += sendPeriod.Nanoseconds()
				// fmt.Printf("Block send period is %d\n", sendPeriod.Nanoseconds())
			}
			if sendCount == 1000 {
				sendPeriodAverage = sendPeriodAverage / 3000
				fmt.Printf("Block send average is %d\n", sendPeriodAverage)
			}
		}
		adapter.Gossip(msg)
	}
}

// SendCatchUpBody 함수는 버전 차이 극복 환경에서 fast peer가 계산한 update batch를
// 테이블에 저장해 GossipOnCatchUp에서 endorsement peer들에게 전송되게 한다.
// 이 함수는 fast peer에서만 호출되어야 한다.
func SendCatchUpBody(blockNum uint64, updates *internalVal.PubAndHashUpdates) {
	// 실험
	// return

	if !CatchingUp {
		return
	}
	catchUpLock.Lock()
	defer catchUpLock.Unlock()
	t1 := time.Now()
	var payload = marshalUpdateBatch(updates)
	// var payload = []byte("test;1;2;3\nplease;4;5;6")

	catchUpMarking[blockNum] = &gossip.GossipMessage{
		Nonce:   util.RandomUInt64(),
		Tag:     gossip.GossipMessage_CHAN_OR_ORG,
		Channel: []byte("jjoriping"),
		Content: &gossip.GossipMessage_CatchUpSignal{
			CatchUpSignal: &gossip.CatchUpSignal{
				Type:    gossip.CatchUpSignal_BODY,
				BlockId: blockNum,
				Payload: payload,
			},
		},
	}
	t2 := time.Since(t1).Nanoseconds()
	if infolab.DebugMode {
		fmt.Printf("SendCatchUpBody took %d\n", t2)
	}
	// fmt.Printf("SendCatchUpBody: %d (%d)\n", blockNum, len(payload))
}

func SendFastWb(blockNum uint64, updates *internalVal.PubAndHashUpdates) {
	fmt.Printf("SendFastWb called for %d\n", blockNum)
	if FastWbChan == nil {
		FastWbChan = make(chan GossipMessageWithBlockNum, 10)
	}
	// 실험
	// return

	if !CatchingUp {
		return
	}
	catchUpLock.Lock()
	defer catchUpLock.Unlock()
	t1 := time.Now()
	var payload = marshalUpdateBatch(updates)
	// var payload = []byte("test;1;2;3\nplease;4;5;6")

	fastWb[blockNum] = &gossip.GossipMessage{
		Nonce:   util.RandomUInt64(),
		Tag:     gossip.GossipMessage_CHAN_OR_ORG,
		Channel: []byte("jjoriping"),
		Content: &gossip.GossipMessage_CatchUpSignal{
			CatchUpSignal: &gossip.CatchUpSignal{
				Type:    gossip.CatchUpSignal_BODY,
				BlockId: math.MaxUint64 - 10000000 + blockNum,
				Payload: payload,
			},
		},
	}
	FastWbChan <- GossipMessageWithBlockNum{fastWb[blockNum], blockNum}
	fmt.Printf("FastWb <- for block %d\n", blockNum)

	t2 := time.Since(t1).Nanoseconds()
	if infolab.DebugMode {
		fmt.Printf("SendFastWb took %d\n", t2)
	}
}

// ReceiveCatchUpBody 함수는 버전 차이 극복 환경에서 fast peer가 보낸 update batch를
// 상태 DB에 설정하게 한다.
// 이 함수는 endorsement peer에서만 호출되어야 한다.
var receiveCount int64
var receiveDurationTotal int64
var MostRecentCatchupBlockId uint64
func ReceiveCatchUpBody(blockId uint64, payload []byte) {
	if infolab.DebugMode {
		// fmt.Printf("ReceiveCatchupBody Start\n")
	}
	if RecentlyUpdatedVersionedDB == nil {
		fmt.Printf("최근 DB 없음: %d\n", blockId)
		return
	}
	if !infolab.CatchUpGossipingEnabled {
		return
	}
	receiveStart := time.Now()
	var updates, maxTxNum = unmarshalUpdateBatch(payload)
	var err = RecentlyUpdatedVersionedDB.ApplyPrivacyAwareUpdates(
		updates,
		version.NewHeight(blockId, maxTxNum),
	)
	fmt.Printf("캐치업 블록 수신: %d\n", blockId)
	if infolab.DebugMode && infolab.MsgRecvTime {
		if blockId >= 301 && !startMeasureReceive {
			startMeasureReceive = true
			receiveStartTime = time.Since(startTime)
			startMeasureReceiveBlockNum = int64(blockId)
		} else if blockId >= 301 {
			fmt.Printf("Catchup receive[%d]\n", blockId)
		}
		if blockId >= 1000 && !endMeasureReceive {
			endMeasureReceive = true
			receivePeriodAverage := (time.Since(startTime) - receiveStartTime).Nanoseconds() / (int64(blockId) - int64(startMeasureReceiveBlockNum) + 1)
			fmt.Printf("** Catchup receive average is %d\n", receivePeriodAverage)
		}
	}
	infolab.UpdateEndorserBlockchainHeight(blockId)

	if err != nil {
		panic(err)
	}
	if infolab.DebugMode {
		// fmt.Printf("ReceiveCatchupBody End\n")
	}
	receiveEndTime := time.Since(receiveStart).Nanoseconds()

	if blockId >= 301 {
		receiveDurationTotal += receiveEndTime
		receiveCount++
		fmt.Printf("Catchup avg: %d\n", receiveDurationTotal / receiveCount)
	}
	infolab.Log.Printf("캐치업 블록 완료: %d\n", blockId)
	fmt.Printf("캐치업 블록 완료: %d\n", blockId)
	MostRecentCatchupBlockId = blockId

	

}

func ReceiveFastWb(blockId uint64, payload []byte) {
	if infolab.DebugMode {
		// fmt.Printf("ReceiveCatchupBody Start\n")
	}
	if RecentlyUpdatedVersionedDB == nil {
		fmt.Printf("최근 DB 없음: %d\n", blockId)
		return
	}
	if !infolab.CatchUpGossipingEnabled {
		return
	}
	var updates, maxTxNum = unmarshalUpdateBatch(payload)
	var err = RecentlyUpdatedVersionedDB.ApplyPrivacyAwareUpdates(
		updates,
		version.NewHeight(blockId, maxTxNum),
	)
	fmt.Printf("FastWb 블록 수신: %d\n", blockId)
	DeferredMsgChan <- DeferredMsg{PeerType, int(blockId)}
	if infolab.DebugMode && infolab.MsgRecvTime {
		if blockId >= 301 && !startMeasureReceive {
			startMeasureReceive = true
			receiveStartTime = time.Since(startTime)
			startMeasureReceiveBlockNum = int64(blockId)
		} else if blockId >= 301 {
			fmt.Printf("Catchup receive[%d]\n", blockId)
		}
		if blockId >= 1000 && !endMeasureReceive {
			endMeasureReceive = true
			receivePeriodAverage := (time.Since(startTime) - receiveStartTime).Nanoseconds() / (int64(blockId) - int64(startMeasureReceiveBlockNum) + 1)
			fmt.Printf("** Catchup receive average is %d\n", receivePeriodAverage)
		}
	}
	infolab.UpdateEndorserBlockchainHeight(blockId)

	if err != nil {
		panic(err)
	}
	if infolab.DebugMode {
		// fmt.Printf("ReceiveCatchupBody End\n")
	}
}

func marshalUpdateBatch(updates *internalVal.PubAndHashUpdates) []byte {
	var table = updates.PubUpdates.GetUpdates("InfoCC")
	var R = []string{}

	for k, v := range table {
		var value, err = strconv.Atoi(string(v.Value))

		if err != nil {
			value = 0
		}
		R = append(R, fmt.Sprintf("%s;%d;%d;%d", k, v.Version.BlockNum, v.Version.TxNum, value))
	}
	return []byte(strings.Join(R, "\n"))
}
func unmarshalUpdateBatch(chunk []byte) (R *privacyenabledstate.UpdateBatch, maxTxNum uint64) {
	R = privacyenabledstate.NewUpdateBatch()

	for _, v := range strings.Split(string(chunk), "\n") {
		if v == "" {
			continue
		}
		var data = strings.Split(v, ";")

		blockNum, _ := strconv.ParseUint(data[1], 10, 64)
		txNum, _ := strconv.ParseUint(data[2], 10, 64)

		if maxTxNum < txNum {
			maxTxNum = txNum
		}
		R.PubUpdates.Put("InfoCC", data[0], []byte(data[3]), version.NewHeight(blockNum, txNum))
	}
	return R, maxTxNum
}
