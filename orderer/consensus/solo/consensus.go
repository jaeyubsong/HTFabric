/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package solo

import (
	"fmt"
	"net"
	"runtime"
	"time"

	"go/build"
	"log"
	"os"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/infolab"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/migration"
	cb "github.com/hyperledger/fabric/protos/common"
)

var logger = flogging.MustGetLogger("orderer.consensus.solo")

type consenter struct{}

type chain struct {
	support         consensus.ConsenterSupport
	sendChan        chan *message
	exitChan        chan struct{}
	migrationStatus migration.Status
}

type message struct {
	configSeq uint64
	normalMsg *cb.Envelope
	configMsg *cb.Envelope
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Order/Configure, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New() consensus.Consenter {
	return &consenter{}
}

func (solo *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	return newChain(support), nil
}

func newChain(support consensus.ConsenterSupport) *chain {
	return &chain{
		support:         support,
		sendChan:        make(chan *message),
		exitChan:        make(chan struct{}),
		migrationStatus: migration.NewStatusStepper(support.IsSystemChannel(), support.ChainID()),
	}
}

func (ch *chain) Start() {
	go ch.main()
}

func (ch *chain) Halt() {
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

func (ch *chain) WaitReady() error {
	return nil
}

// Order accepts normal messages for ordering
func (ch *chain) Order(env *cb.Envelope, configSeq uint64) error {
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		normalMsg: env,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

// Configure accepts configuration update messages for ordering
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {
	select {
	case ch.sendChan <- &message{
		configSeq: configSeq,
		configMsg: config,
	}:
		return nil
	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	}
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan
}

func (ch *chain) MigrationStatus() migration.Status {
	return ch.migrationStatus
}

type orderingPreBlock struct {
	batch            []*cb.Envelope
	localReadDegreeTable map[string]uint32
	localWriteDegreeTable map[string]uint32
}

type nextBlockPreBlock struct {
	batch         []*cb.Envelope
	partitionInfo []byte
}

var CreateNextBlockCount int64
var totalCreateNextBlockTime int64

var batch_startTime time.Time
var batch_lastTime time.Duration
var receivePeriod time.Duration
var receiveCount int64
var receivePeriodAverage int64
var processeAverage int64

var oneMsgDurationAverage int64
var oneMsgDurationCount int64
var thousandthMsgOrderedDurationAverage int64
var thousandthMsgDurationAverage int64
var thousandthMsgDurationCount int64

var thousandMsgRecvDurationStart time.Time
var thousandMsgRecvDurationTotal int64

var fastPeerResponseDurationStart time.Time
var fastPeerResponseDurationTotal int64

var createBatchPeriodAverage int64
var createBatchCount int64
var printOnce bool

var lastIsBatch bool

var FirstCall bool

var timerExpireCount int64

var stepOneTimeTotal int64
var stepOneCount int64

var stepTwoTimeTotal int64
var stepTwoCount int64

var stepThreeTimeTotal int64
var stepThreeCount int64

var stepFourTimeTotal int64
var stepFourCount int64

var txsInBlockTotal int64
var txsInBlockCount int64

var conn1 net.Conn
var conn2 net.Conn
var conn3 net.Conn
var conn4 net.Conn

var lastBlockNum uint64
var curBlockNum uint64


func sendToMars2 (blockNum uint64) {
	msg := fmt.Sprintf("%d,", blockNum + 1)
	var err error
	if conn1 == nil {
		fmt.Printf("Empty conn1, attempt to connect\n")
		conn1, err = net.Dial("tcp", "mars02.org1.jjo.kr:8001")
		if nil != err {
			fmt.Println(err)
		}
	}
	conn1.Write([]byte(msg))
}

func sendToMars3 (blockNum uint64) {
	msg := fmt.Sprintf("%d,", blockNum + 1)
	var err error
	if conn2 == nil {
		fmt.Printf("Empty conn2, attempt to connect\n")
		conn2, err = net.Dial("tcp", "mars03.org1.jjo.kr:8001")
		if nil != err {
			fmt.Println(err)
		}
	}
	conn2.Write([]byte(msg))
}

func sendToMars4 (blockNum uint64) {
	msg := fmt.Sprintf("%d,", blockNum + 1)
	var err error
	if conn3 == nil {
		fmt.Printf("Empty conn3, attempt to connect\n")
		conn3, err = net.Dial("tcp", "mars04.org1.jjo.kr:8001")
		if nil != err {
			fmt.Println(err)
		}
	}
	conn3.Write([]byte(msg))
}

func sendToMars5 (blockNum uint64) {
	msg := fmt.Sprintf("%d,", blockNum + 1)
	var err error
	if conn4 == nil {
		fmt.Printf("Empty conn4, attempt to connect\n")
		conn4, err = net.Dial("tcp", "mars05.org1.jjo.kr:8001")
		if nil != err {
			fmt.Println(err)
		}
	}
	conn4.Write([]byte(msg))
}


func (ch *chain) main() {
	runtime.GOMAXPROCS(infolab.OrdererGoMaxProcs)



	var timer <-chan time.Time
	var err error



	// go func() {
	// 	l, err := net.Listen("tcp", ":8000")
	// 	if nil != err {
	// 		log.Println(err)
	// 	}
	// 	defer l.Close()

	// 	for {
	// 		conn, err := l.Accept()
	// 		if nil != err {
	// 			log.Println(err)
	// 			continue
	// 		}
	// 		defer conn.Close()
	// 		recvBuf := make([]byte, 4096)
	// 		for {
	// 			n, err := conn.Read(recvBuf)
	// 			if nil != err {
	// 				log.Println(err)
	// 				return
	// 			}
	// 			if 0 < n {
	// 				data := recvBuf[:n]
	// 				log.Printf("[Receive from orderer]: %s", string(data))
	// 			}
	// 		}
	// 	}
	// }()




	batch_startTime = time.Now()
	batch_lastTime = time.Since(batch_startTime)
	if infolab.ParallelOrderer {
		blockCreaterChan_idx := 0
		bufferSize := 10
		blockCreaterChan := make(chan []*cb.Envelope, bufferSize)
		orderingChan := make(chan orderingPreBlock, bufferSize)
		tspChan := make(chan orderingPreBlock, bufferSize)
		nextBlockChan := make(chan nextBlockPreBlock, bufferSize)

		// Step 1: Update Degree
		go func() {
			idx := 0
			for curPreBlock := range blockCreaterChan {
				fmt.Printf("Step 1: Update Degree\n")
				cur_start_time := time.Now()
				localReadDegreeTable, localWriteDegreeTable := infolab.UpdateDegreeParallel(curPreBlock)
				if receiveCount > 50 {
					stepOneTimeTotal += time.Since(cur_start_time).Nanoseconds()
					stepOneCount++
				}

				orderingChan <- orderingPreBlock{curPreBlock, localReadDegreeTable, localWriteDegreeTable}
				if receiveCount > 50 {
					txsInBlockTotal += int64(len(curPreBlock))
					txsInBlockCount++
				}

				idx++
			}
		}()

		// Step 2: ordering (plain, ward, etc) and TSP
		go func() {
			idx := 0
			for curOrderingBlock := range orderingChan {
				fmt.Printf("Step 2: Ordering\n")
				// ordering
				cur_start_time := time.Now()
				orderedBatch := infolab.OrderBatchParallel(curOrderingBlock.batch, curOrderingBlock.localReadDegreeTable, curOrderingBlock.localWriteDegreeTable)
				if receiveCount > 50 {
					stepTwoTimeTotal += time.Since(cur_start_time).Nanoseconds()
					stepTwoCount++
				}

				

				tspChan <- orderingPreBlock{orderedBatch, curOrderingBlock.localReadDegreeTable, curOrderingBlock.localWriteDegreeTable}
				idx++
			}
		}()

		// Step 3: TSP
		go func() {
			idx := 0
			for curTspBlock := range tspChan {
				fmt.Printf("Step 3: TSP\n")
				cur_start_time := time.Now()
				var partitionInfo []byte
				if !infolab.PartialFunctionOrdering || infolab.Partitions > 1 {
					partitionInfo = infolab.PartitionBatchParallel(curTspBlock.batch, curTspBlock.localReadDegreeTable, curTspBlock.localWriteDegreeTable)
				}

				if receiveCount > 50 {
					stepThreeTimeTotal += time.Since(cur_start_time).Nanoseconds()
					stepThreeCount++
				}
				fmt.Printf("Step 3: TSP finish\n")
				nextBlockChan <- nextBlockPreBlock{curTspBlock.batch, partitionInfo}
				idx++
			}
		}()

		// Step 4: CreateNextBlock
		go func() {
			idx := 0
			for curPreBlock := range nextBlockChan {
				cur_start_time := time.Now()
				block := ch.support.CreateNextBlockParallel(curPreBlock.batch, curPreBlock.partitionInfo, nil, uint64(0))
				lastBlockNum = block.Header.GetNumber()
				ch.support.WriteBlockParallel(block, nil, nil)
				if receiveCount > 50 {
					stepFourTimeTotal += time.Since(cur_start_time).Nanoseconds()
					stepFourCount++
				}
				timer = nil
				idx++
				fmt.Printf("[%d] Created Next block\n", block.Header.Number)
			}
		}()

		for {
			seq := ch.support.Sequence()
			err = nil
			select {
			case msg := <-ch.sendChan:
				if msg.configMsg == nil {
					// fmt.Printf("[JS] NormalMsg in\n")
					if !FirstCall {
						FirstCall = true
						var logpath = build.Default.GOPATH + "/../orderer.log"
						if _, err := os.Stat(logpath); err == nil {
							e := os.Remove(logpath)
							if e != nil {
								panic(e)
							}
							fmt.Printf("log file found, removed %v\n", logpath)
						} else {
							fmt.Printf("log file not found\n")
						}
						var file, err1 = os.Create(logpath)

						if err1 != nil {
							panic(err1)
						}
						infolab.Log = log.New(file, "", 0)
						infolab.Log.Printf("PowerFabric orderer\n")
					}

					if lastIsBatch {
						lastIsBatch = false
						fastPeerResponseDurationStart = time.Now()

						fastPeerResponseDurationTotal += time.Since(fastPeerResponseDurationStart).Nanoseconds()
						thousandMsgRecvDurationStart = time.Now()
					}

					oneMsgStartTime := time.Now()
					// NormalMsg
					if msg.configSeq < seq {
						_, err = ch.support.ProcessNormalMsg(msg.normalMsg)
						if err != nil {
							logger.Warningf("Discarding bad normal message: %s", err)
							continue
						}
					}
					cutter := ch.support.BlockCutter()
					if cutter.GetPreviousPendingBatchStartTime() == nil {
						now := time.Now()
						cutter.SetPreviousPendingBatchStartTime(&now)
					}
					batches, pending := cutter.OrderedParallel(msg.normalMsg)

					if infolab.OneMsgTime {
						if len(batches) == 0 {
							if receiveCount >= 0 {
								oneMsgEndTimeTime := time.Since(oneMsgStartTime)
								oneMsgDurationAverage += oneMsgEndTimeTime.Nanoseconds()
								oneMsgDurationCount++
							}
						} else {
							receiveCount++
							if receiveCount > 20 {
								thousandthMsgOrderedDurationAverage += time.Since(oneMsgStartTime).Nanoseconds()
								thousandMsgRecvDurationTotal += time.Since(thousandMsgRecvDurationStart).Nanoseconds()
							}
							// if receiveCount >= 301 && receiveCount <= 3300 {
							// 	thousandthMsgOrderedDurationAverage += time.Since(oneMsgStartTime).Nanoseconds()
							// }
							if receiveCount > 770 {
								// fmt.Printf("** Thousandth msg ordered duration average: %d\n", thousandthMsgOrderedDurationAverage/3000)
								// infolab.Log.Printf("ThousandMsgAvg:%d\n", thousandthMsgOrderedDurationAverage / receiveCount)
								// infolab.Log.Printf("ThousandMsgAvg:%d\n", thousandthMsgOrderedDurationAverage / receiveCount)
								infolab.Log.Printf("[parallel] ThousandMsgAvg:%d\n", thousandMsgRecvDurationTotal/(receiveCount-20))
								infolab.Log.Printf("FastPeerDurAvg:%d\n", fastPeerResponseDurationTotal/(receiveCount-20))

							}
						}

					}

					// 여기서 1000개의 메세지가 추가되어야 batch 하나가 생김
					for _, batch := range batches {

						// Send message to endorsers
						fmt.Printf("Sending hi to host\n")
						if curBlockNum == 0 {
							curBlockNum = lastBlockNum + 1
						} else {
							curBlockNum++
						}
						if infolab.EndorNum >= 1 {
							sendToMars2(curBlockNum)
						}
						if infolab.EndorNum >= 2 {
							sendToMars3(curBlockNum)
						}
						if infolab.EndorNum >= 3 {
							sendToMars4(curBlockNum)
						}
						if infolab.EndorNum >= 4 {
							sendToMars5(curBlockNum)
						}
						
						lastIsBatch = true
						// 여기서 메세지 멈출지 판단


						// one_batch_startTime := time.Now()
						if infolab.DebugMode && infolab.Orderer_batchTime {
							receivePeriod = time.Since(batch_startTime) - batch_lastTime
							batch_lastTime = time.Since(batch_startTime)
							fmt.Printf("Orderer batch[%d] period is %d\n", receiveCount, receivePeriod.Nanoseconds())
							if receiveCount >= 301 {
								receivePeriodAverage += receivePeriod.Nanoseconds()
								// fmt.Printf("Block receive period is %d and count is %d\n", receivePeriod.Nanoseconds(), receiveCount)
							}
							if receiveCount == 3300 {
								receivePeriodAverage = receivePeriodAverage / 3000
								fmt.Printf("** Orderer batch receive period average is %d\n", receivePeriodAverage)
							}
						}

						blockCreaterChan_idx++
						// localDegreeTable := infolab.UpdateDegree(batch)

						// Step 1: create batch & update degree
						blockCreaterChan <- batch

						if receiveCount > 50 {
							if stepOneCount > 0 {
								infolab.Log.Printf("UpdateDegreeAvg:%d", stepOneTimeTotal/stepOneCount)
							}
							if stepTwoCount > 0 {
								infolab.Log.Printf("Ordering:%d", stepTwoTimeTotal/stepTwoCount)
							}
							if stepThreeCount > 0 {
								infolab.Log.Printf("TSP:%d", stepThreeTimeTotal/stepThreeCount)
							}
							if stepFourCount > 0 {
								infolab.Log.Printf("CreateNextBlock:%d", stepFourTimeTotal/stepFourCount)
							}
							if txsInBlockCount > 0 {
								infolab.Log.Printf("TxsInBlock:%d", txsInBlockTotal/txsInBlockCount)
							}
							infolab.Log.Printf("BlockNum:%d", receiveCount)
						}
					}

					switch {
					case timer != nil && !pending:
						// Timer is already running but there are no messages pending, stop the timer
						timer = nil
					case timer == nil && pending && blockCreaterChan_idx < 10:
						// Timer is not already running and there are messages pending, so start it
						// 이 타이머는 parallel orderer에 의해 계속 pending이 되기 때문에 일부러 없앴다.
						timer = time.After(ch.support.SharedConfig().BatchTimeout())
						logger.Debugf("Just began %s batch timer", ch.support.SharedConfig().BatchTimeout().String())
						fmt.Printf("Just began %s batch timer\n", ch.support.SharedConfig().BatchTimeout().String())
					default:
						// Do nothing when:
						// 1. Timer is already running and there are messages pending
						// 2. Timer is not set and there are no messages pending
					}

				} else {
					// ConfigMsg
					// fmt.Printf("[JS] ConfigMsg in\n")
					if msg.configSeq < seq {
						msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
						if err != nil {
							logger.Warningf("Discarding bad config message: %s", err)
							continue
						}
					}
					batch := ch.support.BlockCutter().CutParallel()
					if batch != nil {
						localReadDegreeTable, localWriteDegreeTable := infolab.UpdateDegreeParallel(batch)
						batch = infolab.OrderBatchParallel(batch, localReadDegreeTable, localWriteDegreeTable)
						var partitionInfo []byte
						if !infolab.PartialFunctionOrdering || infolab.Partitions > 1 {
							partitionInfo = infolab.PartitionBatchParallel(batch, localReadDegreeTable, localWriteDegreeTable)
						}
						block := ch.support.CreateNextBlockParallel(batch, partitionInfo, nil, uint64(0))
						lastBlockNum = block.Header.GetNumber()
						ch.support.WriteBlockParallel(block, nil, nil)
					}

					block := ch.support.CreateNextBlockParallel([]*cb.Envelope{msg.configMsg}, []byte{}, nil, uint64(0))
					lastBlockNum = block.Header.GetNumber()
					ch.support.WriteConfigBlock(block, nil)
					timer = nil
				}
			case <-timer:
				//clear the timer
				timer = nil
				if !FirstCall {
					FirstCall = true
					var logpath = build.Default.GOPATH + "/../orderer.log"
					if _, err := os.Stat(logpath); err == nil {
						e := os.Remove(logpath)
						if e != nil {
							panic(e)
						}
						fmt.Printf("log file found, removed %v\n", logpath)
					} else {
						fmt.Printf("log file not found\n")
					}
					var file, err1 = os.Create(logpath)

					if err1 != nil {
						panic(err1)
					}
					infolab.Log = log.New(file, "", 0)
					infolab.Log.Printf("PowerFabric orderer")
				}
				infolab.Log.Printf("Timeout in receiveCount: %d", receiveCount)


				timerExpireCount++
				fmt.Printf("Timer called: %d\n", timerExpireCount)
				// Send message to endorsers
				// fmt.Printf("Sending hi to host\n")
				// if conn1 == nil {
				// 	fmt.Printf("Empty conn1, attempt to connect")
				// 	conn1, err = net.Dial("tcp", "143.248.140.147:8001")
				// 	if nil != err {
				// 		fmt.Println(err)
				// 	}
				// }
				// conn1.Write([]byte("Hi"))
				if timerExpireCount > 6 { // After testing
					if lastIsBatch {
						fastPeerResponseDurationStart = time.Now()

						fastPeerResponseDurationTotal += time.Since(fastPeerResponseDurationStart).Nanoseconds()
						thousandMsgRecvDurationStart = time.Now()
					}
					lastIsBatch = true
				}

				cutter := ch.support.BlockCutter()
				batch := cutter.CutParallel()
				if len(batch) == 0 {
					logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
					continue
				}
				logger.Debugf("Batch timer expired, creating block")
				localReadDegreeTable, localWriteDegreeTable := infolab.UpdateDegreeParallel(batch)
				batch = infolab.OrderBatchParallel(batch, localReadDegreeTable, localWriteDegreeTable)
				var partitionInfo []byte
				if !infolab.PartialFunctionOrdering || infolab.Partitions > 1 {
					partitionInfo = infolab.PartitionBatchParallel(batch, localReadDegreeTable, localWriteDegreeTable)
				}
				block := ch.support.CreateNextBlockParallel(batch, partitionInfo, nil, uint64(0))
				lastBlockNum = block.Header.GetNumber()
				ch.support.WriteBlockParallel(block, nil, nil)
				t := cutter.GetPreviousPendingBatchStartTime()
				if t != nil {
					infolab.AddPendingBatchStartTime(block.Header.Number, t)
					cutter.SetPreviousPendingBatchStartTime(nil)
				}
			case <-ch.exitChan:
				logger.Debugf("Exiting")
				return
			}
		}
	} else {
		for {
			seq := ch.support.Sequence()
			err = nil
			select {
			case msg := <-ch.sendChan:
				if msg.configMsg == nil {
					if !FirstCall {
						FirstCall = true
						var logpath = build.Default.GOPATH + "/../orderer.log"
						if _, err := os.Stat(logpath); err == nil {
							e := os.Remove(logpath)
							if e != nil {
								panic(e)
							}
							fmt.Printf("log file found, removed %v\n", logpath)
						} else {
							fmt.Printf("log file not found\n")
						}
						var file, err1 = os.Create(logpath)

						if err1 != nil {
							panic(err1)
						}
						infolab.Log = log.New(file, "", 0)
					}

					if lastIsBatch {
						lastIsBatch = false
						fastPeerResponseDurationStart = time.Now()

						fastPeerResponseDurationTotal += time.Since(fastPeerResponseDurationStart).Nanoseconds()
						thousandMsgRecvDurationStart = time.Now()
					}
					oneMsgStartTime := time.Now()
					// NormalMsg
					if msg.configSeq < seq {
						_, err = ch.support.ProcessNormalMsg(msg.normalMsg)
						if err != nil {
							logger.Warningf("Discarding bad normal message: %s", err)
							continue
						}
					}
					cutter := ch.support.BlockCutter()
					if cutter.GetPreviousPendingBatchStartTime() == nil {
						now := time.Now()
						cutter.SetPreviousPendingBatchStartTime(&now)
					}
					batches, pending := cutter.Ordered(msg.normalMsg)

					if infolab.OneMsgTime {
						if len(batches) == 0 {
							if receiveCount >= 0 {
								oneMsgEndTimeTime := time.Since(oneMsgStartTime)
								oneMsgDurationAverage += oneMsgEndTimeTime.Nanoseconds()
								oneMsgDurationCount++
							}
						} else {
							receiveCount++
							if receiveCount > 20 {
								thousandthMsgOrderedDurationAverage += time.Since(oneMsgStartTime).Nanoseconds()
								thousandMsgRecvDurationTotal += time.Since(thousandMsgRecvDurationStart).Nanoseconds()
							}
							// if receiveCount >= 301 && receiveCount <= 3300 {
							// 	thousandthMsgOrderedDurationAverage += time.Since(oneMsgStartTime).Nanoseconds()
							// }
							if receiveCount > 770 {
								// fmt.Printf("** Thousandth msg ordered duration average: %d\n", thousandthMsgOrderedDurationAverage/3000)
								// infolab.Log.Printf("ThousandMsgAvg:%d\n", thousandthMsgOrderedDurationAverage / receiveCount)
								// infolab.Log.Printf("ThousandMsgAvg:%d\n", thousandthMsgOrderedDurationAverage / receiveCount)
								infolab.Log.Printf("hiThousandMsgAvg:%d\n", thousandMsgRecvDurationTotal/(receiveCount-20))
								infolab.Log.Printf("FastPeerDurAvg:%d\n", fastPeerResponseDurationTotal/(receiveCount-20))

							}
						}

					}

					// 여기서 1000개의 메세지가 추가되어야 batch 하나가 생김
					for _, batch := range batches {
						fmt.Printf("Creating batch\n")
						lastIsBatch = true
						one_batch_startTime := time.Now()
						if infolab.DebugMode && infolab.Orderer_batchTime {
							receivePeriod = time.Since(batch_startTime) - batch_lastTime
							batch_lastTime = time.Since(batch_startTime)
							fmt.Printf("Orderer batch[%d] period is %d\n", receiveCount, receivePeriod.Nanoseconds())
							if receiveCount >= 301 {
								receivePeriodAverage += receivePeriod.Nanoseconds()
								// fmt.Printf("Block receive period is %d and count is %d\n", receivePeriod.Nanoseconds(), receiveCount)
							}
							if receiveCount == 3300 {
								receivePeriodAverage = receivePeriodAverage / 3000
								fmt.Printf("** Orderer batch receive period average is %d\n", receivePeriodAverage)
							}
						}

						func(batch []*cb.Envelope) {
							var block *cb.Block
							CreateNextBlockCount++
							block = ch.support.CreateNextBlock(batch)
							ch.support.WriteBlock(block, nil)

							// infolab.AddPendingBatchStartTime(block.Header.Number, cutter.GetPreviousPendingBatchStartTime())
							one_batch_endTime := time.Since(one_batch_startTime)
							if infolab.DebugMode && infolab.Orderer_batchTime {
								fmt.Printf("Orderer batch[%d] process is %d\n", receiveCount, one_batch_endTime.Nanoseconds())
								if receiveCount >= 301 {
									processeAverage += one_batch_endTime.Nanoseconds()
									// fmt.Printf("Block receive period is %d and count is %d\n", receivePeriod.Nanoseconds(), receiveCount)
								}
								if receiveCount == 3300 {
									processeAverage = processeAverage / 3000
									fmt.Printf("** Orderer batch process average is %d\n", processeAverage)
								}
							}

						}(batch)

						if receiveCount > 10 {
							if stepOneCount > 0 {
								infolab.Log.Printf("UpdateDegreeAvg:%d", stepOneTimeTotal/stepOneCount)
							}
							if stepTwoCount > 0 {
								infolab.Log.Printf("Ordering:%d", stepTwoTimeTotal/stepTwoCount)
							}
							if stepThreeCount > 0 {
								infolab.Log.Printf("TSP:%d", stepThreeTimeTotal/stepThreeCount)
							}
							if stepFourCount > 0 {
								infolab.Log.Printf("CreateNextBlock:%d", stepFourTimeTotal/stepFourCount)
							}
							if txsInBlockCount > 0 {
								infolab.Log.Printf("TxsInBlock:%d", txsInBlockTotal/txsInBlockCount)
							}
							infolab.Log.Printf("BlockNum:%d", receiveCount)
						}

					}
					// cutter.SetPreviousPendingBatchStartTime(nil)

					if infolab.DebugMode && infolab.OneMsgTime {
						if receiveCount >= 301 && receiveCount <= 3300 {
							oneMsgEndTimeTime := time.Since(oneMsgStartTime)
							thousandthMsgDurationAverage += oneMsgEndTimeTime.Nanoseconds()
						}
						if receiveCount == 3300 && !printOnce {
							printOnce = true
							thousandthMsgDurationAverage = thousandthMsgDurationAverage / 3000
							fmt.Printf("** oneMsgDurationCount: %d\n", oneMsgDurationCount)
							fmt.Printf("** oneMsgDurationAverage: %d\n", oneMsgDurationAverage/oneMsgDurationCount)
							fmt.Printf("** thousandth msg duration average is %d\n", thousandthMsgDurationAverage)
						}
					}

					switch {
					case timer != nil && !pending:
						// Timer is already running but there are no messages pending, stop the timer
						timer = nil
					case timer == nil && pending:
						// Timer is not already running and there are messages pending, so start it
						timer = time.After(ch.support.SharedConfig().BatchTimeout())
						logger.Debugf("Just began %s batch timer", ch.support.SharedConfig().BatchTimeout().String())
					default:
						// Do nothing when:
						// 1. Timer is already running and there are messages pending
						// 2. Timer is not set and there are no messages pending
					}

				} else {
					// ConfigMsg
					if msg.configSeq < seq {
						msg.configMsg, _, err = ch.support.ProcessConfigMsg(msg.configMsg)
						if err != nil {
							logger.Warningf("Discarding bad config message: %s", err)
							continue
						}
					}
					batch := ch.support.BlockCutter().Cut()
					if batch != nil {
						block := ch.support.CreateNextBlock(batch)
						ch.support.WriteBlock(block, nil)
					}

					block := ch.support.CreateNextBlock([]*cb.Envelope{msg.configMsg})
					ch.support.WriteConfigBlock(block, nil)
					timer = nil
				}
			case <-timer:
				//clear the timer
				timer = nil

				timerExpireCount++
				fmt.Printf("Timer called: %d\n", timerExpireCount)

				if timerExpireCount > 6 { // After testing
					if lastIsBatch {
						fastPeerResponseDurationStart = time.Now()

						fastPeerResponseDurationTotal += time.Since(fastPeerResponseDurationStart).Nanoseconds()
						thousandMsgRecvDurationStart = time.Now()
					}
					lastIsBatch = true
				}

				cutter := ch.support.BlockCutter()
				batch := cutter.Cut()
				if len(batch) == 0 {
					logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
					continue
				}
				logger.Debugf("Batch timer expired, creating block")
				block := ch.support.CreateNextBlock(batch)
				ch.support.WriteBlock(block, nil)
				t := cutter.GetPreviousPendingBatchStartTime()
				if t != nil {
					infolab.AddPendingBatchStartTime(block.Header.Number, t)
					cutter.SetPreviousPendingBatchStartTime(nil)
				}
			case <-ch.exitChan:
				logger.Debugf("Exiting")
				return
			}
		}
	}

}
