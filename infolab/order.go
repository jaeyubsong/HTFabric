package infolab

import (
	"fmt"
	"hash/fnv"
	"log"
	"runtime"
	"time"

	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/infolab/ordering"
	"github.com/hyperledger/fabric/infolab/partitioning"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

var Log *log.Logger

var Partitions uint8
var Slots uint8
var partitioningMethod string

var isOrderer bool
var ordererConfigurationTable = map[string]channelconfig.Orderer{}
var orderingMethod string

// AddOrdererConfig 함수는 설정 객체를 표에 추가하고, 그 정보를 Redis에 기록한다.
func AddOrdererConfig(channel string, ordererConfig channelconfig.Orderer) {
	ordererConfigurationTable[channel] = ordererConfig
	updateBenchmarkConfiguration()
}

var UpdateDegreeCount int64
var totalUpdateDegreeCountTime int64

// UpdateDegree 함수는 주어진 pendingBatch로부터 degree 정보를 반영한다.
func UpdateDegree(batch []*common.Envelope) {
	// fmt.Printf("Called UpdateDegree\n")
	if PartialFunctionOrdering == true && Partitions == 1 && (orderingMethod == "plain" || orderingMethod == "random") {
		// fmt.Printf("No update degree, just return\n")
		return
	}
	// fmt.Printf("Going below as Partitions: %d and orderingMethod: %s\n", Partitions, orderingMethod)
	UpdateDegreeCount++
	startTime := time.Now()
	if DebugMode && TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(infolab/orderer.go) UpdateDegree() called from %s#%d\n", file, no)
		}
	}
	for _, v := range batch {
		var action, err = utils.GetActionFromEnvelopeMsg(v)

		if err != nil {
			logger.Warning("GetActionFromEnvelopeMsg: " + err.Error())
			continue
		}
		if len(action.Response.KeyMetadataList) < 2 {
			logger.Errorf("Non-invoke2 result: %d\n", len(action.Response.KeyMetadataList))
			continue
		}
		v1 := string(action.Response.KeyMetadataList[0].Name)
		v2 := string(action.Response.KeyMetadataList[1].Name)
		if d1, ok := partitioning.DegreeTable[v1]; ok {
			partitioning.DegreeTable[v1] = d1 + 1
		} else {
			partitioning.DegreeTable[v1] = 1
		}
		if d2, ok := partitioning.DegreeTable[v2]; ok {
			partitioning.DegreeTable[v2] = d2 + 1
		} else {
			partitioning.DegreeTable[v2] = 1
		}
	}
	takenTime := time.Since(startTime)
	totalUpdateDegreeCountTime += takenTime.Nanoseconds()
	if UpdateDegreeCount > 770 {
		avgTimeTaken := totalUpdateDegreeCountTime / UpdateDegreeCount
		// fmt.Printf("UpdateDegreeTableAvg:%d\n", avgTimeTaken)
		Log.Printf("UpdateDegreeTableAvg:%d\n", avgTimeTaken)
	}
	// if DebugMode && UpdateDegreeTime {
	// 	fmt.Printf("[UpdateDegree %d] Time taken: %d\n", UpdateDegreeCount, takenTime.Nanoseconds())
	// 	totalUpdateDegreeCountTime += takenTime.Nanoseconds()
	// 	// if UpdateDegreeCount >= 301 {
	// 	// 	totalUpdateDegreeCountTime += takenTime.Nanoseconds()
	// 	// }
	// 	if UpdateDegreeCount == 3300 {
	// 		avgTimeTaken := totalUpdateDegreeCountTime / 3000
	// 		fmt.Printf("** UpdateDegreeAverage is: %d\n", avgTimeTaken)
	// 	}
	// }
	// fmt.Printf("Finished UpdateDegree\n")

}

// UpdateDegreeParallel 함수는 주어진 pendingBatch로부터 degree 정보를 반영한다.
func UpdateDegreeParallel(batch []*common.Envelope) (map[string]uint32, map[string]uint32) {
	// fmt.Printf("Called UpdateDegreeParallel\n")
	localWriteDegreeTable := make(map[string]uint32)
	localReadDegreeTable := make(map[string]uint32)

	// No need to use degree table at all
	if PartialFunctionOrdering == true && Partitions == 1 && (orderingMethod == "plain" || orderingMethod == "random") {
		// fmt.Printf("No update degree, just return\n")
		return localReadDegreeTable, localWriteDegreeTable
	}
	// fmt.Printf("Going below as Partitions: %d and orderingMethod: %s\n", Partitions, orderingMethod)
	UpdateDegreeCount++
	startTime := time.Now()
	if DebugMode && TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(infolab/orderer.go) UpdateDegree() called from %s#%d\n", file, no)
		}
	}
	for _, v := range batch {
		var action, err = utils.GetActionFromEnvelopeMsg(v)

		if err != nil {
			logger.Warning("GetActionFromEnvelopeMsg: " + err.Error())
			continue
		}
		if len(action.Response.KeyMetadataList) < 2 {
			logger.Errorf("Non-invoke2 result: %d\n", len(action.Response.KeyMetadataList))
			continue
		}
		// fmt.Printf("Length of keyMetadataList: %d\n", len(action.Response.KeyMetadataList))
		for i := 0; i < len(action.Response.KeyMetadataList); i++ {
			v1 := string(action.Response.KeyMetadataList[i].Name)
			if action.Response.KeyMetadataList[i].Writing {
				if d1, ok := partitioning.WriteDegreeTable[v1]; ok {
					partitioning.WriteDegreeTable[v1] = d1 + 1
					localWriteDegreeTable[v1] = d1 + 1
				} else {
					partitioning.WriteDegreeTable[v1] = 1
					localWriteDegreeTable[v1] = 1
				}
			} else {
				if d1, ok := partitioning.ReadDegreeTable[v1]; ok {
					partitioning.ReadDegreeTable[v1] = d1 + 1
					localReadDegreeTable[v1] = d1 + 1
				} else {
					partitioning.ReadDegreeTable[v1] = 1
					localReadDegreeTable[v1] = 1
				}
			}

			// if d1, ok := partitioning.DegreeTable[v1]; ok {
			// 	partitioning.DegreeTable[v1] = d1 + 1
			// 	localDegreeTable[v1] = d1 + 1
			// } else {
			// 	partitioning.DegreeTable[v1] = 1
			// 	localDegreeTable[v1] = 1
			// }
		}
		// v1 := string(action.Response.KeyMetadataList[0].Name)
		// v2 := string(action.Response.KeyMetadataList[1].Name)
		// if d1, ok := partitioning.DegreeTable[v1]; ok {
		// 	partitioning.DegreeTable[v1] = d1 + 1
		// 	localDegreeTable[v1] = d1 + 1
		// } else {
		// 	partitioning.DegreeTable[v1] = 1
		// 	localDegreeTable[v1] = 1
		// }
		// if d2, ok := partitioning.DegreeTable[v2]; ok {
		// 	partitioning.DegreeTable[v2] = d2 + 1
		// 	localDegreeTable[v2] = d2 + 1
		// } else {
		// 	partitioning.DegreeTable[v2] = 1
		// 	localDegreeTable[v2] = 1
		// }
	}
	takenTime := time.Since(startTime)
	if DebugMode && UpdateDegreeTime {
		fmt.Printf("[UpdateDegree %d] Time taken: %d\n", UpdateDegreeCount, takenTime.Nanoseconds())
		if UpdateDegreeCount >= 301 {
			totalUpdateDegreeCountTime += takenTime.Nanoseconds()
		}
		if UpdateDegreeCount == 3300 {
			avgTimeTaken := totalUpdateDegreeCountTime / 3000
			fmt.Printf("** UpdateDegreeAverage is: %d\n", avgTimeTaken)
		}
	}
	// fmt.Printf("Finished UpdateDegreeParallel\n")

	return localReadDegreeTable, localWriteDegreeTable
}

var OrderBatchCount int64
var totalOrderBatchTime int64

// OrderBatch 함수는 전송 직전 pendingBatch를 정렬하고 그 결과를 반환한다.
func OrderBatch(m metrics.Histogram, batch []*common.Envelope) []*common.Envelope {
	OrderBatchCount++
	startTime := time.Now()
	fmt.Printf("OrderBatch[%s] called\n", orderingMethod)
	if DebugMode && TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(fabric/infolab.orderer.go) OrderBatch called from %s#%d\n", file, no)
		}
	}
	if len(batch) < 2 {
		return batch
	}
	switch orderingMethod {
	case "plain":
		return batch
	case "random":
		return ordering.OrderByRandom(batch)
	case "degree-asc":
		batchResult := ordering.OrderByDegree(batch)
		takenTime := time.Since(startTime)
		totalOrderBatchTime += takenTime.Nanoseconds()
		if OrderBatchCount > 770 {
			avgTimeTaken := totalOrderBatchTime / OrderBatchCount
			// fmt.Printf("UpdateDegreeTableAvg:%d\n", avgTimeTaken)
			Log.Printf("WardAvg:%d\n", avgTimeTaken)
		}
		return batchResult
	case "hub-ending":
		return ordering.OrderByHubEnding(batch)
	case "fabric++":
		return ordering.OrderByFabricPP(m, batch)
	default:
		logger.Errorf("알 수 없는 유형: %s", orderingMethod)
		return nil
	}
}

// OrderBatchParallel 함수는 전송 직전 pendingBatch를 정렬하고 그 결과를 반환한다.
func OrderBatchParallel(batch []*common.Envelope, localReadDegreeTable map[string]uint32, localWriteDegreeTable map[string]uint32) []*common.Envelope {
	if DebugMode && TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(fabric/infolab.orderer.go) OrderBatch called from %s#%d\n", file, no)
		}
	}
	if len(batch) < 2 {
		return batch
	}
	switch orderingMethod {
	case "plain":
		return batch
	case "random":
		return ordering.OrderByRandom(batch)
	case "degree-asc":
		return ordering.OrderByDegreeParallel(batch, localReadDegreeTable, localWriteDegreeTable)
	case "hub-ending":
		return ordering.OrderByHubEnding(batch)
	// case "fabric++":
	// 	return ordering.OrderByFabricPP(m, batch)
	default:
		logger.Errorf("알 수 없는 유형: %s", orderingMethod)
		return nil
	}
}

func getHash(target []byte, mod uint8) (R byte) {
	// v, err := strconv.Atoi(string(target))
	// if err != nil {
	// 	return 0
	// }
	// return byte(v % int(mod))
	var hash = fnv.New64a()

	hash.Write(target)
	R = byte(hash.Sum64() % uint64(mod))

	return R
}

/// PartitionOnce 함수는 주어진 트랜잭션의 담당 파티션 번호를 구해 반환한다.
func PartitionOnce(e *common.Envelope) byte {
	switch partitioningMethod {
	case "round-robin":
		return 0
	case "dbh":
		var action, err = utils.GetActionFromEnvelopeMsg(e)
		var d1, d2 uint32

		if err != nil {
			logger.Warning("GetActionFromEnvelopeMsg: " + err.Error())
			return 0
		}
		if len(action.Response.KeyMetadataList) < 2 {
			logger.Errorf("Non-invoke2 result: %d\n", len(action.Response.KeyMetadataList))
			return 0
		}
		v1 := action.Response.KeyMetadataList[0].Name
		v2 := action.Response.KeyMetadataList[1].Name
		// if d, ok := lruHistogram.Get(string(v1)); ok {
		// 	d1 = d.(uint32)
		// } else {
		// 	d1 = action.Response.KeyMetadataList[0].Degree
		// 	lruHistogram.Add(string(v1), d1)
		// }
		// if d, ok := lruHistogram.Get(string(v2)); ok {
		// 	d2 = d.(uint32)
		// } else {
		// 	d2 = action.Response.KeyMetadataList[1].Degree
		// 	lruHistogram.Add(string(v2), d2)
		// }
		if d1 < d2 {
			return getHash(v1, Partitions)
		}
		return getHash(v2, Partitions)
	default:
		logger.Errorf("알 수 없는 유형: %s", partitioningMethod)
		return 0
	}
}

// PartitionBatch 함수는 전송 직전 pendingBatch로부터 각 트랜잭션의 담당 파티션 번호를 구해 반환한다.
func PartitionBatch(batch []*common.Envelope) []byte {
	if DebugMode && TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(fabric/infolab.orderer.go) PartitionBatch called from %s#%d\n", file, no)
		}
	}
	var R = make([]byte, len(batch))

	// if partitioningMethod == "round-robin" {
	// 	for i := range batch {
	// 		R[i] = byte(i % int(Partitions))
	// 	}
	// } else {
	// 	for i, v := range batch {
	// 		if v.Partition == nil {
	// 			logger.Warningf("Partition is nil (#%d)", i)
	// 			R[i] = byte(i % int(Partitions))
	// 		} else {
	// 			R[i] = v.Partition[0]
	// 		}
	// 	}
	// }
	// return R

	switch partitioningMethod {
	case "round-robin":
		for i := range batch {
			R[i] = byte(i % int(Partitions))
		}
	case "dbh":
		partitioning.PartitionByDBH(Partitions, R, batch)
	case "greedy":
		partitioning.PartitionByGreedy(Partitions, R, batch)
	case "hdrf":
		partitioning.PartitionByHDRF(Partitions, R, batch)
	case "mm":
		partitioning.PartitionByMM(Partitions, R, batch)
	case "mm-l5":
		partitioning.PartitionByMML5(Partitions, R, batch)
	case "mm-n2p":
		partitioning.PartitionByMMN2P(Partitions, R, batch)
	default:
		logger.Errorf("알 수 없는 유형: %s", partitioningMethod)
		return nil
	}
	return R
}

// PartitionBatchParallel 함수는 전송 직전 pendingBatch로부터 각 트랜잭션의 담당 파티션 번호를 구해 반환한다.
func PartitionBatchParallel(batch []*common.Envelope, localReadDegreeTable map[string]uint32, localWriteDegreeTable map[string]uint32) []byte {
	if DebugMode && TraceOrderer {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(fabric/infolab.orderer.go) PartitionBatch called from %s#%d\n", file, no)
		}
	}
	var R = make([]byte, len(batch))

	// if partitioningMethod == "round-robin" {
	// 	for i := range batch {
	// 		R[i] = byte(i % int(Partitions))
	// 	}
	// } else {
	// 	for i, v := range batch {
	// 		if v.Partition == nil {
	// 			logger.Warningf("Partition is nil (#%d)", i)
	// 			R[i] = byte(i % int(Partitions))
	// 		} else {
	// 			R[i] = v.Partition[0]
	// 		}
	// 	}
	// }
	// return R

	switch partitioningMethod {
	case "round-robin":
		for i := range batch {
			R[i] = byte(i % int(Partitions))
		}
	case "dbh":
		partitioning.PartitionByDBHParallel(Partitions, R, batch, localReadDegreeTable, localWriteDegreeTable)
	case "greedy":
		partitioning.PartitionByGreedyParallel(Partitions, R, batch, localReadDegreeTable)
	case "hdrf":
		partitioning.PartitionByHDRFParallel(Partitions, R, batch, localReadDegreeTable)
	case "mm":
		partitioning.PartitionByMMParallel(Partitions, R, batch, localReadDegreeTable)
	case "mm-l5":
		partitioning.PartitionByMML5Parallel(Partitions, R, batch, localReadDegreeTable)
	case "mm-n2p":
		partitioning.PartitionByMMN2PParallel(Partitions, R, batch, localReadDegreeTable)
	default:
		logger.Errorf("알 수 없는 유형: %s", partitioningMethod)
		return nil
	}
	return R
}
