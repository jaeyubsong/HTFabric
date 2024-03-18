package infolab

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/fastfabric/config"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/peer"
)

type histogramCell struct {
	key    string
	isHub  bool
	reads  uint
	writes uint
}

const redisChannelForFlush = "histogram-flush"
const redisKeyForDegreeRank = "degree-rank"
const redisKeyForLatestReceivedBlockID = "latest-received-block-id"
const redisScanCount = 1000

// FlushInterval 상수는 히스토그램 반영 주기로 쓰이는 블록 번호를 나타낸다.
var FlushInterval uint64

var histogramEnabled bool
var hubMethod string
var hubN float64
var premade bool

// histogram 변수는 (키, *histogramCell) 쌍의 딕셔너리이다.
// var histogram = sync.Map{}
// var lruHistogram *lru.Cache

var BootstrapCatchUp func(by uint64)

// Flush 함수는 히스토그램을 Redis에 반영시킨다.
// 이 때 각 키에 대한 읽기·쓰기 횟수는 초기화된다.
func Flush() {
	if DebugMode {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(infolab/histogram.go) Flush called from %s#%d\n", file, no)
		}
	}
	if config.IsEndorser || config.IsStorage {
		v, err := redisClient.Get(redisKeyForLatestReceivedBlockID).Result()
		if err != nil {
			panic(err)
		}
		w, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Flush: %d / %d\n", w, LatestReceivedBlockID)
		// if uint64(w) > MaxOutdatedBlockCount+LatestReceivedBlockID {
		// 	BootstrapCatchUp(uint64(w))
		// }
		if CatchUpGossipingEnabled {
			BootstrapCatchUp(uint64(w))
		}
	} else {
		err := redisClient.Set(redisKeyForLatestReceivedBlockID, LatestReceivedBlockID, 0).Err()
		if err != nil {
			panic(err)
		}
	}
	if !histogramEnabled {
		return
	}
	// if premade {
	// 	fmt.Println("Flush: premade이므로 중단합니다.")
	// 	return
	// }
	// var threshold uint
	// var hubs = 0
	// var zBatch = []redis.Z{}

	// switch hubMethod {
	// case "fixed-threshold":
	// 	threshold = uint(hubN)
	// case "top-n":
	// 	cutline := int64(hubN) - 1
	// 	list, err := redisClient.ZRevRangeWithScores(redisKeyForDegreeRank, cutline, cutline).Result()
	// 	if err != nil {
	// 		logger.Error("Flush: ZREVRANGE 실행 중 오류가 발생했습니다: " + err.Error())
	// 		return
	// 	}
	// 	if len(list) != 1 {
	// 		logger.Errorf("Flush: ZREVRANGE 실행 결과(%d)가 예상(1)과 어긋납니다.", len(list))
	// 		return
	// 	}
	// 	threshold = uint(list[0].Score)
	// case "top-n-percent":
	// 	histogramLength := getHistogramLength()
	// 	fmt.Printf("Flush: 히스토그램 길이: %d\n", histogramLength)
	// 	cutline := int64(float64(histogramLength)*hubN*0.01) - 1

	// 	_, test := redisClient.ZCard(redisKeyForDegreeRank).Result()
	// 	if test != nil {
	// 		logger.Panic("Flush: ZREVRANGE 실행 중 오류가 발생했습니다: " + test.Error())
	// 	}
	// 	// fmt.Printf("ZCard: %d (cutline: %d)\n", card, cutline)
	// 	list, err := redisClient.ZRevRangeWithScores(redisKeyForDegreeRank, cutline, cutline).Result()
	// 	if err != nil {
	// 		logger.Error("Flush: ZREVRANGE 실행 중 오류가 발생했습니다: " + err.Error())
	// 		return
	// 	}
	// 	if len(list) != 1 {
	// 		logger.Errorf("Flush: ZREVRANGE 실행 결과(%d)가 예상(1)과 어긋납니다.", len(list))
	// 		return
	// 	}
	// 	threshold = uint(list[0].Score)
	// default:
	// 	logger.Errorf("Flush: 처리되지 않은 hubMethod: %s", hubMethod)
	// 	return
	// }
	// fmt.Printf("Flush: 역치: %d\n", threshold)
	// histogram.Range(func(_key, _value interface{}) bool {
	// 	var key = _key.(string)
	// 	var cell = _value.(*histogramCell)

	// 	var keyForHub = "h." + key

	// 	cell.isHub = cell.getDegree() >= threshold
	// 	if cell.isHub {
	// 		hubs++
	// 		redisClient.Set(keyForHub, "true", 0)
	// 	} else {
	// 		redisClient.Del(keyForHub)
	// 	}
	// 	zBatch = append(zBatch, cell.clean())

	// 	return true
	// })
	// go redisClient.ZAdd(redisKeyForDegreeRank, zBatch...)
	// redisClient.Publish(redisChannelForFlush, nil)
	// fmt.Printf("Flush: 키 %d개가 허브로 지정됨\n", hubs)
}

// Put 함수는 주어진 읽기·쓰기 집합을 히스토그램에 반영시키고, 각 키들의 메타데이터를 구해 반환한다.
func Put(transactionRWSet *rwset.TxReadWriteSet) []*peer.KeyMetadata {
	var R = []*peer.KeyMetadata{}
	// var zTable = map[string]redis.Z{}
	// var zBatch []redis.Z

	rwSet, err := rwsetutil.TxRwSetFromProtoMsg(transactionRWSet)
	if err != nil {
		logger.Error(err)
		return nil
	}
	for _, namespace := range rwSet.NsRwSets {
		if namespace.NameSpace != "InfoCC" {
			continue
		}
		for _, read := range namespace.KvRwSet.Reads {
			var keyMetadata *peer.KeyMetadata
			if read.Version == nil {
				keyMetadata = getKeyMetadata(read.Key, 0, 0, false)
			} else {
				keyMetadata = getKeyMetadata(read.Key, 0, read.Version.BlockNum, false)
			}
			R = append(R, keyMetadata)
			// zTable[read.Key] = put(read.Key, false, 1)
		}
		for _, write := range namespace.KvRwSet.Writes {
			v, _ := strconv.ParseInt(string(write.Value), 10, 64)
			R = append(R, getKeyMetadata(write.Key, v, 0, true))
			// zTable[write.Key] = put(write.Key, true, 1)
		}
	}
	// zBatch = make([]redis.Z, len(zTable))
	// for _, z := range zTable {
	// 	zBatch = append(zBatch, z)
	// }
	// redisClient.ZAdd(redisKeyForDegreeRank, zBatch...)

	return R
}

func (cell *histogramCell) clean() redis.Z {
	cell.reads = 0
	cell.writes = 0

	return redis.Z{
		Member: cell.key,
		Score:  0,
	}
}
func (cell *histogramCell) getDegree() uint {
	return cell.reads + cell.writes
}

func getHistogramLength() uint {
	var length uint = 0

	// histogram.Range(func(_, _ interface{}) bool {
	// 	length++
	// 	return true
	// })
	return length
}
func getKeyMetadata(key string, value int64, version uint64, writing bool) *peer.KeyMetadata {
	// var hubness = peer.Hubness_INVALID
	// var hub, degree = isHub(key)

	// if hub {
	// 	hubness = peer.Hubness_HUB
	// } else {
	// 	hubness = peer.Hubness_NORMAL
	// }
	// if _, ok := lruHistogram.Get(key); ok {
	// 	degree = 0
	// }
	return &peer.KeyMetadata{
		Name:    []byte(key),
		Value:   value,
		Version: version,
		// Degree:  uint32(degree),
		Writing: writing,
		// Hubness: hubness,
	}
}

// func isHub(key string) (bool, uint) {
// 	var cell *histogramCell

// 	if _cell, ok := histogram.Load(key); ok {
// 		cell = _cell.(*histogramCell)
// 	} else {
// 		return false, 0
// 	}
// 	return cell.isHub, cell.reads + cell.writes
// }
func premakeHistogram() {
	var path = fmt.Sprintf("/home/jjoriping/Power-Fabric/inputs/%s.bundled", os.Getenv("CURRENT_INPUT"))
	var file *os.File
	var scanner *bufio.Scanner
	var zBatch = []redis.Z{}
	var err error

	fmt.Printf("premakeHistogram: %s\n", path)
	file, err = os.Open(path)
	if err != nil {
		logger.Panic("Open: " + err.Error())
	}
	defer file.Close()

	scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		// for _, k := range strings.Split(scanner.Text(), "\t") {
		// JNOTE 실제로는 cell.reads와 cell.writes가 1씩 증가해야 한다.
		// zBatch = append(zBatch, put(k, true, 2))
		// }
	}
	if err = scanner.Err(); err != nil {
		logger.Panic("scanner: " + err.Error())
	}
	for i := 0; i < len(zBatch); i += 10000 {
		max := i + 10000
		if max > len(zBatch) {
			max = len(zBatch)
		}
		fmt.Printf("premakeHistogram: %d / %d\n", i, len(zBatch))
		if _, err = redisClient.ZAdd(redisKeyForDegreeRank, zBatch[i:max]...).Result(); err != nil {
			logger.Panic("ZAdd: " + err.Error())
		}
	}
	time.Sleep(time.Second)
	Flush()
	premade = true
}
func prepareInput() {
	var chunk = strings.Split(os.Getenv("CURRENT_INPUT"), ".")
	var command = exec.Command(os.Getenv("GOPATH")+"/src/github.com/hyperledger/fabric/fastfabric/scripts/prepare.sh", chunk[0])

	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		logger.Panic("Run: " + err.Error() + " (" + os.Getenv("CURRENT_INPUT") + ")")
	}
}

// func put(key string, write bool, value uint) redis.Z {
// 	var cell *histogramCell

// 	if _cell, ok := histogram.Load(key); ok {
// 		cell = _cell.(*histogramCell)
// 	} else {
// 		cell = &histogramCell{
// 			key:    key,
// 			isHub:  false,
// 			reads:  0,
// 			writes: 0,
// 		}
// 		histogram.Store(key, cell)
// 		lruHistogram.Add(key, cell)
// 	}
// 	if write {
// 		cell.writes += value
// 	} else {
// 		cell.reads += value
// 	}
// 	return redis.Z{
// 		Member: key,
// 		Score:  float64(cell.getDegree()),
// 	}
// }
