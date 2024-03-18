package infolab

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/msp/mgmt"
	"github.com/spf13/viper"
)

// InitializationData 구조체는 Initialize 함수 호출을 위해 필요한 정보를 담은 구조체이다.
type InitializationData struct {
	IsOrderer bool
	// 피어 전용 필드들
	Endorser        iEndorser
	EndorserSupport iEndorserSupport
}

const redisKeyForConfiguration = "benchmark-configuration"

var logger = flogging.MustGetLogger("infolab")
var CatchUpGossipingEnabled bool
var MaxOutdatedBlockCount uint64
var EarlyResimulationEnabled bool

var LatestReceivedBlockID uint64
var UpdateEndorserBlockchainHeight func(height uint64)
var lruSize int
var GoMaxProcs int

var DebugMode bool
var ValidateAndPrepare bool
var AfterValidateTime bool
var ValidateAndPrepareBatchCallTime bool
var HashGet bool
var HashPut bool
var StateGet bool
var StatePut bool
var GossipPayload bool
var MsgSendTime bool
var MsgRecvTime bool
var FastPayloadRecvTime bool
var ValidCheckTps bool
var MeasureTps bool
var CheckMetric bool
var TraceOrderer bool
var UpdateDegreeTime bool
var DegreeByOrderTIme bool
var CreateNextBlockTime bool
var Orderer_commitBlockTime bool
var Orderer_batchTime bool
var OneMsgTime bool
var Orderer_processMsgTime bool
var MeasureValidateAndPrepareBatchTime bool

var TraceOrdererWriteBlock bool

var PartialFunctionOrdering bool

var OrdererGoMaxProcs int
var EndorserGoMaxProcs int

var DeferredSimulation bool

var BatchValid bool
var DsQueueSize int
var DsLoc string
var FastWb bool

var ParallelOrderer bool

var CheckIntra bool
var CheckInter bool

var UseLocalDegreeTable bool

var NoResim bool

var MaxBlockSize float64
var DeferredVersion int

var DeterministicResim bool

var EndorNum int


// Initialize 함수는 각종 viper 설정값들을 불러와 실행을 준비한다.
func Initialize(data *InitializationData) {
	var err error

	EmbeddedChaincode = viper.GetBool("infolab.simulation.embedded")
	FlushInterval = uint64(viper.GetInt("infolab.histogram.flushInterval"))
	histogramEnabled = viper.GetBool("infolab.histogram.enabled")
	hubMethod = viper.GetString("infolab.histogram.hubMethod")
	hubN = viper.GetFloat64("infolab.histogram.hubN")
	premade = viper.GetBool("infolab.histogram.premade")
	lruSize = viper.GetInt("infolab.histogram.lruSize")
	orderingMethod = viper.GetString("infolab.ordering.method")
	Partitions = uint8(viper.GetInt("infolab.ordering.partitions"))
	Slots = uint8(viper.GetInt("infolab.ordering.slots"))
	partitioningMethod = viper.GetString("infolab.ordering.partitioningMethod")
	UseLocalDegreeTable = viper.GetBool("infolab.ordering.localDegreeTable")

	// prospectusEnabled = viper.GetBool("infolab.prospectus.enabled")
	// nonhubAbortionLimit = uint(viper.GetInt("infolab.prospectus.nonhubAbortionLimit"))
	// hubResimulationLimit = uint(viper.GetInt("infolab.prospectus.hubResimulationLimit"))
	CatchUpGossipingEnabled = viper.GetBool("infolab.catchUp.enabled")
	MaxOutdatedBlockCount = uint64(viper.GetInt("infolab.catchUp.maxOutdatedBlockCount"))
	EarlyResimulationEnabled = viper.GetBool("infolab.simulation.early")
	GoMaxProcs = viper.GetInt("peer.gomaxprocs")
	PartialFunctionOrdering = viper.GetBool("infolab.PartialFunctionOrdering")
	DebugMode = viper.GetBool("infolab.debugMode")

	OrdererGoMaxProcs = viper.GetInt("infolab.ordererGoMaxProcs")
	EndorserGoMaxProcs = viper.GetInt("infolab.endorserGoMaxProcs")


	DeferredSimulation = viper.GetBool("infolab.deferredSimulation")

	BatchValid = viper.GetBool("infolab.batchValid")

	DsQueueSize = viper.GetInt("infolab.dsQueueSize")
	DsLoc = viper.GetString("infolab.dsLoc")

	FastWb = viper.GetBool("infolab.fastWb")

	ParallelOrderer = viper.GetBool("infolab.ordering.parallel")

	CheckIntra = viper.GetBool("infolab.validation.intra")
	CheckInter = viper.GetBool("infolab.validation.inter")

	NoResim = viper.GetBool("infolab.noresim")
	MaxBlockSize = viper.GetFloat64("infolab.maxBlockSize")

	DeferredVersion = viper.GetInt("infolab.deferredVersion")

	DeterministicResim = viper.GetBool("infolab.deterministicResim")

	EndorNum = viper.GetInt("infolab.endorNum")


	ValidateAndPrepare = false
	AfterValidateTime = false
	ValidateAndPrepareBatchCallTime = false
	HashGet = false
	HashPut = false
	StateGet = false
	StatePut = false
	GossipPayload = false
	MsgSendTime = false
	MsgRecvTime = false
	FastPayloadRecvTime = false
	ValidCheckTps = false
	MeasureTps = false
	CheckMetric = false
	MeasureValidateAndPrepareBatchTime = false

	TraceOrderer = false
	UpdateDegreeTime = false
	DegreeByOrderTIme = false
	CreateNextBlockTime = false
	Orderer_commitBlockTime = false
	Orderer_batchTime = false
	OneMsgTime = true
	Orderer_processMsgTime = false

	TraceOrdererWriteBlock = true

	// lruHistogram, err = lru.New(lruSize)
	// if err != nil {
	// 	logger.Error("Initialize: lruHistogram 초기화 중 오류가 발생했습니다: " + err.Error())
	// }
	OpenRedisClient()
	isOrderer = data.IsOrderer
	if isOrderer {
		// subscribeFlush()
		updateBenchmarkConfiguration()
	} else {
		localSigningIdentity = mgmt.GetLocalSigningIdentityOrPanic()
		serializedSigningIdentity, err = localSigningIdentity.Serialize()
		if err != nil {
			logger.Error("Initialize: Serialize 실행 중 오류가 발생했습니다: " + err.Error())
		}
		endorser = data.Endorser
		endorserSupport = data.EndorserSupport
		if premade {
			premade = false
			prepareInput()
			premakeHistogram()
		}
		go func() {
			for {
				var m runtime.MemStats

				runtime.ReadMemStats(&m)
				fmt.Printf("메모리: %v M\n", m.Alloc/1048576)

				time.Sleep(5 * time.Second)
			}
		}()
	}
}

// JNOTE 오더러만 수행한다.
func getBenchmarkConfigurationText() string {
	var R = []string{
		fmt.Sprintf("< 트랜잭션 정렬 >"),
		fmt.Sprintf("\t%20s: %s", "method", orderingMethod),
		fmt.Sprintf("\t%20s: %d", "partitions", Partitions),
		fmt.Sprintf("\t%20s: %d", "slots", Slots),
		fmt.Sprintf("\t%20s: %s", "partitioningMethod", partitioningMethod),
		fmt.Sprintf("\t%20s: %d", "gomaxprocs", GoMaxProcs),
	}

	R = append(R, "", fmt.Sprintf("< 히스토그램 >"))
	if histogramEnabled {
		R = append(
			R,
			fmt.Sprintf("\t%20s: %d", "flushInterval", FlushInterval),
			fmt.Sprintf("\t%20s: %s", "hubMethod", hubMethod),
			fmt.Sprintf("\t%20s: %f", "hubN", hubN),
			fmt.Sprintf("\t%20s: %t", "premade", premade),
			fmt.Sprintf("\t%20s: %d", "lruSize", lruSize),
		)
	} else {
		R = append(R, "\t(비활성화됨)")
	}

	// R = append(R, "", fmt.Sprintf("< 계획서 >"))
	// if prospectusEnabled {
	// 	R = append(
	// 		R,
	// 		fmt.Sprintf("\t%20s: %d", "nonhubAbortionLimit", nonhubAbortionLimit),
	// 		fmt.Sprintf("\t%20s: %d", "hubResimulationLimit", hubResimulationLimit),
	// 	)
	// } else {
	// 	R = append(R, "\t(비활성화됨)")
	// }

	R = append(
		R, "", fmt.Sprintf("< 시뮬레이션 >"),
		fmt.Sprintf("\t%20s: %t", "embedded", EmbeddedChaincode),
		fmt.Sprintf("\t%20s: %t", "early", EarlyResimulationEnabled),
	)

	R = append(R, "", fmt.Sprintf("< 캐치업 >"))
	if CatchUpGossipingEnabled {
		R = append(
			R,
			fmt.Sprintf("\t%20s: %d", "maxOutdatedBlockCount", MaxOutdatedBlockCount),
		)
	} else {
		R = append(R, "\t(비활성화됨)")
	}

	for channel, configuration := range ordererConfigurationTable {
		R = append(
			R,
			"",
			fmt.Sprintf("< 채널: %s >", channel),
			fmt.Sprintf("\t%20s: %s", "OrdererType", configuration.ConsensusType()),
			fmt.Sprintf("\t%20s: %d㎳", "BatchTimeout", configuration.BatchTimeout()/1000000),
			fmt.Sprintf("\t%20s: %d", "MaxMessageCount", configuration.BatchSize().MaxMessageCount),
		)
	}
	return strings.Join(R, "\n")
}
func updateBenchmarkConfiguration() {
	var text = getBenchmarkConfigurationText()

	fmt.Printf("updateBenchmarkConfiguration: %s\n", redisKeyForConfiguration)
	fmt.Printf("본문: \n%s\n", text)

	redisClient.Set(redisKeyForConfiguration, text, 0)
}
