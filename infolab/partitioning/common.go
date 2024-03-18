package partitioning

import (
	"hash/fnv"
	"math/rand"
	"sync"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
)

type partitionSet = map[string]map[byte]bool
type parameters struct {
	set     partitionSet
	pMap    map[byte]uint
	maxSize uint
	minSize uint
	length  uint
	random  *rand.Rand

	s    float64
	p2n  map[byte]map[string]bool
	n2p  map[string][]byte
	save []int
	mark []string
	pids []byte
	indx []int
}

var logger = flogging.MustGetLogger("infolab.partitioning")

var DegreeTable = map[string]uint32{}
var ReadDegreeTable = map[string]uint32{}
var WriteDegreeTable = map[string]uint32{}


// GetEnvelope 함수는 주어진 정점과 degree 정보를 담은 트랜잭션을 만들어 반환한다.
func GetEnvelope(txID string, v1, v2 string, d1, d2 uint32, h1, h2 *version.Height) *common.Envelope {
	var results = &cached.TxRwSet{
		NsRwSets: []*cached.NsRwSet{
			{
				NameSpace: "InfoCC",
				KvRwSet: &kvrwset.KVRWSet{
					Reads: []*kvrwset.KVRead{
						&kvrwset.KVRead{
							Key: v1,
							Version: &kvrwset.Version{
								BlockNum: h1.BlockNum,
								TxNum:    h1.TxNum,
							},
						},
						&kvrwset.KVRead{
							Key: v2,
							Version: &kvrwset.Version{
								BlockNum: h2.BlockNum,
								TxNum:    h2.TxNum,
							},
						},
					},
					Writes: []*kvrwset.KVWrite{
						&kvrwset.KVWrite{
							Key:   v1,
							Value: []byte("value1"),
						},
						&kvrwset.KVWrite{
							Key:   v2,
							Value: []byte("value2"),
						},
					},
				},
			},
		},
	}
	resultsByte, _ := results.ToProtoBytes()

	return &common.Envelope{
		Payload: utils.MarshalOrPanic(
			&common.Payload{
				Header: &common.Header{
					ChannelHeader: utils.MarshalOrPanic(&common.ChannelHeader{
						TxId: txID,
						Type: int32(common.HeaderType_ENDORSER_TRANSACTION),
					}),
				},
				Data: utils.MarshalOrPanic(
					&pb.Transaction{
						Actions: []*pb.TransactionAction{
							{
								Payload: utils.MarshalOrPanic(&pb.ChaincodeActionPayload{
									Action: &pb.ChaincodeEndorsedAction{
										ProposalResponsePayload: utils.MarshalOrPanic(
											&pb.ProposalResponsePayload{
												Extension: utils.MarshalOrPanic(&pb.ChaincodeAction{
													Results: resultsByte,
													Response: &pb.Response{
														KeyMetadataList: []*pb.KeyMetadata{
															&pb.KeyMetadata{Name: []byte(v1), Version: h1.BlockNum},
															&pb.KeyMetadata{Name: []byte(v2), Version: h2.BlockNum},
														},
													},
												}),
											},
										),
									},
								}),
							},
						},
					},
				),
			},
		),
	}
}
func getHash(target []byte, mod uint8) byte {
	var hash = fnv.New64a()

	hash.Write(target)

	return byte(hash.Sum64() % uint64(mod))
}
func newParameters(batch []*common.Envelope) *parameters {
	var R = &parameters{
		set:     partitionSet{},
		pMap:    map[byte]uint{},
		maxSize: 0,
		length:  uint(len(batch)),
	}
	R.minSize = R.length

	return R
}
func partition(P *parameters, K uint8, partitions []byte, batch []*common.Envelope, processor func(P *parameters, K uint8, v1, v2 []byte, d1, d2 uint32) byte, constructMap bool) {
	P.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	wg := &sync.WaitGroup{}

	for i, v := range batch {
		wg.Add(1)
		go func(i int, v *common.Envelope) {
			defer wg.Done()
			var action, err = utils.GetActionFromEnvelopeMsg(v)

			if err != nil {
				logger.Warning("GetActionFromEnvelopeMsg: " + err.Error())
				partitions[i] = uint8(P.random.Intn(int(K)))
				return
			}
			if len(action.Response.KeyMetadataList) < 2 {
				logger.Errorf("Non-invoke2 result: %d\n", len(action.Response.KeyMetadataList))
				partitions[i] = uint8(P.random.Intn(int(K)))
				return
			}
			v1 := action.Response.KeyMetadataList[0].Name
			v2 := action.Response.KeyMetadataList[1].Name
			sv1 := string(v1)
			sv2 := string(v2)
			p := processor(
				P,
				K,
				v1,
				v2,
				DegreeTable[sv1],
				DegreeTable[sv2],
			)
			partitions[i] = p
			if !constructMap {
				return
			}
			if _, ok := P.set[sv1]; !ok {
				P.set[sv1] = map[byte]bool{}
			}
			if _, ok := P.set[sv2]; !ok {
				P.set[sv2] = map[byte]bool{}
			}
			P.set[sv1][p] = true
			P.set[sv2][p] = true
			if v, ok := P.pMap[p]; ok {
				P.pMap[p] = v + 1
			} else {
				P.pMap[p] = 1
			}
			if P.pMap[p] > P.maxSize {
				P.maxSize = P.pMap[p]
			}
			P.minSize = P.length
			for j := byte(0); j < K; j++ {
				if P.pMap[j] < P.minSize {
					P.minSize = P.pMap[j]
				}
			}
		}(i, v)
		if constructMap {
			wg.Wait()
		}
	}
	if !constructMap {
		wg.Wait()
	}
}

func partitionDBH(P *parameters, K uint8, partitions []byte, batch []*common.Envelope, processor func(P *parameters, K uint8, vList [][]byte, dList []uint32) byte, constructMap bool) {
	P.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	wg := &sync.WaitGroup{}

	for i, v := range batch {
		wg.Add(1)
		go func(i int, v *common.Envelope) {
			defer wg.Done()
			var action, err = utils.GetActionFromEnvelopeMsg(v)

			if err != nil {
				logger.Warning("GetActionFromEnvelopeMsg: " + err.Error())
				partitions[i] = uint8(P.random.Intn(int(K)))
				return
			}
			if len(action.Response.KeyMetadataList) < 2 {
				logger.Errorf("Non-invoke2 result: %d\n", len(action.Response.KeyMetadataList))
				partitions[i] = uint8(P.random.Intn(int(K)))
				return
			}
			vList := make([][]byte, 0)
			dList := make([]uint32, 0)
			for j := 0; j < len(action.Response.KeyMetadataList); j++ {
				vList = append(vList, action.Response.KeyMetadataList[j].Name)
				dList = append(dList, DegreeTable[string(vList[j])])
			}
			// v1 := action.Response.KeyMetadataList[0].Name
			// v2 := action.Response.KeyMetadataList[1].Name
			// sv1 := string(v1)
			// sv2 := string(v2)
			p := processor(
				P,
				K,
				vList,
				dList,
			)
			partitions[i] = p
			if !constructMap {
				return
			}
			// if _, ok := P.set[sv1]; !ok {
			// 	P.set[sv1] = map[byte]bool{}
			// }
			// if _, ok := P.set[sv2]; !ok {
			// 	P.set[sv2] = map[byte]bool{}
			// }
			// P.set[sv1][p] = true
			// P.set[sv2][p] = true
			// if v, ok := P.pMap[p]; ok {
			// 	P.pMap[p] = v + 1
			// } else {
			// 	P.pMap[p] = 1
			// }
			// if P.pMap[p] > P.maxSize {
			// 	P.maxSize = P.pMap[p]
			// }
			// P.minSize = P.length
			// for j := byte(0); j < K; j++ {
			// 	if P.pMap[j] < P.minSize {
			// 		P.minSize = P.pMap[j]
			// 	}
			// }
		}(i, v)
		if constructMap {
			wg.Wait()
		}
	}
	if !constructMap {
		wg.Wait()
	}
}



func partitionParallel(P *parameters, K uint8, partitions []byte, batch []*common.Envelope, localDegreeTable map[string]uint32, processor func(P *parameters, K uint8, v1, v2 []byte, d1, d2 uint32) byte, constructMap bool) {
	P.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	wg := &sync.WaitGroup{}

	for i, v := range batch {
		wg.Add(1)
		go func(i int, v *common.Envelope) {
			defer wg.Done()
			var action, err = utils.GetActionFromEnvelopeMsg(v)

			if err != nil {
				logger.Warning("GetActionFromEnvelopeMsg: " + err.Error())
				partitions[i] = uint8(P.random.Intn(int(K)))
				return
			}
			if len(action.Response.KeyMetadataList) < 2 {
				logger.Errorf("Non-invoke2 result: %d\n", len(action.Response.KeyMetadataList))
				partitions[i] = uint8(P.random.Intn(int(K)))
				return
			}
			v1 := action.Response.KeyMetadataList[0].Name
			v2 := action.Response.KeyMetadataList[1].Name
			sv1 := string(v1)
			sv2 := string(v2)
			p := processor(
				P,
				K,
				v1,
				v2,
				localDegreeTable[sv1],
				localDegreeTable[sv2],
			)
			partitions[i] = p
			if !constructMap {
				return
			}
			if _, ok := P.set[sv1]; !ok {
				P.set[sv1] = map[byte]bool{}
			}
			if _, ok := P.set[sv2]; !ok {
				P.set[sv2] = map[byte]bool{}
			}
			P.set[sv1][p] = true
			P.set[sv2][p] = true
			if v, ok := P.pMap[p]; ok {
				P.pMap[p] = v + 1
			} else {
				P.pMap[p] = 1
			}
			if P.pMap[p] > P.maxSize {
				P.maxSize = P.pMap[p]
			}
			P.minSize = P.length
			for j := byte(0); j < K; j++ {
				if P.pMap[j] < P.minSize {
					P.minSize = P.pMap[j]
				}
			}
		}(i, v)
		if constructMap {
			wg.Wait()
		}
	}
	if !constructMap {
		wg.Wait()
	}
}


func partitionDBHParallel(P *parameters, K uint8, partitions []byte, batch []*common.Envelope, localReadDegreeTable map[string]uint32, localWriteDegreeTable map[string]uint32, processor func(P *parameters, K uint8, vList [][]byte, dList []uint32) byte, constructMap bool) {
	P.random = rand.New(rand.NewSource(time.Now().UnixNano()))
	wg := &sync.WaitGroup{}

	for i, v := range batch {
		wg.Add(1)
		go func(i int, v *common.Envelope) {
			defer wg.Done()
			var action, err = utils.GetActionFromEnvelopeMsg(v)

			if err != nil {
				logger.Warning("GetActionFromEnvelopeMsg: " + err.Error())
				partitions[i] = uint8(P.random.Intn(int(K)))
				return
			}
			if len(action.Response.KeyMetadataList) < 2 {
				logger.Errorf("Non-invoke2 result: %d\n", len(action.Response.KeyMetadataList))
				partitions[i] = uint8(P.random.Intn(int(K)))
				return
			}
			vList := make([][]byte, 0)
			dList := make([]uint32, 0)
			for j := 0; j < len(action.Response.KeyMetadataList); j++ {
				vList = append(vList, action.Response.KeyMetadataList[j].Name)
				dList = append(dList, localReadDegreeTable[string(vList[j])] + localWriteDegreeTable[string(vList[j])])
			}
			// v1 := action.Response.KeyMetadataList[0].Name
			// v2 := action.Response.KeyMetadataList[1].Name
			// sv1 := string(v1)
			// sv2 := string(v2)
			p := processor(
				P,
				K,
				vList,
				dList,
			)
			partitions[i] = p
			if !constructMap {
				return
			}
			// if _, ok := P.set[sv1]; !ok {
			// 	P.set[sv1] = map[byte]bool{}
			// }
			// if _, ok := P.set[sv2]; !ok {
			// 	P.set[sv2] = map[byte]bool{}
			// }
			// P.set[sv1][p] = true
			// P.set[sv2][p] = true
			// if v, ok := P.pMap[p]; ok {
			// 	P.pMap[p] = v + 1
			// } else {
			// 	P.pMap[p] = 1
			// }
			// if P.pMap[p] > P.maxSize {
			// 	P.maxSize = P.pMap[p]
			// }
			// P.minSize = P.length
			// for j := byte(0); j < K; j++ {
			// 	if P.pMap[j] < P.minSize {
			// 		P.minSize = P.pMap[j]
			// 	}
			// }
		}(i, v)
		if constructMap {
			wg.Wait()
		}
	}
	if !constructMap {
		wg.Wait()
	}
}
