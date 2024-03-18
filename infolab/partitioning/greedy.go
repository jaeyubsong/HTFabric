package partitioning

import (
	"github.com/hyperledger/fabric/protos/common"
)

// PartitionByGreedy 함수는 greedy 방식으로 트랜잭션을 파티셔닝해 반환한다.
func PartitionByGreedy(K uint8, partitions []byte, batch []*common.Envelope) {
	partition(newParameters(batch), K, partitions, batch, byGreedy, true)
}

func PartitionByGreedyParallel(K uint8, partitions []byte, batch []*common.Envelope, localDegreeTable map[string]uint32) {
	partitionParallel(newParameters(batch), K, partitions, batch, localDegreeTable, byGreedy, true)
}

func byGreedy(P *parameters, K uint8, v1, v2 []byte, d1, d2 uint32) byte {
	var hap = make([]byte, 0, K)
	var gyo = make([]byte, 0, K)
	var S1 = P.set[string(v1)]
	var S2 = P.set[string(v2)]
	var R byte

	for i := range S1 {
		if S2[i] {
			gyo = append(gyo, i)
		} else {
			hap = append(hap, i)
		}
	}
	for i := range S2 {
		hap = append(hap, i)
	}
	if len(gyo) != 0 {
		R = leastLoad(P, gyo)
	}
	if len(gyo) == 0 && len(hap) != 0 {
		R = leastLoad(P, hap)
	}
	if S1 == nil && S2 != nil {
		R = leastLoad(P, getKeys(S2))
	}
	if S1 != nil && S2 == nil {
		R = leastLoad(P, getKeys(S1))
	}
	if S1 == nil && S2 == nil {
		R = leastLoad(P, getAllKeys(K))
	}
	return R
}
func leastLoad(P *parameters, list []byte) byte {
	var R byte = 0
	var min = P.length

	for _, v := range list {
		load := P.pMap[v]
		if min > load {
			min = load
			R = v
		}
	}
	return R
}
func getKeys(m map[byte]bool) []byte {
	var R = make([]byte, len(m))
	var i = 0

	for k := range m {
		R[i] = k
	}
	return R
}
func getAllKeys(K uint8) []byte {
	var R = make([]byte, K)
	var i byte

	for i = 0; i < K; i++ {
		R[i] = i
	}
	return R
}
