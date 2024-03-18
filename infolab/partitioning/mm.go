package partitioning

import (
	"math"

	"github.com/hyperledger/fabric/protos/common"
)

// PartitionByMM 함수는 min-max 방식으로 트랜잭션을 파티셔닝해 반환한다.
func PartitionByMM(K uint8, partitions []byte, batch []*common.Envelope) {
	var P = newParameters(batch)

	P.p2n = map[byte]map[string]bool{}
	for i := byte(0); i <= K; i++ {
		P.p2n[i] = map[string]bool{}
	}
	partition(P, K, partitions, batch, byMM, true)
}

func PartitionByMMParallel(K uint8, partitions []byte, batch []*common.Envelope, localDegreeTable map[string]uint32) {
	var P = newParameters(batch)

	P.p2n = map[byte]map[string]bool{}
	for i := byte(0); i <= K; i++ {
		P.p2n[i] = map[string]bool{}
	}
	partitionParallel(P, K, partitions, batch, localDegreeTable, byMM, true)
}

func byMM(P *parameters, K uint8, v1, v2 []byte, d1, d2 uint32) byte {
	var saved int
	var sv2 = string(v2)
	var p byte

	saved = -1
	for i := byte(1); i <= K; i++ {
		s := math.Max(1, beta*float64(i/byte(K)))
		if float64(P.pMap[i]-P.minSize) < s {
			sum := 1
			if _, ok := P.p2n[i][sv2]; !ok {
				sum--
			}
			if sum > saved {
				saved = sum
				p = i
			}
		}
	}
	P.p2n[p][sv2] = true
	return p
}
