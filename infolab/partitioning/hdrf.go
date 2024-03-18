package partitioning

import (
	"github.com/hyperledger/fabric/protos/common"
)

const lambda float64 = 1
const epsilon float64 = 0.01

// PartitionByHDRF 함수는 hdrf 방식으로 트랜잭션을 파티셔닝해 반환한다.
func PartitionByHDRF(K uint8, partitions []byte, batch []*common.Envelope) {
	partition(newParameters(batch), K, partitions, batch, byHDRF, true)
}

func PartitionByHDRFParallel(K uint8, partitions []byte, batch []*common.Envelope, localDegreeTable map[string]uint32) {
	partitionParallel(newParameters(batch), K, partitions, batch, localDegreeTable, byHDRF, true)
}

func byHDRF(P *parameters, K uint8, v1, v2 []byte, d1, d2 uint32) byte {
	var R byte
	var max float64 = 0

	t1, t2 := normalize(d1, d2)
	for i := byte(0); i < K; i++ {
		v := c(P, v1, v2, t1, t2, i)
		if v > max {
			max = v
			R = i
		}
	}
	return R
}
func c(P *parameters, v1, v2 []byte, t1, t2 float64, i byte) float64 {
	return cRep(P.set, v1, v2, t1, t2, i) + cBal(P, i)
}
func cRep(S partitionSet, v1, v2 []byte, t1, t2 float64, i byte) float64 {
	return g(S, v1, t1, i) + g(S, v2, t2, i)
}
func cBal(P *parameters, i byte) float64 {
	return lambda * float64(P.maxSize-P.pMap[i]) / (epsilon + float64(P.maxSize-P.minSize))
}
func g(S partitionSet, v []byte, t float64, i byte) float64 {
	if s, ok := S[string(v)]; ok {
		if _, ok := s[i]; ok {
			return 1 + (1 - t)
		}
	}
	return 0
}
func normalize(d1, d2 uint32) (float64, float64) {
	var sum = float64(d1 + d2)

	return float64(d1) / sum, float64(d2) / sum
}
