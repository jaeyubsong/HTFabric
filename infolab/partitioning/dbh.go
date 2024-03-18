package partitioning

import (
	"github.com/hyperledger/fabric/protos/common"
)

// PartitionByDBH 함수는 dbh 방식으로 트랜잭션을 파티셔닝해 반환한다.
func PartitionByDBH(K uint8, partitions []byte, batch []*common.Envelope) {
	partitionDBH(newParameters(batch), K, partitions, batch, byDBH, false)
}

func PartitionByDBHParallel(K uint8, partitions []byte, batch []*common.Envelope, localReadDegreeTable map[string]uint32, localWriteDegreeTable map[string]uint32) {
	partitionDBHParallel(newParameters(batch), K, partitions, batch, localReadDegreeTable, localWriteDegreeTable, byDBH, false)
}

// func byDBH(P *parameters, K uint8, v1, v2 []byte, d1, d2 uint32) byte {
// 	if d1 < d2 {
// 		return getHash(v1, K)
// 	}
// 	return getHash(v2, K)
// }

func byDBH(P *parameters, K uint8, vList [][]byte, dList []uint32) byte {
	minVal := dList[0]
	minIdx := 0
	for i := 0; i < len(dList); i++ {
		if dList[i] < minVal {
			minVal = dList[i]
			minIdx = i
		}
	}
	return getHash(vList[minIdx], K)

	// maxVal := dList[0]
	// maxIdx := 0
	// for i := 0; i < len(dList); i++ {
	// 	if dList[i] > maxVal {
	// 		maxVal = dList[i]
	// 		maxIdx = i
	// 	}
	// }
	// return getHash(vList[maxIdx], K)

	// maxVal := dList[0]
	// maxIdx := 0
	// for i := 0; i < len(dList); i++ {
	// 	if dList[i] > maxVal {
	// 		maxVal = dList[i]
	// 		maxIdx = i
	// 	}
	// }
	// return getHash(vList[len(dList)/2], K)
	// if d1 < d2 {
	// 	return getHash(v1, K)
	// }
	// return getHash(v2, K)
}