package partitioning

import (
	"math"

	"github.com/hyperledger/fabric/protos/common"
)

// PartitionByMMN2P 함수는 min-max N2P 방식으로 트랜잭션을 파티셔닝해 반환한다.
func PartitionByMMN2P(K uint8, partitions []byte, batch []*common.Envelope) {
	var P = newParameters(batch)

	P.n2p = map[string][]byte{}
	P.save = make([]int, K+1)
	P.mark = make([]string, K)
	P.pids = make([]byte, K+1)
	P.indx = make([]int, K)
	for i := range P.mark {
		P.mark[i] = "-1"
	}
	partition(P, K, partitions, batch, byMMN2P, true)
}

func PartitionByMMN2PParallel(K uint8, partitions []byte, batch []*common.Envelope, localDegreeTable map[string]uint32) {
	var P = newParameters(batch)

	P.n2p = map[string][]byte{}
	P.save = make([]int, K+1)
	P.mark = make([]string, K)
	P.pids = make([]byte, K+1)
	P.indx = make([]int, K)
	for i := range P.mark {
		P.mark[i] = "-1"
	}
	partitionParallel(P, K, partitions, batch, localDegreeTable, byMMN2P, true)
}

func byMMN2P(P *parameters, K uint8, v1, v2 []byte, d1, d2 uint32) byte {
	var active = 0
	var saved int
	var sv1 = string(v1)
	var sv2 = string(v2)
	var p byte

	if _, ok := P.n2p[sv2]; !ok {
		P.n2p[sv2] = []byte{}
	}
	for _, i := range P.n2p[sv2] {
		if P.mark[i] != sv1 {
			P.mark[i] = sv1
			active++
			P.pids[active] = i
			P.save[active] = 1
			P.indx[i] = active
		} else {
			P.save[P.indx[i]]++
		}
	}
	saved = -1
	for j := 1; j <= active; j++ {
		i := P.pids[j]
		s := math.Max(1, beta*float64(i/byte(K)))
		if float64(P.pMap[i]-P.minSize) < s {
			if P.save[j] > saved {
				saved = P.save[j]
				p = i
			}
		}
	}
	P.n2p[sv2] = append(P.n2p[sv2], p)
	return p
}
