package ordering

import (
	"fmt"

	"github.com/hyperledger/fabric/protos/common"
)

type hubIndexCell struct {
	name  string
	index int
}

// OrderByHubEnding 함수는 hub-ending 방식으로 트랜잭션들을 정렬해 반환한다.
func OrderByHubEnding(batch []*common.Envelope) []*common.Envelope {
	var R = make([]*common.Envelope, len(batch))
	var hubIndexMap = map[string]*hubIndexCell{}
	var hubDependency = map[string]map[string]*hubIndexCell{}

	for index, envelope := range batch {
		var nonhubs, hubs = getKeyGroups(envelope)

		// fmt.Printf("#%d: N(%+v) H(%+v) B(%d)\n", index, nonhubs, hubs, len(batch))
		if nonhubs == nil && hubs == nil {
			continue
		}
		if len(hubs) > 0 {
			/* [허브 트랜잭션인 경우] (예: A X)
			* hubDependency[A][X]가 존재한다면 넘어간다.
			* hubDependency[A][X]가 존재하지 않는다면 그 값을 index로 설정한다.
			 */
			for _, nonhub := range nonhubs {
				var table map[string]*hubIndexCell
				var ok bool

				if table, ok = hubDependency[nonhub]; !ok {
					table = map[string]*hubIndexCell{}
				}
				for _, hub := range hubs {
					if _, ok = hubDependency[nonhub][hub]; ok {
						continue
					}
					var cell = &hubIndexCell{
						name:  hub,
						index: index,
					}
					hubIndexMap[hub] = cell
					table[hub] = cell
				}
				hubDependency[nonhub] = table
			}
			R[index] = envelope
		} else {
			/* [논허브 트랜잭션인 경우] (예: A B)
			* hubDependency[A 또는 B]가 존재하지 않는다면 넘어간다.
			* hubDependency[A 또는 B]가 존재한다면 그 최솟값 인덱스 위치에 꽂아넣는다.
			 */
			var minHubIndex = index

			for _, nonhub := range nonhubs {
				if table, ok := hubDependency[nonhub]; ok {
					for _, cell := range table {
						if minHubIndex > cell.index {
							minHubIndex = cell.index
						}
					}
				}
			}
			if minHubIndex < index {
				for _, cell := range hubIndexMap {
					if cell.index < minHubIndex {
						continue
					}
					cell.index++
				}
				for i := index; i > minHubIndex; i-- {
					R[i] = R[i-1]
				}
			}
			R[minHubIndex] = envelope
		}
	}
	fmt.Printf("batch의 길이: %d\nR의 길이: %d\n", len(batch), len(R))
	return R
}
