package ordering

import (
	"math/rand"
	"time"

	"github.com/hyperledger/fabric/protos/common"
)

// OrderByRandom 함수는 random 방식으로 트랜잭션들을 정렬해 반환한다.
func OrderByRandom(batch []*common.Envelope) []*common.Envelope {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(batch), func(i, j int) {
		batch[i], batch[j] = batch[j], batch[i]
	})
	return batch
}
