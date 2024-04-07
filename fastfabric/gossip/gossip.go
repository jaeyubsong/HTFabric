package gossip

import (
	"sync"

	"github.com/hyperledger/fabric/protos/gossip"
)

func GetQueue(blocknum uint64) chan *gossip.Payload {
	mtx.Lock()
	defer mtx.Unlock()
	if c, ok := queue[blocknum]; ok {
		return c
	} else {
		queue[blocknum] = make(chan *gossip.Payload, 1)
		return queue[blocknum]
	}
}
func RemoveQueue(blockNum uint64) {
	mtx.Lock()
	defer mtx.Unlock()
	close(queue[blockNum])
	delete(queue, blockNum)
}

var mtx = sync.Mutex{}
var queue = make(map[uint64]chan *gossip.Payload)
