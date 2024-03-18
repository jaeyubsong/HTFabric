package dependency

import (
	"fmt"
	"strings"
	"sync"

	"github.com/hyperledger/fabric/infolab"

	"github.com/hyperledger/fabric/fastfabric/config"
	"github.com/hyperledger/fabric/fastfabric/dependency/deptype"

	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/infolab/simulating"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

var compositeKeySep = string([]byte{0x00})

type analyzer struct {
	block         uint64
	dependencyMap map[string]*skipList
	blockedTxs    map[uint64]*deptype.Transaction
	arrayTxs      []*deptype.Transaction
	committedTxs  chan *deptype.Transaction
	output        chan *deptype.Transaction

	done          chan bool
	committingTxs map[string]bool
	once          sync.Once
	input         chan uint64
}

func (a *analyzer) Stop() {
	a.once.Do(func() {
		close(a.done)
		// close(a.committedTxs)
		// close(a.input)
	})
	// 제발.
	// if !(config.IsStorage || config.IsEndorser) {
	// 	go func() {
	// 		time.Sleep(time.Minute)
	// 		fmt.Printf("애널라이저 %d 해제 (%d)\n", a.block, len(a.output))
	// 		for _, v := range a.dependencyMap {
	// 			v.Clear()
	// 		}
	// 		a.dependencyMap = nil
	// 		a.committingTxs = nil
	// 		a.blockedTxs = nil
	// 	}()
	// }
}

func (a *analyzer) NotifyAboutCommit(tx *deptype.Transaction) {
	a.committedTxs <- tx
}

type Analyzer interface {
	Analyze(block *cached.Block) (<-chan *deptype.Transaction, error)
	NotifyAboutCommit(*deptype.Transaction)
	Stop()
}

func NewAnalyzer(blockID uint64) Analyzer {
	a := &analyzer{
		block: blockID,
		// dependencyMap: make(map[string]*skipList),
		// committedTxs:  make(chan *deptype.Transaction, 200),
		done: make(chan bool),
		// committingTxs: make(map[string]bool),
		blockedTxs: make(map[uint64]*deptype.Transaction),
		input:      make(chan uint64, 1),
	}
	go func() {
		for {
			select {
			case _, more := <-a.done:
				if !more {
					a.dependencyMap = nil
					return
				}
			case <-a.committedTxs:
				// a.removeDependencies(committedTx)
			case id := <-a.input:
				// 제발.
				// please := id >= 0 && !(config.IsEndorser || config.IsStorage)
				please := id >= 0

				if please {
					for _, t := range a.arrayTxs {
						// a.committingTxs[t.TxID] = true
						a.output <- t
					}
					a.blockedTxs = nil
					a.arrayTxs = nil
					a.committedTxs = make(chan *deptype.Transaction, 200)
					close(a.output)
				} else {
					a.dependencyMap = make(map[string]*skipList)
					a.committedTxs = make(chan *deptype.Transaction, 200)
					a.done = make(chan bool)
					a.committingTxs = make(map[string]bool)
					for _, t := range a.blockedTxs {
						a.addDependencies(t)
					}
					a.tryReleaseIdpTxs()
				}
			}
		}
	}()
	return a
}

func (a *analyzer) Analyze(b *cached.Block) (<-chan *deptype.Transaction, error) {
	blockNum := b.Header.Number
	txs := map[uint64]*deptype.Transaction{}
	dones := map[string]bool{}
	keys := map[string]*peer.KeyMetadata{}
	// 제발.
	// if blockNum > 100 && !(config.IsEndorser || config.IsStorage) {
	// 	for i := uint64(0); i < 100; i++ {
	// 		txs[i] = &deptype.Transaction{Version: &version.Height{BlockNum: blockNum, TxNum: i}}
	// 	}
	// } else {
	envs, err := b.UnmarshalAllEnvelopes()
	arrayTxs := make([]*deptype.Transaction, 0, len(envs))
	if err != nil {
		return nil, err
	}
	for txNum, env := range envs {

		pl, err := env.UnmarshalPayload()
		if err != nil {
			return nil, err
		}

		t, err := NewTransaction(blockNum, uint64(txNum), pl, keys)
		if err != nil {
			return nil, err
		}
		txs[t.Version.TxNum] = t
		arrayTxs = append(arrayTxs, t)
		dones[t.TxID] = true
	}
	// }
	if infolab.EarlyResimulationEnabled && (config.IsStorage || config.IsEndorser) {
		var latest = make([]*peer.KeyMetadata, 0, len(keys))
		var victims = make(map[string]func(uint64) bool)

		for _, v := range keys {
			latest = append(latest, v)
		}
		simulating.RememberLock.Lock()
		for k, v := range keys {
			var list []*ccprovider.SimulationParameter

			if chunk, ok := simulating.SimulatedProposals[k]; ok {
				list = chunk
				delete(simulating.SimulatedProposals, k)
			} else {
				continue
			}
			for _, parameter := range list {
				if _, ok := dones[parameter.TxParams.TxID]; ok {
					continue
				}
				for _, w := range parameter.KeyMetadataList {
					if !w.Writing && string(w.Name) == string(v.Name) && w.Version > v.Version {
						victims[parameter.TxParams.TxID] = func(blockID uint64) bool {
							w.Version = blockID
							return simulating.Resimulate(parameter, latest)
						}
						break
					}
				}
			}
		}
		simulating.RememberLock.Unlock()
		go func(blockID uint64) {
			var count = 0

			// JNOTE 임시
			// if blockNum > 10 && !simulating.CatchingUp {
			// 	simulating.CatchingUp = true
			// 	go func() {
			// 		time.Sleep(simulating.CatchUpDuration)
			// 		simulating.CatchingUp = false
			// 	}()
			// 	simulating.GossipServiceInstance.SendToFastPeer("jjoriping", &gossip.GossipMessage{
			// 		Nonce:   0,
			// 		Tag:     gossip.GossipMessage_CHAN_OR_ORG,
			// 		Channel: []byte("jjoriping"),
			// 		Content: &gossip.GossipMessage_CatchUpSignal{
			// 			CatchUpSignal: &gossip.CatchUpSignal{
			// 				Type:    gossip.CatchUpSignal_START,
			// 				BlockId: blockID,
			// 			},
			// 		},
			// 	})
			// }
			if infolab.LatestReceivedBlockID > infolab.MaxOutdatedBlockCount+blockID {
				return
			}
			for _, v := range victims {
				if v(blockNum) {
					count++
				}
			}
			if count > 0 {
				fmt.Printf("[%5d] 조기 재시뮬레이션 %d회 실행함\n", blockNum, count)
			}
		}(blockNum)
	}
	output := make(chan *deptype.Transaction, len(txs))
	a.output = output
	a.blockedTxs = txs
	a.arrayTxs = arrayTxs

	a.input <- blockNum

	return output, nil
}

func (a *analyzer) addDependencies(tx *deptype.Transaction) {
	if tx.RwSet == nil {
		return
	}
	// fmt.Printf("(%5d %5d) t{\n", tx.Version.BlockNum, tx.Version.TxNum)
	for _, set := range tx.RwSet.NsRwSets {
		for _, w := range set.KvRwSet.Writes {
			if strings.HasPrefix(w.Key, "oracle_") {
				continue
			}
			compKey := constructCompositeKey(set.NameSpace, w.Key)
			list := a.getDPListForKey(tx.Version.BlockNum, compKey)
			list.AddWrite(tx)
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			list := a.getDPListForKey(tx.Version.BlockNum, compKey)
			list.AddRead(tx)
		}
	}
	// fmt.Printf("(%5d %5d) t}\n", tx.Version.BlockNum, tx.Version.TxNum)
}

func (a *analyzer) getDPListForKey(blockID uint64, compKey string) *skipList {
	list, ok := a.dependencyMap[compKey]
	if !ok {
		list = NewSkipList(compKey)
		a.dependencyMap[compKey] = list
	}
	return list
}

func (a *analyzer) removeDependencies(tx *deptype.Transaction) {
	if tx.RwSet == nil {
		return
	}
	txsToRelease := map[string]*deptype.Transaction{}
	for _, set := range tx.RwSet.NsRwSets {
		for _, w := range set.KvRwSet.Writes {
			compKey := constructCompositeKey(set.NameSpace, w.Key)
			list := a.getDPListForKey(tx.Version.BlockNum, compKey)
			list.Delete(tx.TxID)
			first := list.First()
			if first != nil {
				for _, el := range first.elements {
					txsToRelease[el.Transaction.TxID] = el.Transaction
				}
			}
		}
		for _, r := range set.KvRwSet.Reads {
			compKey := constructCompositeKey(set.NameSpace, r.Key)
			list := a.getDPListForKey(tx.Version.BlockNum, compKey)
			list.Delete(tx.TxID)
			first := list.First()
			if first != nil {
				for _, el := range first.elements {
					txsToRelease[el.Transaction.TxID] = el.Transaction
				}
			}
		}
		// a.getDPListForKey(constructCompositeKey(set.NameSpace, "1")).debug()
		// if combo > 100 && len(txsToRelease) == 0 {
		// 	fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		// 	tx.debug()
		// 	fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
		// 	combo = 0
		// 	for _, w := range set.KvRwSet.Writes {
		// 		compKey := constructCompositeKey(set.NameSpace, w.Key)
		// 		list := a.getDPListForKey(compKey)
		// 		list.debug()
		// 	}
		// } else {
		// 	combo++
		// }
		for _, tx := range txsToRelease {
			a.tryRelease(tx)
		}
	}
}

func (a *analyzer) tryReleaseIdpTxs() {
	for _, tx := range a.blockedTxs {
		a.tryRelease(tx)
	}
}

func (a *analyzer) tryRelease(tx *deptype.Transaction) {
	if len(tx.Dependencies) != 0 {
		return
	}
	if _, ok := a.committingTxs[tx.TxID]; ok {
		return
	}
	a.committingTxs[tx.TxID] = true
	output := a.output
	output <- tx
	delete(a.blockedTxs, tx.Version.TxNum)
	if len(a.blockedTxs) == 0 {
		close(output)
	}
}

func NewTransaction(blockNum uint64, txNum uint64, payload *cached.Payload, keys map[string]*peer.KeyMetadata) (*deptype.Transaction, error) {
	var earlySimulated bool
	var rwset *cached.TxRwSet
	var keyMetadataList []*peer.KeyMetadata

	chdr, err := payload.Header.UnmarshalChannelHeader()
	if err != nil {
		return nil, err
	}

	tx, err := payload.UnmarshalTransaction()
	if err != nil {
		return nil, err
	}
	acPl, err := tx.Actions[0].UnmarshalChaincodeActionPayload()
	if err != nil {
		return nil, err
	}

	txType := common.HeaderType(chdr.Type)
	if txType != common.HeaderType_ENDORSER_TRANSACTION {
		return &deptype.Transaction{Version: &version.Height{BlockNum: blockNum, TxNum: txNum},
			TxID:            chdr.TxId,
			Payload:         payload,
			RwSet:           nil,
			KeyMetadataList: nil,
			Dependencies:    map[string]*deptype.Transaction{},
		}, nil
	}

	respPl, err := acPl.Action.UnmarshalProposalResponsePayload()
	if err != nil {
		return nil, err
	}
	ccAc, err := respPl.UnmarshalChaincodeAction()
	if err != nil {
		return nil, err
	}
	if config.IsStorage || config.IsEndorser {
		// 완료된 JTODO (200917) endorsement peer가 여기서 자신이 발급한 트랜잭션 목록으로부터 outdated된 것들을 다시 시뮬레이션해 fast peer로 보내야 한다. (gossip network를 사용함을 전제)
		for _, v := range ccAc.Response.KeyMetadataList {
			keys[string(v.Name)] = v
		}
	} else if infolab.EarlyResimulationEnabled {
		// 완료된 JTODO (200917) fast peer가 endorsement peer로부터 대체 트랜잭션을 받았다면 여기서 그 트랜잭션의 rwset을 따라 validate해야 한다.
		simulating.RememberLock.Lock()
		if result, ok := simulating.EarlyResimulationResults[chdr.TxId]; ok {
			// fmt.Printf("조기 재시뮬레이션 결과 존재: %s\n", chdr.TxId)
			earlySimulated = true
			rwset, err = cached.TxRwSetFromProtoMsg(result.RwSet)
			keyMetadataList = result.KeyMetadataList
			delete(simulating.EarlyResimulationResults, chdr.TxId)
		} else {
			rwset, err = ccAc.UnmarshalRwSet()
			keyMetadataList = ccAc.Response.KeyMetadataList
			// for _, n := range rwset.NsRwSets {
			// 	if n.NameSpace != "InfoCC" {
			// 		continue
			// 	}
			// 	for _, v := range n.KvRwSet.Reads {
			// 		fmt.Println("실험: "+v.Key, v.Version)
			// 	}
			// }
		}
		simulating.RememberLock.Unlock()
	} else {
		rwset, err = ccAc.UnmarshalRwSet()
		keyMetadataList = ccAc.Response.KeyMetadataList
	}
	if err != nil {
		return nil, err
	}
	return &deptype.Transaction{Version: &version.Height{BlockNum: blockNum, TxNum: txNum},
		TxID:            chdr.TxId,
		Payload:         payload,
		EarlySimulated:  earlySimulated,
		RwSet:           rwset,
		KeyMetadataList: keyMetadataList,
		Dependencies:    map[string]*deptype.Transaction{}}, nil

}

func constructCompositeKey(ns string, key string) string {
	var buffer strings.Builder
	buffer.WriteString(ns)
	buffer.WriteString(compositeKeySep)
	buffer.WriteString(key)
	return buffer.String()
}
