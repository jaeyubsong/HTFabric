package deptype

import (
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/protos/peer"
)

type Transaction struct {
	Version         *version.Height
	Payload         *cached.Payload
	TxID            string
	KeyMetadataList []*peer.KeyMetadata
	Partition       uint8
	Dependencies    map[string]*Transaction

	EarlySimulated bool
	RwSet          *cached.TxRwSet
	ValidationCode peer.TxValidationCode
}

func (tx *Transaction) AddDependency(other *Transaction) {
	if other == tx {
		return
	}

	tx.Dependencies[other.TxID] = other
}

func (tx *Transaction) RemoveDependency(other *Transaction) {
	delete(tx.Dependencies, other.TxID)
}

func (t *Transaction) ContainsPvtWrites() bool {
	for _, ns := range t.RwSet.NsRwSets {
		for _, coll := range ns.CollHashedRwSets {
			if coll.PvtRwSetHash != nil {
				return true
			}
		}
	}
	return false
}

// IsHub 함수는 트랜잭션이 허브 키를 포함하는지 여부를 반환한다.
func (t *Transaction) IsHub() bool {
	// for _, metadata := range t.KeyMetadataList {
	// 	if metadata.Hubness == peer.Hubness_HUB {
	// 		return true
	// 	}
	// }
	return false
}

// RetrieveHash returns the hash of the private write-set present
// in the public data for a given namespace-collection
func (t *Transaction) RetrieveHash(ns string, coll string) []byte {
	if t.RwSet == nil {
		return nil
	}
	for _, nsData := range t.RwSet.NsRwSets {
		if nsData.NameSpace != ns {
			continue
		}

		for _, collData := range nsData.CollHashedRwSets {
			if collData.CollectionName == coll {
				return collData.PvtRwSetHash
			}
		}
	}
	return nil
}
