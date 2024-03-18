package infolab

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/infolab/partitioning"
	"github.com/hyperledger/fabric/protos/common"
)

const partitions = 3

var chaincodeType = "send2"
var batch = 100

type transaction struct {
	Type    string
	fromKey string
	toKey   string
	amountA uint32
	amountB uint32
}

func New(Type, from, to string, a, b uint32) *transaction {
	return &transaction{Type, from, to, a, b}
}

// Variable holding list of transactions modify here to test various transactions
var transactions = []*transaction{
	New(chaincodeType, "A", "B", 3, 2),
	New(chaincodeType, "A", "X", 3, 10),
	New(chaincodeType, "B", "X", 2, 10),
	New(chaincodeType, "C", "D", 5, 1),
	New(chaincodeType, "X", "Y", 10, 15),
	New(chaincodeType, "C", "Y", 5, 15),
}

func createTestBatch(transactions []*transaction) []*common.Envelope {
	var testBatch = make([]*common.Envelope, len(transactions))
	for i, v := range transactions {
		testBatch[i] = partitioning.GetEnvelope(
			fmt.Sprintf("txid%d", i),
			v.fromKey,
			v.toKey,
			v.amountA,
			v.amountB,
			version.NewHeight(uint64(i/100+1), uint64(i)),
			version.NewHeight(uint64(i/100+1), uint64(i)),
		)
	}
	return testBatch
}

var testBatch = createTestBatch(transactions)

func TestGreedy(t *testing.T) {
	var R = make([]byte, len(testBatch))

	partitioning.PartitionByGreedy(partitions, R, testBatch)

	for i, v := range R {
		fmt.Printf("[#%d]: %v\n", i, v)
	}
}
func TestDBH(t *testing.T) {
	var R = make([]byte, len(testBatch))

	partitioning.PartitionByDBH(partitions, R, testBatch)

	for i, v := range R {
		fmt.Printf("[#%d]: %v\n", i, v)
	}
}
func TestHDRF(t *testing.T) {
	var R = make([]byte, len(testBatch))

	partitioning.PartitionByHDRF(partitions, R, testBatch)

	for i, v := range R {
		fmt.Printf("[#%d]: %v\n", i, v)
	}
}
