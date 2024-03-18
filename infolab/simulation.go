package infolab

import (
	"sync"

	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protos/peer"
)

type iEndorser interface {
	SimulateProposal(parameters *ccprovider.TransactionParams, chaincodeInput *peer.ChaincodeID) (ccprovider.ChaincodeDefinition, *peer.Response, []byte, *peer.ChaincodeEvent, error)
}
type iEndorserSupport interface {
	GetTxSimulator(channelID string, transactionID string) (ledger.TxSimulator, error)
	GetHistoryQueryExecutor(channelID string) (ledger.HistoryQueryExecutor, error)
}
type getStateHandler func(namespace string, key string) []byte

var EmbeddedChaincode bool
var localSigningIdentity msp.SigningIdentity
var serializedSigningIdentity []byte
var endorser iEndorser
var endorserSupport iEndorserSupport

// getStateTable 변수는 ((네임스페이스, 트랜잭션), getStateHandler) 쌍의 딕셔너리이다.
var getStateTable = sync.Map{}

// HookGetState 함수는 getStateTable에 등록된 핸들러를 이용해
// 데이터베이스 대신 update batch의 내용을 읽어와 반환한다.
func HookGetState(namespace string, transaction string, key string) []byte {
	var handler getStateHandler

	if value, ok := getStateTable.Load([2]string{namespace, transaction}); ok {
		handler = value.(getStateHandler)
	} else {
		return nil
	}
	return handler(namespace, key)
}
