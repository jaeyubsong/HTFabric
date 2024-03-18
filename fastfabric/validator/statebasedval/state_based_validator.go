/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statebasedval

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"go/build"
	"log"
	"os"

	"io/ioutil"
	"strconv"
	"strings"
	"sync"

	// "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/infolab/simulating"

	"github.com/go-python/gpython/repl"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/fastfabric/cached"
	"github.com/hyperledger/fabric/fastfabric/config"
	"github.com/hyperledger/fabric/fastfabric/dependency/deptype"
	"github.com/hyperledger/fabric/fastfabric/validator/internalVal"
	"github.com/hyperledger/fabric/infolab"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"

	// import required modules
	_ "github.com/go-python/gpython/builtin"
	_ "github.com/go-python/gpython/math"
	_ "github.com/go-python/gpython/sys"
	_ "github.com/go-python/gpython/time"
)

var logger = flogging.MustGetLogger("statebasedval")

// Validator validates a tx against the latest committed state
// and preceding valid transactions with in the same block
type Validator struct {
	db                  privacyenabledstate.DB
	validatedKeys       chan map[string]*version.Height
	vmList              []*repl.REPL
	output              func() string
	lock                sync.RWMutex
	partitions          []chan *partitionTask
	partitionTxs        chan *partitionTasks
	AdjMat              [][]uint8
	partitionWaitGroups []sync.WaitGroup
}

type partitionTask struct {
	f  func(partition uint8, tx *deptype.Transaction) error
	tx *deptype.Transaction
}

type partitionTasks struct {
	f         func(partition uint8, tx *deptype.Transaction) error
	txs       []*deptype.Transaction
	partition uint8
	wg        *sync.WaitGroup
}

const partitionDAGEnabled = false

// var first = true

var codePath = "../chaincode/contract.py"

var startTime time.Time
var lastTime time.Duration
var receivePeriod time.Duration
var receiveCount int64
var periodCount int64
var receivePeriodAverage int64

var ValidAndPrepareBatchCallTime uint64

var txCount_total uint64
var validCount_total uint64
var resimCount_total uint64

var txCount uint64
var validCount uint64
var resimCount uint64

var tpsStartTime time.Time
var tpsTxCount uint64
var tpsValidCount uint64
var tpsResimCount uint64

var Log *log.Logger

var intraBlockDep int32
var interBlockDep int32



// NewValidator constructs StateValidator
func NewValidator(db privacyenabledstate.DB) *Validator {
	fmt.Printf("New Validator called\n")
	receiveCount = 0
	startTime = time.Now()
	lastTime = time.Since(startTime)
	ValidAndPrepareBatchCallTime = 0
	txCount_total = 0
	tpsTxCount = 0
	tpsValidCount = 0
	tpsResimCount = 0
	validCount_total = 0
	resimCount_total = 0

	ui := &mockUI{}

	AdjMatInit := make([][]uint8, infolab.Partitions)
	for m := range AdjMatInit {
		AdjMatInit[m] = make([]uint8, infolab.Partitions)
	}

	validator := &Validator{
		db:            db,
		validatedKeys: make(chan map[string]*version.Height, 1),
		// vm:                  repl.New(),
		output:              func() string { return ui.output },
		partitions:          make([]chan *partitionTask, infolab.Partitions),
		partitionTxs:        make(chan *partitionTasks, infolab.Partitions),
		AdjMat:              AdjMatInit,
		partitionWaitGroups: make([]sync.WaitGroup, infolab.Partitions),
	}

	validator.vmList = make([]*repl.REPL, 0)
	for i := 0; i < int(infolab.Partitions); i++ {
		validator.vmList = append(validator.vmList, repl.New())
	}

	// Parallel Validation : Wait until all predecessors finish validation
	fmt.Printf("Before parallel validation for loop\n")

	var logpath = build.Default.GOPATH + "/../state_based_validator_log.log"
	if _, err := os.Stat(logpath); err == nil {
		e := os.Remove(logpath)
		if e != nil {
			panic(e)
		}
		fmt.Printf("log file found, removed %v\n", logpath)
	} else {
		fmt.Printf("log file not found\n")
	}
	var file, err1 = os.Create(logpath)

	if err1 != nil {
		panic(err1)
	}
	Log = log.New(file, "", 0)
	Log.Printf("Power fast peer\n")

	jobs := make(chan *partitionTasks, int(infolab.Partitions))
	for w := 0; w < int(infolab.Slots); w++ {

		go func(jobs <-chan *partitionTasks) {
			for task := range jobs {
				for i := range task.txs {
					tx := task.txs[i]
					if err := task.f(task.partition, tx); err != nil {
						panic(err)
					}
				}
				task.wg.Done()
			}
		}(jobs)

	}
	validator.partitionTxs = jobs

	// for i := range validator.partitions {
	// 	fmt.Printf("(i: %d) Inside for loop\n", i);
	// 	var taskChan = make(chan *partitionTask, 100000)
	// 	// fmt.Printf("(i: %d) After creatking taskChan, before go function\n", i);

	// 	go func(partition uint8) {
	// 		// if partitionDAGEnabled {
	// 		// 	validator.partitionWaitGroups[partition].Wait()
	// 		// }
	// 		fmt.Printf("(i: %d)[GO] Right after creating function\n", i);

	// 		for task := range taskChan {
	// 			if err := task.f(partition, task.tx); err != nil {
	// 				panic(err)
	// 			}
	// 			// fmt.Printf("(i: %d) [GO] Processing function\n", i);
	// 		}
	// 		// fmt.Printf("(i: %d) [GO] Befpre ending function", i);

	// 		// if partitionDAGEnabled {
	// 		// 	for i := 0; uint8(i) < infolab.Partitions; i++ {
	// 		// 		if validator.AdjMat[i][partition] == 1 {
	// 		// 			validator.partitionWaitGroups[i].Done()
	// 		// 		}
	// 		// 	}
	// 		// }
	// 	}(uint8(i))
	// 	// fmt.Printf("(i: %d) After go function\n", i);
	// 	validator.partitions[i] = taskChan

	// }
	fmt.Printf("(240122 v1) Catchup\n")
	validator.validatedKeys <- make(map[string]*version.Height)

	for i := 0; i < int(infolab.Partitions); i++ {
		validator.vmList[i].SetUI(ui)
		// validator.vm.SetUI(ui)
		// data, err := ioutil.ReadFile(codePath)
		data, err := ioutil.ReadFile("../chaincode/contract_" + strconv.Itoa(i) + ".py")
		if err != nil {
			logger.Fatal(err)
			fmt.Printf("Error detected\n")
		}
		code := string(data)
		validator.vmList[i].Run(code)
	}

	return validator
}

// preLoadCommittedVersionOfRSet loads committed version of all keys in each
// transaction's read set into a cache.
func (v *Validator) preLoadCommittedVersionOfRSet(block *internalVal.Block) error {
	// Collect both public and hashed keys in read sets of all transactions in a given block
	var pubKeys []*statedb.CompositeKey
	var hashedKeys []*privacyenabledstate.HashedCompositeKey

	// pubKeysMap and hashedKeysMap are used to avoid duplicate entries in the
	// pubKeys and hashedKeys. Though map alone can be used to collect keys in
	// read sets and pass as an argument in LoadCommittedVersionOfPubAndHashedKeys(),
	// array is used for better code readability. On the negative side, this approach
	// might use some extra memory.
	pubKeysMap := make(map[statedb.CompositeKey]interface{})
	hashedKeysMap := make(map[privacyenabledstate.HashedCompositeKey]interface{})

	txs := block.Txs
	processedTxs := make(chan *deptype.Transaction, cap(txs))
	block.Txs = processedTxs
	for tx := range txs {
		for _, nsRWSet := range tx.RwSet.NsRwSets {
			for _, kvRead := range nsRWSet.KvRwSet.Reads {
				compositeKey := statedb.CompositeKey{
					Namespace: nsRWSet.NameSpace,
					Key:       kvRead.Key,
				}
				if _, ok := pubKeysMap[compositeKey]; !ok {
					pubKeysMap[compositeKey] = nil
					pubKeys = append(pubKeys, &compositeKey)
				}

			}
			for _, colHashedRwSet := range nsRWSet.CollHashedRwSets {
				for _, kvHashedRead := range colHashedRwSet.HashedRwSet.HashedReads {
					hashedCompositeKey := privacyenabledstate.HashedCompositeKey{
						Namespace:      nsRWSet.NameSpace,
						CollectionName: colHashedRwSet.CollectionName,
						KeyHash:        string(kvHashedRead.KeyHash),
					}
					if _, ok := hashedKeysMap[hashedCompositeKey]; !ok {
						hashedKeysMap[hashedCompositeKey] = nil
						hashedKeys = append(hashedKeys, &hashedCompositeKey)
					}
				}
			}
		}
		processedTxs <- tx
	}

	// Load committed version of all keys into a cache
	if len(pubKeys) > 0 || len(hashedKeys) > 0 {
		err := v.db.LoadCommittedVersionsOfPubAndHashedKeys(pubKeys, hashedKeys)
		if err != nil {
			return err
		}
	}

	return nil
}

var functionDurationAvg int64
var functionStartTime time.Time

var val_tx_time_tot int64
var val_tx_time_cnt int64

var write_tx_time_tot int64
var write_tx_time_cnt int64

var resim_tx_time_tot int64
var resim_tx_time_cnt int64

var end_to_end_tot int64

var prepare_rwset_time_tot int64
var prepare_rwset_time_cnt int64

var validateLatencyTotal int64
var validateLatencyCount int64

var isNewBlock bool

// ValidateAndPrepareBatch implements method in Validator interface
func (v *Validator) ValidateAndPrepareBatch(block *internalVal.Block, doMVCCValidation bool, committedTxs chan<- *deptype.Transaction) (*internalVal.PubAndHashUpdates, []uint64, []uint64, error) {
	isNewBlock = true
	fmt.Printf("Start ValidateANdPrepareBatch\n")
	validateLatencyStart := time.Now()

	txCount = 0
	validCount = 0
	resimCount = 0

	tpsMeasureStart := 301
	tpsMeasureEnd := 1000
	end_to_end_start := time.Now()
	if infolab.DebugMode && infolab.MeasureValidateAndPrepareBatchTime {
		functionStartTime = time.Now()
	}
	if infolab.DebugMode && infolab.MeasureTps {
		if block.Num == uint64(tpsMeasureStart) {
			fmt.Printf("Time starts now\n")
			tpsStartTime = time.Now()
		}
	}

	if infolab.DebugMode && infolab.ValidateAndPrepareBatchCallTime {
		atomic.AddUint64(&ValidAndPrepareBatchCallTime, 1)
	}
	if infolab.DebugMode && infolab.ValidateAndPrepare {
		_, file, no, ok := runtime.Caller(1)
		if ok {
			fmt.Printf("(fastfabric/validator/statebasedval/state_based_validator.go) Block: %d ValidateAndPrepareBatch from %s#%d\n", block.Num, file, no)
		}
	}
	if infolab.DebugMode && infolab.ValidCheckTps {
		receiveCount++
		receivePeriod = time.Since(startTime) - lastTime
		lastTime = time.Since(startTime)
		if receiveCount >= 301 {
			receivePeriodAverage += receivePeriod.Nanoseconds()
			fmt.Printf("Before ValidateAndPrepareBatch[%d] period is %d\n", receiveCount, receivePeriod.Nanoseconds())
		}
		if receiveCount == 1000 {
			receivePeriodAverage = receivePeriodAverage / 3000
			fmt.Printf("**Before ValidateAndPrepareBatch receive average is %d\n", receivePeriodAverage)
		}
	}
	// fmt.Printf("Start ValidateAndPrepareBatch\n")
	defer close(committedTxs)
	// Check whether statedb implements BulkOptimizable interface. For now,
	// only CouchDB implements BulkOptimizable to reduce the number of REST
	// API calls from peer to CouchDB instance.
	if v.db.IsBulkOptimizable() {
		err := v.preLoadCommittedVersionOfRSet(block)
		if err != nil {
			return nil, []uint64{}, []uint64{}, err
		}
	}

	updates := internalVal.NewPubAndHashUpdates()
	// fmt.Printf("Updates before starting: %p\n", updates)
	reexecutedTxIndices := make([]uint64, 0, 100)
	badTxIndices := make([]uint64, 0, 100)
	txs := block.Txs
	processedTxs := make(chan *deptype.Transaction, cap(txs))
	defer close(processedTxs)
	block.Txs = processedTxs

	// fmt.Printf("Partitions: %d\n", infolab.Partitions)
	if infolab.NoResim {
		fmt.Printf("*********************************************\n")
		fmt.Printf("No Resim\n")

		wg := &sync.WaitGroup{}
		resimWg := &sync.WaitGroup{}
		validationPartitions := make([]chan *deptype.Transaction, infolab.Partitions)
		for i, _ := range validationPartitions {
			validationPartitions[i] = make(chan *deptype.Transaction, 10000)
		}
		resimulationPartitions := make([]chan *deptype.Transaction, infolab.Partitions)
		for i, _ := range resimulationPartitions {
			resimulationPartitions[i] = make(chan *deptype.Transaction, 10000)
		}

		validatePartition := func(partition uint8, tx *deptype.Transaction, updates *internalVal.PubAndHashUpdates) error {
			atomic.AddUint64(&txCount, 1)
			// fmt.Printf("<%d> (%5d, %5d)\n", partition, block.Num, tx.Version.TxNum)
			var validationCode peer.TxValidationCode
			var err error
			val_start_time := time.Now()
			v.lock.RLock()
			if validationCode, err = v.validateEndorserTX(tx, doMVCCValidation, updates); err != nil {
				panic(err)
			}
			val_tx_time_tot += time.Since(val_start_time).Nanoseconds()
			val_tx_time_cnt++
			tx.ValidationCode = validationCode
			for validationCode != peer.TxValidationCode_VALID {
				v.lock.RUnlock()
				resimulationPartitions[partition] <- tx
				return nil
			}
			// reexecuted = validationCode == peer.TxValidationCode_RESIMULATED
			write_start_time := time.Now()
			if validationCode == peer.TxValidationCode_VALID {
				logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator (%s)", block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
				committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
				v.lock.RUnlock()
				v.lock.Lock()
				updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
				v.lock.Unlock()
			} else {
				v.lock.RUnlock()
				logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
					block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
				if validationCode == peer.TxValidationCode_BAD_RWSET || validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT {
					badTxIndices = append(badTxIndices, tx.Version.TxNum)
				}
			}
			write_tx_time_tot += time.Since(write_start_time).Nanoseconds()
			write_tx_time_cnt++
			processedTxs <- tx
			committedTxs <- tx
			// wg.Done()
			return nil
		}

		resimulatePartition := func(partition uint8, tx *deptype.Transaction, updates *internalVal.PubAndHashUpdates) error {
			reexecuted := false
			validationCode := peer.TxValidationCode_MVCC_READ_CONFLICT

			if validationCode == peer.TxValidationCode_VALID || reexecuted {
				logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator (%s)", block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
				committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
				v.lock.Lock()
				updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
				v.lock.Unlock()
				if reexecuted {
					reexecutedTxIndices = append(reexecutedTxIndices, tx.Version.TxNum)
				}
			} else {
				logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
					block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
				if validationCode == peer.TxValidationCode_BAD_RWSET || validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT {
					badTxIndices = append(badTxIndices, tx.Version.TxNum)
				}
			}
			processedTxs <- tx
			committedTxs <- tx
			return nil
		}

		go func() {
			for i, partition := range validationPartitions {
				go func(i int, partition chan *deptype.Transaction) {
					
					for tx := range partition {
						validatePartition(uint8(i), tx, updates)
					}
					// fmt.Printf("wg done\n")
					// fmt.Printf("Closing resim[%d]\n", i)
					// fmt.Printf("Updates in validate: %p\n", updates)
					close(resimulationPartitions[i])
					// fmt.Printf("Closing resim[%d] done\n", i)
					wg.Done()
				}(i, partition)
			}
			// fmt.Printf("Waiting all validation\n")
			wg.Wait()
			if infolab.FastWb == true && !(config.IsEndorser || config.IsStorage) {
				fmt.Printf("[state_based_val] No-resim fastWb\n")
				simulating.SendFastWb(block.Num, updates)
			}

			fmt.Printf("[noresim] Finished Validation, intra/inter: %d, %d\n", intraBlockDep, interBlockDep)
			// fmt.Printf("Start re-simulation\n")

			for i, partition := range resimulationPartitions {
				go func(i int, partition chan *deptype.Transaction) {
					for tx := range partition {
						resimulatePartition(uint8(i), tx, updates)
					}
					// fmt.Printf("Updates in re-simulate: %p\n", updates)
					// fmt.Printf("resimWg done\n")
					resimWg.Done()
				}(i, partition)
			}
		}()







		// fmt.Printf("Generating  validation Partition channel\n")

		// fmt.Printf("Putting transactions to right partition\n")
		wg.Add(int(infolab.Partitions))
		resimWg.Add(int(infolab.Partitions))
		for tx := range txs {
			validationPartitions[tx.Partition] <- tx
		}

		for _, c := range validationPartitions {
			close(c)
		}
		resimWg.Wait()
		fmt.Printf("Finished re-simulation, intra/inter: %d, %d\n", intraBlockDep, interBlockDep)
		fmt.Printf("*********************************************\n")

	} else if infolab.BatchValid {
		if infolab.Partitions > 100 {
			fmt.Printf("Batch valid = true, partition == 1\n")
			var invalidTxs []*deptype.Transaction
			for tx := range txs {
				v.lock.RLock()
				atomic.AddUint64(&txCount, 1)
				val_start_time := time.Now()
				// Log.Printf("txId: %s\n", tx.TxID)
				// fmt.Printf("Inside partitions<=1\n")
				atomic.AddUint64(&txCount_total, 1)

				if block.Num >= uint64(tpsMeasureStart) {
					atomic.AddUint64(&tpsTxCount, 1)
				}

				var validationCode peer.TxValidationCode
				var err error
				if validationCode, err = v.validateEndorserTX(tx, doMVCCValidation, updates); err != nil {
					panic(err)
				}
				tx.ValidationCode = validationCode
				if validationCode == peer.TxValidationCode_IS_HUB {
					invalidTxs = append(invalidTxs, tx)
					val_tx_time_tot += time.Since(val_start_time).Nanoseconds()
					val_tx_time_cnt++
					v.lock.RUnlock()
					continue
					// validationCode = v.reExecute(tx, updates)
					// tx.ValidationCode = validationCode
				} else {
					val_tx_time_tot += time.Since(val_start_time).Nanoseconds()
					val_tx_time_cnt++
				}
				write_start_time := time.Now()
				if validationCode == peer.TxValidationCode_VALID {
					logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator (%s)", block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
					// committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
					v.lock.RUnlock()
					// v.lock.Lock()
					// updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
					// v.lock.Unlock()

					atomic.AddUint64(&validCount, 1)
					atomic.AddUint64(&validCount_total, 1)
					if block.Num >= uint64(tpsMeasureStart) {
						atomic.AddUint64(&tpsValidCount, 1)
					}

				} else {
					logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
						block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
					if validationCode == peer.TxValidationCode_BAD_RWSET || validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT {
						badTxIndices = append(badTxIndices, tx.Version.TxNum)
					}
				}
				processedTxs <- tx
				committedTxs <- tx
				write_tx_time_tot += time.Since(write_start_time).Nanoseconds()
				write_tx_time_cnt++
			}

			if infolab.FastWb == true && !(config.IsEndorser || config.IsStorage){
				fmt.Printf("[state_based_val] fastWb\n")
				simulating.SendFastWb(block.Num, updates)
			}

			fmt.Printf("Before trying to re-simulate len %d\n", len(invalidTxs))
			fmt.Printf("Finished Validation, intra/inter: %d, %d\n", intraBlockDep, interBlockDep)
			for _, tx := range invalidTxs {
				resim_start_time := time.Now()
				// fmt.Printf("Length of invalidTxs is %d\n", len(invalidTxs))
				var validationCode peer.TxValidationCode
				var reexecuted bool
				// fmt.Printf("Before re-simulate\n")
				validationCode = v.reExecute(tx, updates, 0)
				// fmt.Printf("After re-simulate\n")
				tx.ValidationCode = validationCode

				resim_tx_time_tot += time.Since(resim_start_time).Nanoseconds()
				resim_tx_time_cnt++

				write_start_time := time.Now()
				reexecuted = validationCode == peer.TxValidationCode_RESIMULATED
				committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
				// fmt.Printf("Before Apply Write set\n")
				v.lock.RUnlock()
				v.lock.Lock()
				updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
				v.lock.Unlock()
				// fmt.Printf("After Apply Write set\n")
				// if reexecuted {
				reexecutedTxIndices = append(reexecutedTxIndices, tx.Version.TxNum)
				// }

				atomic.AddUint64(&validCount, 1)
				atomic.AddUint64(&validCount_total, 1)
				if block.Num >= uint64(tpsMeasureStart) {
					atomic.AddUint64(&tpsValidCount, 1)
				}

				if infolab.DebugMode && infolab.ValidateAndPrepareBatchCallTime {
					if reexecuted {
						atomic.AddUint64(&resimCount, 1)
						atomic.AddUint64(&resimCount_total, 1)
						if block.Num >= uint64(tpsMeasureStart) {
							atomic.AddUint64(&tpsResimCount, 1)
						}

					} else {
						// atomic.AddUint64(&validCount, 1)
						// atomic.AddUint64(&validCount_total, 1)
						// if block.Num >= uint64(tpsMeasureStart) {
						// 	atomic.AddUint64(&tpsValidCount, 1)
						// }
					}
				}


				processedTxs <- tx
				committedTxs <- tx
				write_tx_time_tot += time.Since(write_start_time).Nanoseconds()
				write_tx_time_cnt++
			}
			fmt.Printf("Finished Re-simulation, intra/inter: %d, %d\n", intraBlockDep, interBlockDep)
		} else {

			if infolab.DeterministicResim {
				fmt.Printf("-----------Deterministic-Resim---------------\n")
				// Serial Validation
				invalidTxs := make([][]*deptype.Transaction, int(infolab.Partitions))
				for tx := range txs {
					v.lock.RLock()
					atomic.AddUint64(&txCount, 1)
					atomic.AddUint64(&txCount_total, 1)
					val_start_time := time.Now()
					var validationCode peer.TxValidationCode
					var err error
					if validationCode, err = v.validateEndorserTX(tx, doMVCCValidation, updates); err != nil {
						panic(err)
					}
					tx.ValidationCode = validationCode
					if validationCode == peer.TxValidationCode_IS_HUB {
						invalidTxs[tx.Partition] = append(invalidTxs[tx.Partition], tx)
						val_tx_time_tot += time.Since(val_start_time).Nanoseconds()
						val_tx_time_cnt++
						v.lock.RUnlock()
						continue
						// validationCode = v.reExecute(tx, updates)
						// tx.ValidationCode = validationCode
					} else {
						val_tx_time_tot += time.Since(val_start_time).Nanoseconds()
						val_tx_time_cnt++
					}
					write_start_time := time.Now()
					if validationCode == peer.TxValidationCode_VALID {
						logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator (%s)", block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
						committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
						v.lock.RUnlock()
						v.lock.Lock()
						updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
						v.lock.Unlock()
	
						atomic.AddUint64(&validCount, 1)
						atomic.AddUint64(&validCount_total, 1)
						if block.Num >= uint64(tpsMeasureStart) {
							atomic.AddUint64(&tpsValidCount, 1)
						}
	
					} else {
						logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
							block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
						if validationCode == peer.TxValidationCode_BAD_RWSET || validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT {
							badTxIndices = append(badTxIndices, tx.Version.TxNum)
						}
					}
					processedTxs <- tx
					committedTxs <- tx
					write_tx_time_tot += time.Since(write_start_time).Nanoseconds()
					write_tx_time_cnt++

				}
				if infolab.FastWb == true && !(config.IsEndorser || config.IsStorage){
					fmt.Printf("[state_based_val] fastWb\n")
					simulating.SendFastWb(block.Num, updates)
				}
				// Deterministic Parallel Re-execution
				k := 0
				for i := 0; i < int(infolab.Partitions); i++ {
					if len(invalidTxs[i]) > k {
						k = len(invalidTxs[i])
					}
				}
				fmt.Printf("k: %d, partitions: %d\n", k, infolab.Partitions)

				reExecutedTxs := make([] peer.TxValidationCode, infolab.Partitions)
				for j := 0; j < k; j++ {
					// fmt.Printf("j: %d\n", j)
					wg := &sync.WaitGroup{}
					wg.Add(int(infolab.Partitions))
					for i := 0; i < int(infolab.Partitions); i++ {
						// fmt.Printf("i: %d ", i)
						// fmt.Printf("len(invalidTxs[i]: %d\n", len(invalidTxs[i]))
						if len(invalidTxs[i]) <= j {
							// fmt.Printf("[continue] Len(invalidTxs[i]): %d, i: %d, j: %d\n", len(invalidTxs[i]), i, j)
							reExecutedTxs[i] = peer.TxValidationCode_VALID
							wg.Done()
							continue
						}
						go func(i int, j int) {
							// fmt.Printf("[execute] Len(invalidTxs[i]): %d, i: %d, j: %d\n", len(invalidTxs[i]), i, j)
							reExecutedTxs[i] = v.reExecute(invalidTxs[i][j], updates, uint8(i))
							v.lock.RUnlock()
							wg.Done()

						}(i, j)
					}
					wg.Wait()
					// fmt.Printf("[start] commit tx\n")
					for i := 0; i < int(infolab.Partitions); i++ {
						if reExecutedTxs[i] != peer.TxValidationCode_RESIMULATED {
							// fmt.Printf("Valid tx, continue\n")
							continue
						}
						tx := invalidTxs[i][j]

						// Validate
						
			

						// for {
						// 	// fmt.Printf("Checking re-re-execute\n")
						// 	var validationCode peer.TxValidationCode
						// 	var err error
						// 	v.lock.RLock()
						// 	if validationCode, err = v.revalidateEndorserTX(tx, doMVCCValidation, updates); err != nil {
						// 		panic(err)
						// 	}
						// 	tx.ValidationCode = validationCode
						// 	if validationCode == peer.TxValidationCode_IS_HUB {
						// 		v.lock.RUnlock()
						// 		// fmt.Printf("[Finish] Checking re-re-execute\n")
						// 		break
						// 	} else {
						// 		// fmt.Printf("[Again] Checking re-re-execute\n")
						// 		v.lock.RUnlock()
						// 		reExecutedTxs[i] = v.reExecute(invalidTxs[i][j], updates, uint8(i))
						// 		v.lock.RUnlock()
						// 	}
						// }
						// fmt.Printf("Invalid Valid tx, check\n")
						write_start_time := time.Now()
						// fmt.Printf("Got Tx\n")
						committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
						// fmt.Printf("Before Apply Write set\n")
						v.lock.Lock()
						updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
						v.lock.Unlock()
						reexecutedTxIndices = append(reexecutedTxIndices, tx.Version.TxNum)
						processedTxs <- tx
						committedTxs <- tx
						write_tx_time_tot += time.Since(write_start_time).Nanoseconds()
						write_tx_time_cnt++
					}
					// fmt.Printf("[end] commit tx\n")
				}
				fmt.Printf("Finish processing block\n")

			} else {
				fmt.Printf("*********************************************\n")
				fmt.Printf("parallel validation with batch valid = true\n")
	
				wg := &sync.WaitGroup{}
				resimWg := &sync.WaitGroup{}
				validationPartitions := make([]chan *deptype.Transaction, infolab.Partitions)
				for i, _ := range validationPartitions {
					validationPartitions[i] = make(chan *deptype.Transaction, 10000)
				}
				resimulationPartitions := make([]chan *deptype.Transaction, infolab.Partitions)
				for i, _ := range resimulationPartitions {
					resimulationPartitions[i] = make(chan *deptype.Transaction, 10000)
				}
	
				validatePartition := func(partition uint8, tx *deptype.Transaction, updates *internalVal.PubAndHashUpdates) error {
					atomic.AddUint64(&txCount, 1)
					// fmt.Printf("<%d> (%5d, %5d)\n", partition, block.Num, tx.Version.TxNum)
					var validationCode peer.TxValidationCode
					var err error
					var reexecuted bool
					val_start_time := time.Now()
					v.lock.RLock()
					if validationCode, err = v.validateEndorserTX(tx, doMVCCValidation, updates); err != nil {
						panic(err)
					}
					val_tx_time_tot += time.Since(val_start_time).Nanoseconds()
					val_tx_time_cnt++
					tx.ValidationCode = validationCode
					for validationCode == peer.TxValidationCode_IS_HUB {
						v.lock.RUnlock()
						resimulationPartitions[partition] <- tx
						return nil
					}
					// reexecuted = validationCode == peer.TxValidationCode_RESIMULATED
					write_start_time := time.Now()
					if validationCode == peer.TxValidationCode_VALID || reexecuted {
						logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator (%s)", block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
						committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
						v.lock.RUnlock()
						v.lock.Lock()
						updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
						v.lock.Unlock()
					} else {
						v.lock.RUnlock()
						logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
							block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
						if validationCode == peer.TxValidationCode_BAD_RWSET || validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT {
							badTxIndices = append(badTxIndices, tx.Version.TxNum)
						}
					}
					write_tx_time_tot += time.Since(write_start_time).Nanoseconds()
					write_tx_time_cnt++
					processedTxs <- tx
					committedTxs <- tx
					// wg.Done()
					return nil
				}
	
				resimulatePartition := func(partition uint8, tx *deptype.Transaction, updates *internalVal.PubAndHashUpdates) error {
					reexecuted := true
					validationCode := v.reExecute(tx, updates, partition)
	
					if validationCode == peer.TxValidationCode_VALID || reexecuted {
						logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator (%s)", block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
						committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
						v.lock.RUnlock()
						v.lock.Lock()
						updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
						v.lock.Unlock()
						if reexecuted {
							reexecutedTxIndices = append(reexecutedTxIndices, tx.Version.TxNum)
						}
					} else {
						v.lock.RUnlock()
						logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
							block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
						if validationCode == peer.TxValidationCode_BAD_RWSET || validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT {
							badTxIndices = append(badTxIndices, tx.Version.TxNum)
						}
					}
					processedTxs <- tx
					committedTxs <- tx
					return nil
				}
	
				go func() {
					for i, partition := range validationPartitions {
						go func(i int, partition chan *deptype.Transaction) {
							
							for tx := range partition {
								validatePartition(uint8(i), tx, updates)
							}
							// fmt.Printf("wg done\n")
							// fmt.Printf("Closing resim[%d]\n", i)
							// fmt.Printf("Updates in validate: %p\n", updates)
							close(resimulationPartitions[i])
							// fmt.Printf("Closing resim[%d] done\n", i)
							wg.Done()
						}(i, partition)
					}
					// fmt.Printf("Waiting all validation\n")
					wg.Wait()
					if infolab.FastWb == true && !(config.IsEndorser || config.IsStorage){
						fmt.Printf("[state_based_val] fastWb\n")
						simulating.SendFastWb(block.Num, updates)
					}
	
					fmt.Printf("Finished Validation, intra/inter: %d, %d\n", intraBlockDep, interBlockDep)
					// fmt.Printf("Start re-simulation\n")
	
					for i, partition := range resimulationPartitions {
						go func(i int, partition chan *deptype.Transaction) {
							for tx := range partition {
								resimulatePartition(uint8(i), tx, updates)
							}
							// fmt.Printf("Updates in re-simulate: %p\n", updates)
							// fmt.Printf("resimWg done\n")
							resimWg.Done()
						}(i, partition)
					}
				}()
	
	
	
	
	
	
	
				// fmt.Printf("Generating  validation Partition channel\n")
	
				// fmt.Printf("Putting transactions to right partition\n")
				wg.Add(int(infolab.Partitions))
				resimWg.Add(int(infolab.Partitions))
				for tx := range txs {
					validationPartitions[tx.Partition] <- tx
				}
	
				for _, c := range validationPartitions {
					close(c)
				}
				resimWg.Wait()
				fmt.Printf("Finished re-simulation, intra/inter: %d, %d\n", intraBlockDep, interBlockDep)
				fmt.Printf("*********************************************\n")
			}


		}

	} else {
		if infolab.Partitions <= 1 {
			Log.Printf("Block: %d\n", block.Num)
			fmt.Printf("Batch valid = false, partition == 1\n")
			for tx := range txs {
				val_start_time := time.Now()
				v.lock.RLock()
				atomic.AddUint64(&txCount, 1)
				// Log.Printf("txId: %s\n", tx.TxID)
				// fmt.Printf("Inside partitions<=1\n")
				atomic.AddUint64(&txCount_total, 1)
				if infolab.DebugMode && infolab.ValidateAndPrepareBatchCallTime {
					// atomic.AddUint64(&txCount_total, 1)
				}

				if block.Num >= uint64(tpsMeasureStart) {
					atomic.AddUint64(&tpsTxCount, 1)
				}

				var validationCode peer.TxValidationCode
				var err error
				var reexecuted bool
				if validationCode, err = v.validateEndorserTX(tx, doMVCCValidation, updates); err != nil {
					panic(err)
				}
				val_tx_time_tot += time.Since(val_start_time).Nanoseconds()
				val_tx_time_cnt++
				tx.ValidationCode = validationCode
				for validationCode == peer.TxValidationCode_IS_HUB {
					resim_start_time := time.Now()
					v.lock.RUnlock()
					validationCode = v.reExecute(tx, updates, 0)
					tx.ValidationCode = validationCode
					resim_tx_time_tot += time.Since(resim_start_time).Nanoseconds()
					resim_tx_time_cnt++
				}
				reexecuted = validationCode == peer.TxValidationCode_RESIMULATED
				write_start_time := time.Now()
				if validationCode == peer.TxValidationCode_VALID || reexecuted {
					logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator (%s)", block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
					committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
					v.lock.RUnlock()
					v.lock.Lock()
					updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
					v.lock.Unlock()
					if reexecuted {
						reexecutedTxIndices = append(reexecutedTxIndices, tx.Version.TxNum)
					}

					atomic.AddUint64(&validCount, 1)
					atomic.AddUint64(&validCount_total, 1)
					if block.Num >= uint64(tpsMeasureStart) {
						atomic.AddUint64(&tpsValidCount, 1)
					}

					if infolab.DebugMode && infolab.ValidateAndPrepareBatchCallTime {
						if reexecuted {
							atomic.AddUint64(&resimCount, 1)
							atomic.AddUint64(&resimCount_total, 1)
							if block.Num >= uint64(tpsMeasureStart) {
								atomic.AddUint64(&tpsResimCount, 1)
							}

						} else {
							// atomic.AddUint64(&validCount, 1)
							// atomic.AddUint64(&validCount_total, 1)
							// if block.Num >= uint64(tpsMeasureStart) {
							// 	atomic.AddUint64(&tpsValidCount, 1)
							// }
						}
					}

				} else {
					logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
						block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
					if validationCode == peer.TxValidationCode_BAD_RWSET || validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT {
						badTxIndices = append(badTxIndices, tx.Version.TxNum)
					}
				}
				processedTxs <- tx
				committedTxs <- tx
				write_tx_time_tot += time.Since(write_start_time).Nanoseconds()
				write_tx_time_cnt++
			}
		} else {
			wg := &sync.WaitGroup{}

			f := func(partition uint8, tx *deptype.Transaction) error {
				atomic.AddUint64(&txCount, 1)
				// fmt.Printf("<%d> (%5d, %5d)\n", partition, block.Num, tx.Version.TxNum)
				var validationCode peer.TxValidationCode
				var err error
				var reexecuted bool
				val_start_time := time.Now()
				v.lock.RLock()
				if validationCode, err = v.validateEndorserTX(tx, doMVCCValidation, updates); err != nil {
					panic(err)
				}
				val_tx_time_tot += time.Since(val_start_time).Nanoseconds()
				val_tx_time_cnt++
				tx.ValidationCode = validationCode
				for validationCode == peer.TxValidationCode_IS_HUB {
					reexecuted = true
					resim_start_time := time.Now()
					v.lock.RUnlock()
					validationCode = v.reExecute(tx, updates, partition)
					tx.ValidationCode = validationCode
					resim_tx_time_tot += time.Since(resim_start_time).Nanoseconds()
					resim_tx_time_cnt++
					// if validationCode, err = v.validateEndorserTX(tx, doMVCCValidation, updates); err != nil {
					// 	panic(err)
					// }
				}
				// reexecuted = validationCode == peer.TxValidationCode_RESIMULATED
				write_start_time := time.Now()
				if validationCode == peer.TxValidationCode_VALID || reexecuted {
					logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator (%s)", block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
					committingTxHeight := version.NewHeight(block.Num, uint64(tx.Version.TxNum))
					v.lock.RUnlock()
					v.lock.Lock()
					updates.ApplyWriteSet(tx.RwSet, committingTxHeight, v.db)
					v.lock.Unlock()
					if reexecuted {
						reexecutedTxIndices = append(reexecutedTxIndices, tx.Version.TxNum)
					}
				} else {
					v.lock.RUnlock()
					logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
						block.Num, tx.Version.TxNum, tx.TxID, validationCode.String())
					if validationCode == peer.TxValidationCode_BAD_RWSET || validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT {
						badTxIndices = append(badTxIndices, tx.Version.TxNum)
					}
				}
				write_tx_time_tot += time.Since(write_start_time).Nanoseconds()
				write_tx_time_cnt++
				processedTxs <- tx
				// wg.Done()
				return nil
			}

			// Before tx iteration, block partitions, initialize AdjMat
			if partitionDAGEnabled {
				for i := range v.partitions {
					v.partitionWaitGroups[i].Add(1)
				}
			}

			v.AdjMat = make([][]uint8, infolab.Partitions)
			for m := range v.AdjMat {
				v.AdjMat[m] = make([]uint8, infolab.Partitions)
			}
			// Opened := make([]uint8, infolab.Partitions)
			// txlist := make([]uint8, 61)
			// var count = 0

			// var numTx = 0;

			partitionSlice := make([][]*deptype.Transaction, infolab.Partitions)
			for i := range partitionSlice {
				partitionSlice[i] = make([]*deptype.Transaction, 0, 1000)
			}

			for tx := range txs {
				// fmt.Printf("Added transaction to partition %d\n", tx.Partition)
				partitionSlice[tx.Partition] = append(partitionSlice[tx.Partition], tx)
			}

			for j := 0; j < int(infolab.Partitions); j++ {
				if len(partitionSlice[j]) == 0 {
					continue
				}
				// fmt.Printf("Putting partition %d in jobs\n", j)
				wg.Add(1)
				v.partitionTxs <- &partitionTasks{
					txs:       partitionSlice[j],
					f:         f,
					partition: uint8(j),
					wg:        wg,
				}
				for k := range partitionSlice[j] {
					committedTxs <- partitionSlice[j][k]
				}
			}

			wg.Wait()
			// fmt.Printf("Nums of tx in block: %d\n", numTx)
		}

	}

	if !(config.IsEndorser || config.IsStorage) {
		simulating.SendCatchUpBody(block.Num, updates)
	}
	if infolab.DebugMode && infolab.ValidateAndPrepareBatchCallTime {
		fmt.Printf("Function call count: %d, txs: %d, valid: %d, resim: %d, blockNum: %d\n", ValidAndPrepareBatchCallTime, txCount_total, validCount_total, resimCount_total, block.Num)
	}
	fmt.Printf("Function call count: %d, txs: %d, valid: %d, resim: %d, blockNum: %d\n", ValidAndPrepareBatchCallTime, txCount_total, validCount_total, resimCount_total, block.Num)
	infolab.Log.Printf("Function call count: %d, txs: %d, valid: %d, resim: %d, blockNum: %d\n", ValidAndPrepareBatchCallTime, txCount_total, validCount_total, resimCount_total, block.Num)

	if infolab.DebugMode && infolab.MeasureTps {
		if block.Num == uint64(tpsMeasureEnd) {
			tpsDuration := time.Since(tpsStartTime)
			fmt.Printf("Tx Count: %f\n", float64(tpsTxCount))
			fmt.Printf("Valid Count: %f\n", float64(tpsValidCount))
			fmt.Printf("Resim Count: %f\n\n", float64(tpsResimCount))
			fmt.Printf("Duration: %f\n\n", tpsDuration.Seconds())

			tps := float64(tpsTxCount) / tpsDuration.Seconds()
			valid_tps := float64(tpsValidCount) / tpsDuration.Seconds()
			resim_tps := float64(tpsResimCount) / tpsDuration.Seconds()

			fmt.Printf("TPS: %f\n", tps)
			fmt.Printf("Valid TPS: %f\n", float64(valid_tps))
			fmt.Printf("Resim TPS: %f\n", float64(resim_tps))

		}
	}

	if infolab.DebugMode && infolab.MeasureValidateAndPrepareBatchTime {
		functionEndTime := time.Since(functionStartTime).Nanoseconds()
		if receiveCount >= 301 {
			functionDurationAvg += functionEndTime
			fmt.Printf("ValidateAndPrepareBatch[%d] Duration is %d\n", receiveCount, functionEndTime)
		}
		if receiveCount == 1000 {
			functionDurationAvg = functionDurationAvg / 3000
			fmt.Printf("** ValidateAndPrepareBatch Duration Average is %d\n", functionDurationAvg)
		}
	}

	end_to_end_tot += time.Since(end_to_end_start).Nanoseconds()
	if block.Num > 0 && block.Num%100 == 0 {
		val_tx_time_cnt++
		resim_tx_time_cnt++
		write_tx_time_cnt++
		fmt.Printf("val avg: %d, cnt: %d\n", val_tx_time_tot/val_tx_time_cnt, val_tx_time_cnt)
		fmt.Printf("resim avg: %d, cnt: %d\n", resim_tx_time_tot/resim_tx_time_cnt, resim_tx_time_cnt)
		fmt.Printf("write avg: %d, cnt: %d\n", write_tx_time_tot/write_tx_time_cnt, write_tx_time_cnt)
		fmt.Printf("e-e avg: %d\n", end_to_end_tot/int64(block.Num))

	}
	if block.Num > 10 {
		validateLatencyTotal += time.Since(validateLatencyStart).Nanoseconds()
		validateLatencyCount++
		Log.Printf("ValidateLatency:%d\n", validateLatencyTotal/validateLatencyCount)
		Log.Printf("IntraBlockDep:%d\tInterBlockDep:%d\n", intraBlockDep, interBlockDep)
		Log.Printf("BlockNum:%d\n", block.Num)
		Log.Printf("txsInBlock:%d\n", txCount)
	}

	// Log.Printf("Block num: %d\n", block.Num)
	// if block.Num > 770 {
	// 	Log.Printf("IntraBlockDep:%d\tInterBlockDep:%d\n", intraBlockDep, interBlockDep)
	// 	Log.Printf("valAvg:%d\n", val_tx_time_tot/int64(block.Num))
	// 	Log.Printf("resimAvg:%d\n", resim_tx_time_tot/int64(block.Num))
	// 	Log.Printf("writeAvg:%d\n", write_tx_time_tot/int64(block.Num))
	// 	Log.Printf("prepareRwSetAvg:%d\n", prepare_rwset_time_tot/int64(block.Num))
	// 	Log.Printf("eteAvg:%d\n", end_to_end_tot/int64(block.Num))
	// 	Log.Printf("BlockNum:%d\n", block.Num)
	// }

	return updates, reexecutedTxIndices, badTxIndices, nil
}

// validateEndorserTX validates endorser transaction
func (v *Validator) validateEndorserTX(
	tx *deptype.Transaction,
	doMVCCValidation bool,
	updates *internalVal.PubAndHashUpdates) (peer.TxValidationCode, error) {

	var validationCode = peer.TxValidationCode_VALID
	var err error

	if !infolab.NoResim  && (config.IsStorage || config.IsEndorser) {
		validationCode = tx.ValidationCode
	} else if doMVCCValidation {
		// v.lock.RLock()
		validationCode, err = v.validateTxInfo(tx, updates)
		// v.lock.RUnlock()
		// if validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT && tx.IsHub() {
		if validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT && !infolab.NoResim {
			// fmt.Printf("Eventually re-simulate this")
			validationCode = peer.TxValidationCode_IS_HUB
		} else {
			// fmt.Printf("validateEndorserTX: (%d %d): %s\n", tx.Version.BlockNum, tx.Version.TxNum, validationCode.String())
		}
	}
	if err != nil {
		return peer.TxValidationCode(-1), err
	}

	return validationCode, nil
}

func (v *Validator) validateTx(tx *deptype.Transaction, updates *internalVal.PubAndHashUpdates) (peer.TxValidationCode, error) {
	// Uncomment the following only for local debugging. Don't want to print data in the logs in production
	//logger.Debugf("validateTx - validating txRWSet: %s", spew.Sdump(txRWSet))

	if !infolab.NoResim  && (config.IsStorage || config.IsEndorser) {
		return tx.ValidationCode, nil
	}

	for _, nsRWSet := range tx.RwSet.NsRwSets {
		ns := nsRWSet.NameSpace

		// Validate public reads
		if valid, err := v.validateReadSet(ns, nsRWSet.KvRwSet.Reads, updates.PubUpdates, tx.EarlySimulated); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
		// Validate range queries for phantom items
		if valid, err := v.validateRangeQueries(ns, nsRWSet.KvRwSet.RangeQueriesInfo, updates.PubUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_PHANTOM_READ_CONFLICT, nil
		}
		// Validate hashes for private reads
		if valid, err := v.validateNsHashedReadSets(ns, nsRWSet.CollHashedRwSets, updates.HashUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
	}
	v.rememberWrites(tx.RwSet, tx.Version)

	return peer.TxValidationCode_VALID, nil
}

func (v *Validator) rememberWrites(rwset *cached.TxRwSet, txVersion *version.Height) {
	validatedKeys := <-v.validatedKeys
	for _, set := range rwset.NsRwSets {
		for _, write := range set.KvRwSet.Writes {

			validatedKeys[set.NameSpace+"_"+write.Key] = txVersion
		}
	}
	v.validatedKeys <- validatedKeys
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of public read-set
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateReadSet(ns string, kvReads []*kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch, earlySimulated bool) (bool, error) {
	for _, kvRead := range kvReads {
		if valid, err := v.validateKVRead(ns, kvRead, updates, earlySimulated); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}


// validateKVRead performs mvcc check for a key read during transaction simulation.
// i.e., it checks whether a key/version combination is already updated in the statedb (by an already committed block)
// or in the updates (by a preceding valid transaction in the current block)
func (v *Validator) validateKVRead(ns string, kvRead *kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch, earlySimulated bool) (bool, error) {
	if updates.Exists(ns, kvRead.Key) {
		atomic.AddInt32(&intraBlockDep, 1)
		// fmt.Println("validateKVRead 1", ns, kvRead.Key, kvRead.Version)
		return false, nil
	}
	if earlySimulated {
		// fmt.Println("validateKVRead 2", ns, kvRead.Key, kvRead.Version)
		return true, nil
	}

	committedVersion, err := v.db.GetVersion(ns, kvRead.Key)
	if err != nil {
		// fmt.Println("validateKVRead 3", ns, kvRead.Key, kvRead.Version)
		return false, err
	}
	// if kvRead.Version != nil && kvRead.Version.TxNum == 12345678 {
	// fmt.Println("validateKVRead 4", ns, kvRead.Key, kvRead.Version)
	// return true, nil
	// }
	// fmt.Println("validateKVRead 5", ns, kvRead.Key, kvRead.Version)

	// logger.Debugf("Comparing versions for key [%s]: committed version=%#v and read version=%#v",
	// 	kvRead.Key, committedVersion, rwsetutil.NewVersion(kvRead.Version))
	target := rwsetutil.NewVersion(kvRead.Version)

	if !version.AreSame(committedVersion, target) {
		atomic.AddInt32(&interBlockDep, 1)
		logger.Debugf("Version mismatch for key [%s:%s]. Committed version = [%#v], Version in readSet [%#v]",
			ns, kvRead.Key, committedVersion, kvRead.Version)
		return false, nil
	}

	validatedKeys := <-v.validatedKeys
	if ver, ok := validatedKeys[ns+"_"+kvRead.Key]; ok {
		if committedVersion != nil && ver.Compare(committedVersion) <= 0 {
			delete(validatedKeys, ns+"_"+kvRead.Key)
			v.validatedKeys <- validatedKeys
			return true, nil
		}
		if !version.AreSame(ver, target) {
			v.validatedKeys <- validatedKeys
			return false, nil
		}
	}
	v.validatedKeys <- validatedKeys

	return true, nil
}



////////////////////////////////////////////////////////////////////////////////
/////                 Validation of range queries
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateRangeQueries(ns string, rangeQueriesInfo []*kvrwset.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	for _, rqi := range rangeQueriesInfo {
		if valid, err := v.validateRangeQuery(ns, rqi, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateRangeQuery performs a phantom read check i.e., it
// checks whether the results of the range query are still the same when executed on the
// statedb (latest state as of last committed block) + updates (prepared by the writes of preceding valid transactions
// in the current block and yet to be committed as part of group commit at the end of the validation of the block)
func (v *Validator) validateRangeQuery(ns string, rangeQueryInfo *kvrwset.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	logger.Debugf("validateRangeQuery: ns=%s, rangeQueryInfo=%s", ns, rangeQueryInfo)

	// If during simulation, the caller had not exhausted the iterator so
	// rangeQueryInfo.EndKey is not actual endKey given by the caller in the range query
	// but rather it is the last key seen by the caller and hence the combinedItr should include the endKey in the results.
	includeEndKey := !rangeQueryInfo.ItrExhausted

	combinedItr, err := newCombinedIterator(v.db, updates.UpdateBatch,
		ns, rangeQueryInfo.StartKey, rangeQueryInfo.EndKey, includeEndKey)
	if err != nil {
		return false, err
	}
	defer combinedItr.Close()
	var validator rangeQueryValidator
	if rangeQueryInfo.GetReadsMerkleHashes() != nil {
		logger.Debug(`Hashing results are present in the range query info hence, initiating hashing based validation`)
		validator = &rangeQueryHashValidator{}
	} else {
		logger.Debug(`Hashing results are not present in the range query info hence, initiating raw KVReads based validation`)
		validator = &rangeQueryResultsValidator{}
	}
	validator.init(rangeQueryInfo, combinedItr)
	return validator.validate()
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of hashed read-set
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateNsHashedReadSets(ns string, collHashedRWSets []*cached.CollHashedRwSet,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, collHashedRWSet := range collHashedRWSets {
		if valid, err := v.validateCollHashedReadSet(ns, collHashedRWSet.CollectionName, collHashedRWSet.HashedRwSet.HashedReads, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateCollHashedReadSet(ns, coll string, kvReadHashes []*kvrwset.KVReadHash,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, kvReadHash := range kvReadHashes {
		if valid, err := v.validateKVReadHash(ns, coll, kvReadHash, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateKVReadHash performs mvcc check for a hash of a key that is present in the private data space
// i.e., it checks whether a key/version combination is already updated in the statedb (by an already committed block)
// or in the updates (by a preceding valid transaction in the current block)
func (v *Validator) validateKVReadHash(ns, coll string, kvReadHash *kvrwset.KVReadHash,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	if updates.Contains(ns, coll, kvReadHash.KeyHash) {
		return false, nil
	}
	committedVersion, err := v.db.GetKeyHashVersion(ns, coll, kvReadHash.KeyHash)
	if err != nil {
		return false, err
	}

	if !version.AreSame(committedVersion, rwsetutil.NewVersion(kvReadHash.Version)) {
		logger.Debugf("Version mismatch for key hash [%s:%s:%#v]. Committed version = [%s], Version in hashedReadSet [%s]",
			ns, coll, kvReadHash.KeyHash, committedVersion, kvReadHash.Version)
		return false, nil
	}
	return true, nil
}

type mockUI struct {
	output string
}

func (mockUI) SetPrompt(string) {
}

func (m *mockUI) Print(s string) {
	m.output = s
}

func (v *Validator) reExecute(tx *deptype.Transaction, updates *internalVal.PubAndHashUpdates, partition uint8) peer.TxValidationCode {
	// 실험
	// if tx.Version.BlockNum > 10 {
	// 	return peer.TxValidationCode_VALID
	// }
	// fmt.Printf("[start] re-execute\n")
	valCode := v.executeChaincode(tx, updates, v.db, partition)
	// fmt.Printf("execute chaincode done\n")
	if valCode == peer.TxValidationCode_VALID {
		valCode = peer.TxValidationCode_RESIMULATED
		v.rememberWrites(tx.RwSet, tx.Version)
	}
	// fmt.Printf("[end] re-execute\n")

	return valCode
}

func (v *Validator) executeChaincode(transaction *deptype.Transaction, updates *internalVal.PubAndHashUpdates, db privacyenabledstate.DB, partition uint8) peer.TxValidationCode {
	for {
		var newSet []map[string]interface{}

		// v.lock.Lock()
		prepare_start := time.Now()
		v.lock.RLock()
		rwset := prepareRwSet(transaction.RwSet, updates, db)
		v.lock.RUnlock()
		prepare_rwset_time_tot += time.Since(prepare_start).Nanoseconds()
		prepare_rwset_time_cnt++
		benchmark := rwset[0]["benchmark"]
		delete(rwset[0], "benchmark")
		args := getArgs(transaction.Payload)

		// JNOTE 하드코딩된 체인코드
		if infolab.EmbeddedChaincode {
			switch string(args[0]) {
			case "send2":
				account1 := string(args[1])
				account2 := string(args[2])
				acc1Value := 0
				acc2Value := 0
				if chunk, ok := rwset[0][account1]; ok {
					acc1Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account2]; ok {
					acc2Value, _ = strconv.Atoi(chunk.(string))
				}
				transferAmount, _ := strconv.Atoi(string(args[3]))
				newSet = []map[string]interface{}{
					rwset[0],
					{
						account1: fmt.Sprint(acc1Value - transferAmount),
						account2: fmt.Sprint(acc2Value + transferAmount),
					},
					rwset[2],
				}
			case "send4":
				account1 := string(args[1])
				account2 := string(args[2])
				account3 := string(args[3])
				account4 := string(args[4])
				acc1Value := 0
				acc2Value := 0
				acc3Value := 0
				acc4Value := 0
				if chunk, ok := rwset[0][account1]; ok {
					acc1Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account2]; ok {
					acc2Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account3]; ok {
					acc3Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account4]; ok {
					acc4Value, _ = strconv.Atoi(chunk.(string))
				}
				transferAmount, _ := strconv.Atoi(string(args[5]))
				newSet = []map[string]interface{}{
					rwset[0],
					{
						account1: fmt.Sprint(acc1Value - 3*transferAmount),
						account2: fmt.Sprint(acc2Value + transferAmount),
						account3: fmt.Sprint(acc3Value + transferAmount),
						account4: fmt.Sprint(acc4Value + transferAmount),
					},
					rwset[2],
				}
			case "send6":
				account1 := string(args[1])
				account2 := string(args[2])
				account3 := string(args[3])
				account4 := string(args[4])
				account5 := string(args[5])
				account6 := string(args[6])
				acc1Value := 0
				acc2Value := 0
				acc3Value := 0
				acc4Value := 0
				acc5Value := 0
				acc6Value := 0
				if chunk, ok := rwset[0][account1]; ok {
					acc1Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account2]; ok {
					acc2Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account3]; ok {
					acc3Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account4]; ok {
					acc4Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account5]; ok {
					acc5Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account6]; ok {
					acc6Value, _ = strconv.Atoi(chunk.(string))
				}
				transferAmount, _ := strconv.Atoi(string(args[7]))
				newSet = []map[string]interface{}{
					rwset[0],
					{
						account1: fmt.Sprint(acc1Value - 5*transferAmount),
						account2: fmt.Sprint(acc2Value + transferAmount),
						account3: fmt.Sprint(acc3Value + transferAmount),
						account4: fmt.Sprint(acc4Value + transferAmount),
						account5: fmt.Sprint(acc5Value + transferAmount),
						account6: fmt.Sprint(acc6Value + transferAmount),
					},
					rwset[2],
				}
			case "send8":
				account1 := string(args[1])
				account2 := string(args[2])
				account3 := string(args[3])
				account4 := string(args[4])
				account5 := string(args[5])
				account6 := string(args[6])
				account7 := string(args[7])
				account8 := string(args[8])
				acc1Value := 0
				acc2Value := 0
				acc3Value := 0
				acc4Value := 0
				acc5Value := 0
				acc6Value := 0
				acc7Value := 0
				acc8Value := 0


				if chunk, ok := rwset[0][account1]; ok {
					acc1Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account2]; ok {
					acc2Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account3]; ok {
					acc3Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account4]; ok {
					acc4Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account5]; ok {
					acc5Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account6]; ok {
					acc6Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account7]; ok {
					acc7Value, _ = strconv.Atoi(chunk.(string))
				}
				if chunk, ok := rwset[0][account8]; ok {
					acc8Value, _ = strconv.Atoi(chunk.(string))
				}
				transferAmount, _ := strconv.Atoi(string(args[9]))
				newSet = []map[string]interface{}{
					rwset[0],
					{
						account1: fmt.Sprint(acc1Value - 7*transferAmount),
						account2: fmt.Sprint(acc2Value + transferAmount),
						account3: fmt.Sprint(acc3Value + transferAmount),
						account4: fmt.Sprint(acc4Value + transferAmount),
						account5: fmt.Sprint(acc5Value + transferAmount),
						account6: fmt.Sprint(acc6Value + transferAmount),
						account7: fmt.Sprint(acc7Value + transferAmount),
						account8: fmt.Sprint(acc8Value + transferAmount),
					},
					rwset[2],
				}
			}
		} else {
			pyRwSet := pythonifyRwSet(rwset)
			pyArgs := pythonifySlice(args)
			call := "execute(" + pyRwSet + "," + pyArgs + ")"
			v.vmList[partition].RunWithGas(call, config.Gas)
			if v.vmList[partition].OutOfGas {
				continue
				// return peer.TxValidationCode_INVALID_OTHER_REASON
			}
			newSet = toRwSetObject(v.output())
			if newSet == nil {
				continue
				// return peer.TxValidationCode_BAD_RWSET
			}
		}

		newSet[0]["benchmark"] = benchmark
		if err := compareAndUpdate(transaction, newSet); err != nil {
			continue
			// return peer.TxValidationCode_BAD_RWSET
		}
		break
	}
	v.lock.RLock()
	return peer.TxValidationCode_VALID
}

func pythonifySlice(args [][]byte) string {
	sb := strings.Builder{}
	sb.WriteString("[")
	for i := 0; i < len(args); i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("\"")
		sb.Write(args[i])
		sb.WriteString("\"")
	}
	sb.WriteString("]")
	return sb.String()
}

func getArgs(payload *cached.Payload) [][]byte {
	tx, _ := payload.UnmarshalTransaction()
	cca, _ := tx.Actions[0].UnmarshalChaincodeActionPayload()
	ppl, _ := cca.UnmarshalProposalPayload()
	input, _ := ppl.UnmarshalInput()
	return input.ChaincodeSpec.Input.Args
}

func prepareRwSet(set *cached.TxRwSet, updates *internalVal.PubAndHashUpdates, db privacyenabledstate.DB) []map[string]interface{} {
	newSet := make([]map[string]interface{}, 3, 3)
	for i := 0; i < 3; i++ {
		newSet[i] = make(map[string]interface{})
	}
	for _, ns := range set.NsRwSets {
		for _, r := range ns.KvRwSet.Reads {
			val := updates.PubUpdates.Get(ns.NameSpace, r.Key)
			if val == nil {
				var err error
				val, err = db.GetState(ns.NameSpace, r.Key)
				if err != nil {
					panic(err)
				}
			}
			if val == nil {
				newSet[0][r.Key] = "0"
			} else {
				newSet[0][r.Key] = string(val.Value)
			}
		}
		for _, w := range ns.KvRwSet.Writes {
			if !strings.HasPrefix(w.Key, "oracle_") {
				newSet[1][w.Key] = string(w.Value)
			} else {
				newSet[2][strings.TrimPrefix(w.Key, "oracle_")] = string(w.Value)
			}
		}
	}
	return newSet
}

func pythonifyRwSet(set []map[string]interface{}) string {
	s, _ := json.Marshal(set)
	return string(s)
}

func toRwSetObject(s string) []map[string]interface{} {
	var set []map[string]interface{} = nil

	s = strings.Replace(s, "\"", "\\u0022", -1)
	s = strings.Replace(s, "'", "\"", -1)
	s = strings.Replace(s, "\\x", "\\u00", -1)
	if err := json.Unmarshal([]byte(s), &set); err != nil {
		// panic(err)
		fmt.Printf("헉: %s\n", s)
		// for i := 0; i < len(s); i++ {
		// 	fmt.Printf("[%d] %d\n", i, s[i])
		// }
		// logger.Warningf("헉: %s", err.Error())
		return nil
	}
	return set
}

func compareAndUpdate(transaction *deptype.Transaction, newSet []map[string]interface{}) error {
	set := transaction.RwSet
	readCount := 0
	writeCount := 0
	oracleCount := 0
	hasError := false
	for _, ns := range set.NsRwSets {
		for _, r := range ns.KvRwSet.Reads {
			readCount++
			if _, ok := newSet[0][r.Key]; !ok {
				// logger.Panicf("읽기 집합에 없는 키: %s", r.Key)
				hasError = true
				break
			}
		}
		for _, w := range ns.KvRwSet.Writes {
			if !strings.HasPrefix(w.Key, "oracle_") {
				writeCount++
				if _, ok := newSet[1][w.Key]; !ok {
					// for k, v := range newSet[1] {
					// 	fmt.Printf("[%5s] %+v\n", k, v)
					// }
					// logger.Panicf("[%s] 쓰기 집합에 없는 키: %s", transaction.TxID, w.Key)
					hasError = true
					break
				} else {
					w.Value = []byte(newSet[1][w.Key].(string))
				}
			} else {
				oracleCount++
				if _, ok := newSet[2][strings.TrimPrefix(w.Key, "oracle_")]; !ok {
					hasError = true
					break
				}
			}
		}
	}
	if hasError {
		return errors.New("could not update transaction after re-execution")
	}
	return nil
}


func (v *Validator) validateTxInfo(tx *deptype.Transaction, updates *internalVal.PubAndHashUpdates) (peer.TxValidationCode, error) {
	// Uncomment the following only for local debugging. Don't want to print data in the logs in production
	//logger.Debugf("validateTx - validating txRWSet: %s", spew.Sdump(txRWSet))

	if !infolab.NoResim  && (config.IsStorage || config.IsEndorser) {
		return tx.ValidationCode, nil
	}

	for _, nsRWSet := range tx.RwSet.NsRwSets {
		ns := nsRWSet.NameSpace

		// Validate public reads for Intra-block conflict
		if infolab.CheckIntra {
			if valid, err := v.validateReadSetInfoIntra(ns, nsRWSet.KvRwSet.Reads, updates.PubUpdates, tx.EarlySimulated); !valid || err != nil {
				atomic.AddInt32(&intraBlockDep, 1)
				if err != nil {
					return peer.TxValidationCode(-1), err
				}
				return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
			}
		}

	}


	for _, nsRWSet := range tx.RwSet.NsRwSets {
		ns := nsRWSet.NameSpace

		// Validate public reads for Inter-block conflict
		if infolab.CheckInter {
			if valid, err := v.validateReadSetInfoInter(ns, nsRWSet.KvRwSet.Reads, updates.PubUpdates, tx.EarlySimulated); !valid || err != nil {
				atomic.AddInt32(&interBlockDep, 1)
				if err != nil {
					return peer.TxValidationCode(-1), err
				}
				return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
			}
		}

		// Validate range queries for phantom items
		if valid, err := v.validateRangeQueries(ns, nsRWSet.KvRwSet.RangeQueriesInfo, updates.PubUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_PHANTOM_READ_CONFLICT, nil
		}
		// Validate hashes for private reads
		if valid, err := v.validateNsHashedReadSets(ns, nsRWSet.CollHashedRwSets, updates.HashUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
	}
	v.rememberWrites(tx.RwSet, tx.Version)

	return peer.TxValidationCode_VALID, nil
}

func (v *Validator) revalidateTxInfo(tx *deptype.Transaction, updates *internalVal.PubAndHashUpdates) (peer.TxValidationCode, error) {
	// Uncomment the following only for local debugging. Don't want to print data in the logs in production
	//logger.Debugf("validateTx - validating txRWSet: %s", spew.Sdump(txRWSet))

	if !infolab.NoResim  && (config.IsStorage || config.IsEndorser) {
		return tx.ValidationCode, nil
	}


	// for _, nsRWSet := range tx.RwSet.NsRwSets {
	// 	ns := nsRWSet.NameSpace

	// 	// Validate public reads for Intra-block conflict
	// 	if infolab.CheckIntra {
	// 		if valid, err := v.validateReadSetInfoIntra(ns, nsRWSet.KvRwSet.Reads, updates.PubUpdates, tx.EarlySimulated); !valid || err != nil {
	// 			atomic.AddInt32(&intraBlockDep, 1)
	// 			if err != nil {
	// 				return peer.TxValidationCode(-1), err
	// 			}
	// 			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
	// 		}
	// 	}

	// }

	for _, nsRWSet := range tx.RwSet.NsRwSets {
		ns := nsRWSet.NameSpace

		// Validate public reads for Inter-block conflict
		if infolab.CheckInter {
			if valid, err := v.validateReadSetInfoInter(ns, nsRWSet.KvRwSet.Reads, updates.PubUpdates, tx.EarlySimulated); !valid || err != nil {
				atomic.AddInt32(&interBlockDep, 1)
				if err != nil {
					return peer.TxValidationCode(-1), err
				}
				return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
			}
		}

		// Validate range queries for phantom items
		if valid, err := v.validateRangeQueries(ns, nsRWSet.KvRwSet.RangeQueriesInfo, updates.PubUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_PHANTOM_READ_CONFLICT, nil
		}
		// Validate hashes for private reads
		if valid, err := v.validateNsHashedReadSets(ns, nsRWSet.CollHashedRwSets, updates.HashUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
	}
	v.rememberWrites(tx.RwSet, tx.Version)

	return peer.TxValidationCode_VALID, nil
}


func (v *Validator) validateReadSetInfoIntra(ns string, kvReads []*kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch, earlySimulated bool) (bool, error) {
	for _, kvRead := range kvReads {
		// Log.Printf("key: %s, ", kvRead.Key)
		if valid, err := v.validateKVReadIntra(ns, kvRead, updates, earlySimulated); !valid || err != nil {
			// Log.Printf("Intrablock detected")
			return valid, err
		}
	}
	// Log.Printf("\n")
	return true, nil
}

func (v *Validator) validateReadSetInfoInter(ns string, kvReads []*kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch, earlySimulated bool) (bool, error) {
	for _, kvRead := range kvReads {
		if valid, err := v.validateKVReadInter(ns, kvRead, updates, earlySimulated); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateKVReadIntra(ns string, kvRead *kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch, earlySimulated bool) (bool, error) {
	// fmt.Printf("Length of intra wb is: %d\n", updates.getLen(ns))
	if updates.Exists(ns, kvRead.Key) {
		// Log.Printf("IntrablockDep++ => %d", intraBlockDep)
		// fmt.Println("validateKVRead 1", ns, kvRead.Key, kvRead.Version)
		return false, nil
	}
	if earlySimulated {
		// fmt.Println("validateKVRead 2", ns, kvRead.Key, kvRead.Version)
		return true, nil
	}
	return true, nil
}


func (v *Validator) validateKVReadInter(ns string, kvRead *kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch, earlySimulated bool) (bool, error) {
	committedVersion, err := v.db.GetVersion(ns, kvRead.Key)
	if err != nil {
		// fmt.Println("validateKVRead 3", ns, kvRead.Key, kvRead.Version)
		return false, err
	}
	// if kvRead.Version != nil && kvRead.Version.TxNum == 12345678 {
	// fmt.Println("validateKVRead 4", ns, kvRead.Key, kvRead.Version)
	// return true, nil
	// }
	// fmt.Println("validateKVRead 5", ns, kvRead.Key, kvRead.Version)

	// logger.Debugf("Comparing versions for key [%s]: committed version=%#v and read version=%#v",
	// 	kvRead.Key, committedVersion, rwsetutil.NewVersion(kvRead.Version))
	target := rwsetutil.NewVersion(kvRead.Version)

	if !version.AreSame(committedVersion, target) {
		logger.Debugf("Version mismatch for key [%s:%s]. Committed version = [%#v], Version in readSet [%#v]",
			ns, kvRead.Key, committedVersion, kvRead.Version)
		return false, nil
	}

	validatedKeys := <-v.validatedKeys
	if ver, ok := validatedKeys[ns+"_"+kvRead.Key]; ok {
		if committedVersion != nil && ver.Compare(committedVersion) <= 0 {
			delete(validatedKeys, ns+"_"+kvRead.Key)
			v.validatedKeys <- validatedKeys
			return true, nil
		}
		if !version.AreSame(ver, target) {
			v.validatedKeys <- validatedKeys
			return false, nil
		}
	}
	v.validatedKeys <- validatedKeys

	return true, nil
}


// Tx1: Write key: K_a
// Tx2: Read key: K_a

// validateEndorserTX validates endorser transaction
func (v *Validator) revalidateEndorserTX(
	tx *deptype.Transaction,
	doMVCCValidation bool,
	updates *internalVal.PubAndHashUpdates) (peer.TxValidationCode, error) {

	var validationCode = peer.TxValidationCode_VALID
	var err error

	if !infolab.NoResim  && (config.IsStorage || config.IsEndorser) {
		validationCode = tx.ValidationCode
	} else if doMVCCValidation {
		// v.lock.RLock()
		validationCode, err = v.revalidateTxInfo(tx, updates)
		// v.lock.RUnlock()
		// if validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT && tx.IsHub() {
		if validationCode == peer.TxValidationCode_MVCC_READ_CONFLICT && !infolab.NoResim {
			// fmt.Printf("Eventually re-simulate this")
			validationCode = peer.TxValidationCode_IS_HUB
		} else {
			// fmt.Printf("validateEndorserTX: (%d %d): %s\n", tx.Version.BlockNum, tx.Version.TxNum, validationCode.String())
		}
	}
	if err != nil {
		return peer.TxValidationCode(-1), err
	}

	return validationCode, nil
}