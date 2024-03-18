/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
	"github.com/hyperledger/fabric/fastfabric/config"
	"github.com/hyperledger/fabric/infolab"
	"github.com/hyperledger/fabric/protos/peer"
)

type stats struct {
	blockchainHeight         metrics.Gauge
	endorserBlockchainHeight metrics.Gauge
	blockProcessingTime      metrics.Histogram
	blockstorageCommitTime   metrics.Histogram
	blockstorageUnlockTime   metrics.Histogram
	statedbCommitTime        metrics.Histogram
	transactionsCount        metrics.Counter
}

func newStats(metricsProvider metrics.Provider) *stats {
	stats := &stats{}
	stats.blockchainHeight = metricsProvider.NewGauge(blockchainHeightOpts)
	stats.endorserBlockchainHeight = metricsProvider.NewGauge(endorserBlockchainHeightOpts)
	stats.blockProcessingTime = metricsProvider.NewHistogram(blockProcessingTimeOpts)
	stats.blockstorageCommitTime = metricsProvider.NewHistogram(blockstorageCommitTimeOpts)
	stats.blockstorageUnlockTime = metricsProvider.NewHistogram(blockstorageUnlockTimeOpts)
	stats.statedbCommitTime = metricsProvider.NewHistogram(statedbCommitTimeOpts)
	stats.transactionsCount = metricsProvider.NewCounter(transactionCountOpts)
	return stats
}

type ledgerStats struct {
	stats    *stats
	ledgerid string
}

func (s *stats) ledgerStats(ledgerid string) *ledgerStats {
	return &ledgerStats{
		s, ledgerid,
	}
}

var validNum uint64
var resimNum uint64

func (s *ledgerStats) updateBlockchainHeight(height uint64) {
	// casting uint64 to float64 guarentees precision for the numbers upto 9,007,199,254,740,992 (1<<53)
	// since, we are not expecting the blockchains of this scale anytime soon, we go ahead with this for now.
	if config.IsEndorser || config.IsStorage {
		if infolab.UpdateEndorserBlockchainHeight == nil {
			infolab.UpdateEndorserBlockchainHeight = func(h uint64) {
				s.stats.endorserBlockchainHeight.With("channel", s.ledgerid).Set(float64(h))
			}
		}
		s.stats.endorserBlockchainHeight.With("channel", s.ledgerid).Set(float64(height))
	} else {
		s.stats.blockchainHeight.With("channel", s.ledgerid).Set(float64(height))
	}
}

func (s *ledgerStats) updateBlockProcessingTime(timeTaken time.Duration) {
	s.stats.blockProcessingTime.With("channel", s.ledgerid).Observe(timeTaken.Seconds())
}

func (s *ledgerStats) updateBlockstorageCommitTime(timeTaken time.Duration) {
	s.stats.blockstorageCommitTime.With("channel", s.ledgerid).Observe(timeTaken.Seconds())
}

func (s *ledgerStats) updateBlockstorageUnlockTime(timeTaken time.Duration) {
	s.stats.blockstorageUnlockTime.With("channel", s.ledgerid).Observe(timeTaken.Seconds())
}

func (s *ledgerStats) updateStatedbCommitTime(timeTaken time.Duration) {
	s.stats.statedbCommitTime.With("channel", s.ledgerid).Observe(timeTaken.Seconds())
}

func (s *ledgerStats) updateTransactionsStats(
	txstatsInfo []*txmgr.TxStatInfo,
) {
	for _, txstat := range txstatsInfo {
		transactionTypeStr := "unknown"
		if txstat.TxType != -1 {
			transactionTypeStr = txstat.TxType.String()
		}

		chaincodeName := "unknown"
		if txstat.ChaincodeID != nil {
			chaincodeName = txstat.ChaincodeID.Name + ":" + txstat.ChaincodeID.Version
		}

		s.stats.transactionsCount.With(
			"channel", s.ledgerid,
			"transaction_type", transactionTypeStr,
			"chaincode", chaincodeName,
			"validation_code", txstat.ValidationCode.String(),
		).Add(1)
		if infolab.DebugMode && infolab.CheckMetric {
			if txstat.ValidationCode == peer.TxValidationCode_VALID {
				atomic.AddUint64(&validNum, 1)
			} else if txstat.ValidationCode == peer.TxValidationCode_RESIMULATED {
				atomic.AddUint64(&resimNum, 1)
			} else {
				fmt.Printf("(core/ledger/kvledger/metrics.go) Current tx type is %s\n", txstat.ValidationCode.String())
			}
		}
	}
	if infolab.DebugMode && infolab.CheckMetric {
		fmt.Printf("(core/ledger/kvledger/metrics.go) updateTransactionStats ValidNum: %d, resimNum: %d\n", validNum, resimNum)
	}

}

var hostName, _ = os.Hostname()
var (
	blockchainHeightOpts = metrics.GaugeOpts{
		Namespace:    "ledger",
		Subsystem:    "",
		Name:         "blockchain_height",
		Help:         "Height of the chain in blocks.",
		LabelNames:   []string{"channel"},
		StatsdFormat: "%{#fqname}.%{channel}",
	}

	endorserBlockchainHeightOpts = metrics.GaugeOpts{
		Namespace:    "ledger",
		Subsystem:    "",
		Name:         "endorser_blockchain_height",
		Help:         "Height of the chain in endorsers' blocks.",
		LabelNames:   []string{"channel"},
		StatsdFormat: "%{#fqname}." + hostName + ".%{channel}",
	}

	blockProcessingTimeOpts = metrics.HistogramOpts{
		Namespace:    "ledger",
		Subsystem:    "",
		Name:         "block_processing_time",
		Help:         "Time taken in seconds for ledger block processing.",
		LabelNames:   []string{"channel"},
		StatsdFormat: "%{#fqname}.%{channel}",
		Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	}

	blockstorageCommitTimeOpts = metrics.HistogramOpts{
		Namespace:    "ledger",
		Subsystem:    "",
		Name:         "blockstorage_commit_time",
		Help:         "Time taken in seconds for committing the block and private data to storage.",
		LabelNames:   []string{"channel"},
		StatsdFormat: "%{#fqname}.%{channel}",
		Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	}

	blockstorageUnlockTimeOpts = metrics.HistogramOpts{
		Namespace:    "ledger",
		Subsystem:    "",
		Name:         "blockstorage_unlock_time",
		Help:         "Time taken in seconds for unlocking the block and private data to storage.",
		LabelNames:   []string{"channel"},
		StatsdFormat: "%{#fqname}.%{channel}",
		Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	}

	statedbCommitTimeOpts = metrics.HistogramOpts{
		Namespace:    "ledger",
		Subsystem:    "",
		Name:         "statedb_commit_time",
		Help:         "Time taken in seconds for committing block changes to state db.",
		LabelNames:   []string{"channel"},
		StatsdFormat: "%{#fqname}.%{channel}",
		Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	}

	transactionCountOpts = metrics.CounterOpts{
		Namespace:    "ledger",
		Subsystem:    "",
		Name:         "transaction_count",
		Help:         "Number of transactions processed.",
		LabelNames:   []string{"channel", "transaction_type", "chaincode", "validation_code"},
		StatsdFormat: "%{#fqname}.%{channel}.%{transaction_type}.%{chaincode}.%{validation_code}",
	}
)
