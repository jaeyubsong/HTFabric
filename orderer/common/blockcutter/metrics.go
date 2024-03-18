/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockcutter

import "github.com/hyperledger/fabric/common/metrics"

var (
	blockFillDuration = metrics.HistogramOpts{
		Namespace:    "blockcutter",
		Name:         "block_fill_duration",
		Help:         "The time from first transaction enqueing to the block being cut in seconds.",
		LabelNames:   []string{"channel"},
		StatsdFormat: "%{#fqname}.%{channel}",
	}
	blockRearrangeDuration = metrics.HistogramOpts{
		Namespace:    "blockcutter",
		Name:         "block_rearrange_duration",
		Help:         "블록이 Fabric++ 방법으로 재정렬되는데 걸린 시간 (초)",
		LabelNames:   []string{"step"},
		StatsdFormat: "%{#fqname}.%{step}",
	}
)

type Metrics struct {
	BlockFillDuration      metrics.Histogram
	BlockRearrangeDuration metrics.Histogram
}

func NewMetrics(p metrics.Provider) *Metrics {
	return &Metrics{
		BlockFillDuration:      p.NewHistogram(blockFillDuration),
		BlockRearrangeDuration: p.NewHistogram(blockRearrangeDuration),
	}
}
