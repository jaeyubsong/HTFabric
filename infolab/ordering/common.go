package ordering

import (
	"strconv"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/infolab/partitioning"
	"github.com/hyperledger/fabric/protos/common"
	protoutil "github.com/hyperledger/fabric/protos/utils"
)

const keyVectorLength = 16384 / 64

var logger = flogging.MustGetLogger("infolab.ordering")

func getDegree(envelope *common.Envelope) (R uint32, W uint32) {
	action, err := protoutil.GetActionFromEnvelopeMsg(envelope)
	if err != nil {
		logger.Error("GetActionFromEnvelopeMsg: " + err.Error())
		return R, W
	}
	for _, metadata := range action.Response.KeyMetadataList {
		var d, ok = partitioning.DegreeTable[string(metadata.Name)]

		if !ok {
			d = 0
		}
		if metadata.Writing {
			W += d
		} else {
			R += d
		}
	}
	return R, W
}

func getDegreeParallel(envelope *common.Envelope, localReadDegreeTable map[string]uint32, localWriteDegreeTable map[string]uint32) (R uint32, W uint32) {
	action, err := protoutil.GetActionFromEnvelopeMsg(envelope)
	if err != nil {
		logger.Error("GetActionFromEnvelopeMsg: " + err.Error())
		return R, W
	}
	for _, metadata := range action.Response.KeyMetadataList {
		if metadata.Writing {
			var d, ok = localReadDegreeTable[string(metadata.Name)]
			if !ok {
				d = 0
			}
			W += d
		} else {
			var d, ok = localWriteDegreeTable[string(metadata.Name)]
			if !ok {
				d = 0
			}
			R += d
		}
	}
	return R, W
}

func getKeyGroups(envelope *common.Envelope) ([]string, []string) {
	var nonhubs = []string{}
	var hubs = []string{}

	action, err := protoutil.GetActionFromEnvelopeMsg(envelope)
	if err != nil {
		logger.Error("GetActionFromEnvelopeMsg: " + err.Error())
		return nil, nil
	}
	for _, metadata := range action.Response.KeyMetadataList {
		// if metadata.Hubness == peer.Hubness_HUB {
		// 	hubs = append(hubs, string(metadata.Name))
		// } else {
		nonhubs = append(nonhubs, string(metadata.Name))
		// }
	}
	return nonhubs, hubs
}
func getKeyVector(envelope *common.Envelope) [2][]uint64 {
	var R = [2][]uint64{
		make([]uint64, keyVectorLength),
		make([]uint64, keyVectorLength),
	}

	action, err := protoutil.GetActionFromEnvelopeMsg(envelope)
	if err != nil {
		logger.Error("GetActionFromEnvelopeMsg: " + err.Error())
		return R
	}
	for _, metadata := range action.Response.KeyMetadataList {
		value, err := strconv.ParseUint(string(metadata.Name), 10, 64)
		if err != nil {
			logger.Error("ParseUint: " + err.Error())
			return R
		}
		nodap := (value / 64) % keyVectorLength
		if metadata.Writing {
			R[1][nodap] |= 1 << (value % 64)
		} else {
			R[0][nodap] |= 1 << (value % 64)
		}
	}
	return R
}
