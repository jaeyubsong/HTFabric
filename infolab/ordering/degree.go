package ordering

import (
	"fmt"
	"sort"
	"time"

	"github.com/hyperledger/fabric/protos/common"
)

type degreeIndexCell struct {
	index   int
	degreeR uint32
	degreeW uint32
}
type byDegree []*degreeIndexCell

func (a byDegree) Len() int      { return len(a) }
func (a byDegree) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// WARD
func (a byDegree) Less(i, j int) bool {
	if a[i].degreeW != a[j].degreeW {
		return a[i].degreeW < a[j].degreeW
	}
	return a[i].degreeR > a[j].degreeR
}

// RDWA
// func (a byDegree) Less(i, j int) bool {
// 	if a[i].degreeR != a[j].degreeR {
// 		return a[i].degreeR > a[j].degreeR
// 	}
// 	return a[i].degreeW < a[j].degreeW
// }

// WA+RD
// func (a byDegree) Less(i, j int) bool {
// 	if a[i].degreeW - a[i].degreeR != a[j].degreeW - a[j].degreeR {
// 		return a[i].degreeW < a[j].degreeW
// 	}
// 	return a[i].degreeW - a[i].degreeR < a[j].degreeW - a[j].degreeR
// }

var OrderByDegreeCount int64
var totalOrderByDegreeCountTime int64
var tableOrderByDegreeCountTime int64


// OrderByDegree 함수는 degree-asc 방식으로 트랜잭션들을 정렬해 반환한다.
func OrderByDegree(batch []*common.Envelope) []*common.Envelope {
	OrderByDegreeCount++
	startTime := time.Now()
	var R = make([]*common.Envelope, len(batch))
	var cells = make([]*degreeIndexCell, len(R))

	for index, envelope := range batch {
		r, w := getDegree(envelope)
		cells[index] = &degreeIndexCell{
			index:   index,
			degreeR: r,
			degreeW: w,
		}
	}
	sort.Sort(byDegree(cells))
	for i, v := range cells {
		R[i] = batch[v.index]
	}

	// fmt.Printf("batch의 길이: %d\nR의 길이: %d\n", len(batch), len(R))
	takenTime := time.Since(startTime)
	totalOrderByDegreeCountTime += takenTime.Nanoseconds()
	if OrderByDegreeCount == 3300 {
		avgTimeTaken := totalOrderByDegreeCountTime / 3000
		fmt.Printf("** OrderByDegree Average is: %d\n", avgTimeTaken)
	}

	if false {
		fmt.Printf("[OrderByDegree %d] Time taken: %d\n", OrderByDegreeCount, takenTime.Nanoseconds())
		if OrderByDegreeCount >= 301 {
			totalOrderByDegreeCountTime += takenTime.Nanoseconds()
		}
		if OrderByDegreeCount == 3300 {
			avgTimeTaken := totalOrderByDegreeCountTime / 3000
			fmt.Printf("** OrderByDegree Average is: %d\n", avgTimeTaken)
		}
	}
	return R
}

func OrderByDegreeParallel(batch []*common.Envelope, localReadDegreeTable map[string]uint32, localWriteDegreeTable map[string]uint32) []*common.Envelope {
	OrderByDegreeCount++
	startTime := time.Now()
	var R = make([]*common.Envelope, len(batch))
	var cells = make([]*degreeIndexCell, len(R))

	for index, envelope := range batch {
		r, w := getDegreeParallel(envelope, localReadDegreeTable, localWriteDegreeTable)
		cells[index] = &degreeIndexCell{
			index:   index,
			degreeR: r,
			degreeW: w,
		}
	}
	if true {
		stepOneTime := time.Since(startTime)
		fmt.Printf("[OrderByDegree %d] Time taken: %d\n", OrderByDegreeCount, stepOneTime.Nanoseconds())
		if OrderByDegreeCount >= 301 {
			tableOrderByDegreeCountTime += stepOneTime.Nanoseconds()
		}
		if OrderByDegreeCount >= 301 {
			avgTimeTaken := tableOrderByDegreeCountTime / (OrderByDegreeCount - 300)
			fmt.Printf("** [1/2] OrderByDegree Average is: %d\n", avgTimeTaken)
		}
	}
	sort.Sort(byDegree(cells))
	for i, v := range cells {
		R[i] = batch[v.index]
	}
	// fmt.Printf("batch의 길이: %d\nR의 길이: %d\n", len(batch), len(R))
	takenTime := time.Since(startTime)
	if true {
		fmt.Printf("[OrderByDegree %d] Time taken: %d\n", OrderByDegreeCount, takenTime.Nanoseconds())
		if OrderByDegreeCount >= 301 {
			totalOrderByDegreeCountTime += takenTime.Nanoseconds()
		}
		if OrderByDegreeCount >= 301 {
			avgTimeTaken := totalOrderByDegreeCountTime / (OrderByDegreeCount - 300)
			fmt.Printf("** [2/2] OrderByDegree Average is: %d\n", avgTimeTaken)
		}
	}
	return R
}
