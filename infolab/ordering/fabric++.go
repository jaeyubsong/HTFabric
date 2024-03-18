package ordering

import (
	"container/heap"
	"fmt"
	"time"

	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/protos/common"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"
)

type internalGraph map[int][]int
type stack struct {
	list []int
}
type heapItem struct {
	index int
	count int
}
type maxHeap []*heapItem
type vectorMap map[int][2][]uint64

// OrderByFabricPP 함수는 Fabric++ 방식으로 트랜잭션들을 정렬해 반환한다.
func OrderByFabricPP(m metrics.Histogram, batch []*common.Envelope) []*common.Envelope {
	var R []*common.Envelope
	var transactionsInCycles = &maxHeap{}
	var cyclesOfTransaction = map[int][]int{}

	// STEP 1
	fmt.Println("STEP 1")
	timer := time.Now()
	var conflictGraph = buildConflictGraph(batch)
	m.With("step", "1").Observe(time.Since(timer).Seconds())

	// STEP 2
	// var conflictSubgraphs = divideIntoSubgraphs(conflictGraph)

	// heap.Init(transactionsInCycles)
	// for i, g := range conflictSubgraphs {
	// 	fmt.Println(i)
	// 	cycles = append(cycles, g.getAllCycles()...)
	// }

	// STEP 3
	fmt.Println("STEP 3")
	var cycles [][]graph.Node

	cyclesChan := make(chan [][]graph.Node, 1)
	go func() {
		cyclesChan <- topo.DirectedCyclesIn(conflictGraph)
	}()
	timer = time.Now()
	select {
	case data := <-cyclesChan:
		m.With("step", "3").Observe(time.Since(timer).Seconds())
		cycles = data
	case <-time.After(5 * time.Second):
		m.With("step", "3").Observe(time.Since(timer).Seconds())
		fmt.Println("시간 초과. 재정렬 없음.")
		return batch
	}
	for cycleIndex, cycle := range cycles {
		for _, node := range cycle {
			index := int(node.ID())
			cyclesOfTransaction[index] = append(cyclesOfTransaction[index], cycleIndex)
		}
	}
	// for cycleIndex, cycle := range cycles {
	// 	for _, transactionIndex := range cycle {
	// 		cyclesOfTransaction[transactionIndex] = append(cyclesOfTransaction[transactionIndex], cycleIndex)
	// 	}
	// }
	for k, v := range cyclesOfTransaction {
		heap.Push(transactionsInCycles, &heapItem{
			index: k,
			count: len(v),
		})
	}

	// STEP 4
	fmt.Println("STEP 4")
	timer = time.Now()
	for len(cycles) > 0 {
		var victim = heap.Pop(transactionsInCycles).(*heapItem)

		batch[victim.index] = nil
		for i := 0; i < len(cycles); i++ {
			var cycle = cycles[i]
			var found = false
			var nodeIndex int
			var node graph.Node

			for nodeIndex, node = range cycle {
				if victim.index == int(node.ID()) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
			cycle = append(cycle[:nodeIndex], cycle[nodeIndex+1:]...)
			cycles = append(cycles[:i], cycles[i+1:]...)
			i--
			for _, node := range cycle {
				if index := transactionsInCycles.indexOf(int(node.ID())); index != -1 {
					(*transactionsInCycles)[index].count--
					heap.Fix(transactionsInCycles, index)
				}
			}
		}
	}
	m.With("step", "4").Observe(time.Since(timer).Seconds())

	// STEP 5
	fmt.Println("STEP 5")
	timer = time.Now()
	filteredBatch := []*common.Envelope{}
	for _, v := range batch {
		if v != nil {
			filteredBatch = append(filteredBatch, v)
		}
	}
	finalLength := len(filteredBatch)
	parentTable := map[int][]int{}
	scheduled := map[int]bool{}
	conflictGraph = buildConflictGraph(filteredBatch)
	edges := conflictGraph.Edges()
	for edge := edges.Edge(); edges.Next(); edge = edges.Edge() {
		if edge == nil {
			break
		}
		var from = int(edge.From().ID())
		var to = int(edge.To().ID())

		parentTable[to] = append(parentTable[to], from)
	}
	R = make([]*common.Envelope, finalLength)
	inputCount := 0
	currentNode := 0
	for inputCount < finalLength {
		var addNode = true

		if value, ok := scheduled[currentNode]; ok && value {
			currentNode = (currentNode + 1) % finalLength
			continue
		}
		for _, parent := range parentTable[currentNode] {
			if value, ok := scheduled[parent]; ok && value {
				continue
			}
			currentNode = parent
			addNode = false
			break
		}
		if !addNode {
			continue
		}
		scheduled[currentNode] = true
		inputCount++
		R[finalLength-inputCount] = filteredBatch[currentNode]
		edges.Reset()
		for edge := edges.Edge(); edges.Next(); edge = edges.Edge() {
			if edge == nil {
				break
			}
			if edge.From().ID() != int64(currentNode) {
				continue
			}
			child := int(edge.To().ID())
			if value, ok := scheduled[child]; !ok || !value {
				currentNode = child
				break
			}
		}
	}
	m.With("step", "5").Observe(time.Since(timer).Seconds())
	fmt.Printf("트랜잭션 %d개 중 %d개 유실됨\n", len(batch), len(batch)-finalLength)
	return R
}

func buildConflictGraph(batch []*common.Envelope) *simple.DirectedGraph {
	var R = simple.NewDirectedGraph()
	var table = make(vectorMap)
	var length = len(batch)

	for i, envelope := range batch {
		table[i] = getKeyVector(envelope)
	}
	for i := 0; i < length; i++ {
		for j := 0; j < length; j++ {
			if i == j {
				continue
			}
			if table.hasConflict(i, j) {
				R.SetEdge(simple.Edge{F: simple.Node(j), T: simple.Node(i)})
			}
		}
	}
	fmt.Printf("정점 수: %d, 간선 수: %d\n", R.Nodes().Len(), R.Edges().Len())
	return R
}

// func divideIntoSubgraphs(g internalGraph) []internalGraph {
// 	var R = []internalGraph{}
// 	var S = &stack{list: []int{}}
// 	var dfsOrder = map[int]int{}
// 	var finished = map[int]bool{}

// 	var orderCounter = 0
// 	var dfs func(node int) int

// 	dfs = func(node int) int {
// 		orderCounter++
// 		S.push(node)
// 		dfsOrder[node] = orderCounter
// 		minOrder := dfsOrder[node]
// 		for _, v := range g[node] {
// 			if dfsOrder[v] == 0 {
// 				order := dfs(v)
// 				if minOrder > order {
// 					minOrder = order
// 				}
// 			} else if !finished[v] {
// 				if minOrder > dfsOrder[v] {
// 					minOrder = dfsOrder[v]
// 				}
// 			}
// 		}
// 		if minOrder == dfsOrder[node] {
// 			subgraph := make(internalGraph)
// 			for {
// 				rear := S.pop()
// 				finished[rear] = true
// 				subgraph[rear] = g[rear]
// 				if rear == node {
// 					break
// 				}
// 			}
// 			R = append(R, subgraph)
// 		}
// 		return minOrder
// 	}
// 	for node := range g {
// 		if dfsOrder[node] == 0 {
// 			dfs(node)
// 		}
// 	}
// 	return R
// }

func values(data map[int]bool) []bool {
	var R = make([]bool, len(data))

	i := 0
	for _, v := range data {
		R[i] = v
		i++
	}
	return R
}

// func (g internalGraph) getAllCycles() []cycle {
// 	var R = []cycle{}
// 	var A = g
// 	var B = internalGraph{}
// 	var S = map[int]bool{}
// 	var s int
// 	var blocked = map[int]bool{}
// 	var circuit func(node int) bool
// 	var unblock func(node int)
// 	var contains = func(list []int, target int) bool {
// 		for _, v := range list {
// 			if v == target {
// 				return true
// 			}
// 		}
// 		return false
// 	}

// 	if len(g) <= 1 {
// 		return R
// 	}
// 	unblock = func(node int) {
// 		blocked[node] = false
// 		for _, v := range B[node] {
// 			if blocked[v] {
// 				unblock(v)
// 			}
// 		}
// 		B[node] = []int{}
// 	}
// 	circuit = func(node int) bool {
// 		var r = false

// 		S[node] = true
// 		blocked[node] = true
// 		for _, w := range A[node] {
// 			if w == s {
// 				list := []int{}
// 				for x := range S {
// 					list = append(list, x)
// 				}
// 				R = append(R, append(list, s))
// 				r = true
// 			} else if value, ok := blocked[w]; !ok || !value {
// 				if circuit(w) {
// 					r = true
// 				}
// 			}
// 		}
// 		if r {
// 			unblock(node)
// 		} else {
// 			for _, w := range A[node] {
// 				if !contains(B[w], node) {
// 					B[w] = append(B[w], node)
// 				}
// 			}
// 		}
// 		delete(S, node)
// 		return r
// 	}
// 	A.printComputable()
// 	s = A.getMinimumKey()
// 	circuit(s)

// 	fmt.Printf("len(R) = %d\n", len(R))
// 	if len(R) >= 100 {
// 		for i := 0; i < 100; i++ {
// 			fmt.Printf("[%3d] %+v\n", i, R[i])
// 		}
// 	}
// 	return R
// }

func (g internalGraph) printComputable() {
	fmt.Println(len(g))
	fmt.Println("-")
	for key := range g {
		fmt.Println(key)
	}
	for key, list := range g {
		for _, v := range list {
			fmt.Printf("%d %d\n", key, v)
		}
	}
}
func (g internalGraph) print() {
	for key, list := range g {
		fmt.Printf("[%6d]", key)
		for _, v := range list {
			fmt.Printf(" %6d", v)
		}
		fmt.Printf("\n")
	}
}
func (g internalGraph) getMinimumKey() int {
	var R int = 16384

	for k := range g {
		if R > k {
			R = k
		}
	}
	return R
}

func (s *stack) print() {
	fmt.Println("┌")
	for i, v := range s.list {
		fmt.Printf("│[%6d] %6d\n", i, v)
	}
	fmt.Println("└")
}
func (s *stack) push(value int) {
	s.list = append(s.list, value)
}
func (s *stack) pop() int {
	var rear = len(s.list) - 1
	var R = s.list[rear]

	s.list = s.list[:rear]
	return R
}

func (h maxHeap) print() {
	for i, v := range h {
		fmt.Printf("[%d] #%d (%d)\n", i, v.index, v.count)
	}
}
func (h maxHeap) indexOf(transactionIndex int) int {
	for i, v := range h {
		if v.index == transactionIndex {
			return i
		}
	}
	return -1
}
func (h maxHeap) Len() int {
	return len(h)
}
func (h maxHeap) Less(i, j int) bool {
	if h[i].count == h[j].count {
		return h[i].index < h[j].index
	}
	return h[i].count > h[j].count
}
func (h maxHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *maxHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}
func (h *maxHeap) Pop() interface{} {
	var old = *h
	var n = len(old)
	var x = old[n-1]

	*h = old[:n-1]

	return x
}

func (table vectorMap) hasConflict(a, b int) bool {
	var vectorA = table[a][0]
	var vectorB = table[b][1]

	for i := 0; i < keyVectorLength; i++ {
		if vectorA[i]&vectorB[i] != 0 {
			return true
		}
	}
	return false
}
