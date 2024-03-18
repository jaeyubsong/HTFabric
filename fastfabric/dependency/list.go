package dependency

import (
	"fmt"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/fastfabric/dependency/deptype"
)

type TxNode struct {
	elements   map[string]*TxElement
	next       *TxNode
	prev       *TxNode
	skipNext   *TxNode
	skipPrev   *TxNode
	rangeStart *version.Height
	rangeEnd   *version.Height
	isWrite    bool
}

type TxElement struct {
	*deptype.Transaction
	parentNode *TxNode
}

type skipList struct {
	key         string
	root        *TxNode
	len         int
	txs         map[string][]*TxElement
	appendCount uint
	removeCount uint
}

func NewSkipList(key string) *skipList {
	return &skipList{key: key, len: 0, txs: map[string][]*TxElement{}, root: &TxNode{
		rangeStart: version.NewHeight(0, 0),
		rangeEnd:   version.NewHeight(0, 0),
		isWrite:    true,
	}}
}

func (l *skipList) Delete(txID string) {
	elements, ok := l.txs[txID]
	if !ok {
		return
	}
	for _, element := range elements {
		l.removeCount++
		node := element.parentNode
		node.removeElement(element)
		if len(node.elements) == 0 {
			node.removeFromSkip()
			l.removeNode(node)
		}

		l.len -= 1
	}
	delete(l.txs, txID)
}

func (n *TxNode) removeElement(e *TxElement) {
	if n.next != nil {
		for _, e := range n.next.elements {
			e.RemoveDependency(e.Transaction)
		}
	}
	e.parentNode = nil
	delete(n.elements, e.TxID)
}

func (n *TxNode) removeFromSkip() {
	if n.skipNext == n.next {
		linkSkip(n.skipPrev, n.next)
	} else {
		n.next = n.substituteSkipWith(n.next)
	}
}

func (l *skipList) removeNode(node *TxNode) {
	link(node.prev, node.next)

	if node.prev == nil {
		l.root = node.next
	}
}

func (l *skipList) AddRead(tx *deptype.Transaction) {
	l.add(tx, false)
}

func (l *skipList) AddWrite(tx *deptype.Transaction) {
	l.add(tx, true)

}
func (l *skipList) Clear() {
	for x := l.root; x != nil; x = x.next {
		x.elements = nil
	}
	for i := range l.txs {
		l.txs[i] = nil
	}
}

func (l *skipList) debug() {
	fmt.Printf("***** %s (%d / %d) *****\n", l.key, l.appendCount, l.removeCount)
	temp := map[string]bool{}
	limit := 20
	for x := l.root; x != nil; x = x.next {
		if _, ok := temp[x.Key()]; ok {
			break
		}
		temp[x.Key()] = true
		if len(x.elements) < 1 {
			continue
		}
		limit--
		if limit < 1 {
			fmt.Println("TL;DR")
			break
		}
		pk := ""
		nk := ""
		if x.prev != nil {
			pk = x.prev.Key()
		}
		if x.next != nil {
			nk = x.next.Key()
		}
		fmt.Printf("[%s] <- [%s] (%d) -> [%s]\n", pk, x.Key(), len(x.elements), nk)
		// for _, y := range x.elements {
		// 	y.debug()
		// }
	}
}

// func (tx *Transaction) debug() {
// 	fmt.Printf("Tx(%5d %5d)\n", tx.Version.BlockNum, tx.Version.TxNum)
// 	for _, v := range tx.RwSet.NsRwSets {
// 		for _, w := range v.KvRwSet.Reads {
// 			fmt.Printf("R(%6s) ", w.Key)
// 		}
// 		for _, w := range v.KvRwSet.Writes {
// 			fmt.Printf("W(%6s) ", w.Key)
// 		}
// 		fmt.Print("\n")
// 	}
// }

func (l *skipList) add(tx *deptype.Transaction, isWrite bool) {
	// var nodeMap = map[string]bool{}

	// JNOTE 무한 루프 발생 지점
	// fmt.Printf("트랜잭션 (%5d %5d) (%v)\n", tx.Version.BlockNum, tx.Version.TxNum, isWrite)
	// for node := l.root; node != nil; node = node.skipNext {
	// 	key := node.Key()

	// 	if _, ok := nodeMap[key]; ok {
	// 		l.debug()
	// 		fmt.Println("▼ 주인공")
	// 		tx.debug()
	// 		panic("순환 참조")
	// 	}
	// 	nodeMap[key] = true
	// fmt.Printf("노드 #%s (W: %v) -----\n", key, node.isWrite)
	// if node.skipNext != nil {
	// 	fmt.Printf("----> %s\n", node.skipNext.Key())
	// } else {
	// 	fmt.Printf("-----\n")
	// }
	// }
	l.appendCount++

	skipClosest, closest := l.findClosestNode(tx)
	var e *TxElement
	if isWrite {
		// if l.key == constructCompositeKey("InfoCC", "1") {
		// 	fmt.Println("<<BEFORE W>>")
		// 	l.debug(closest)
		// }
		e = closest.appendNewNode(tx, isWrite)
		// if l.key == constructCompositeKey("InfoCC", "1") {
		// 	fmt.Println("<<AFTER W>>")
		// 	l.debug(closest)
		// }
	} else {
		switch {
		case !closest.isWrite:
			e = closest.add(tx)
			break
		case closest.next != nil && !closest.next.isWrite:
			e = closest.next.add(tx)
			break
		default:
			// if l.key == constructCompositeKey("InfoCC", "1") {
			// 	fmt.Println("<<BEFORE R>>")
			// 	l.debug(closest)
			// }
			e = closest.appendNewNode(tx, isWrite)
			// if l.key == constructCompositeKey("InfoCC", "1") {
			// 	fmt.Println("<<AFTER R>>")
			// 	l.debug(closest)
			// }
		}
	}
	skipClosest.updateSkips(e.parentNode)
	e.addDependencies()

	l.len += 1
	l.txs[tx.TxID] = append(l.txs[tx.TxID], e)
}

func (n *TxNode) substituteSkipWith(newNode *TxNode) *TxNode {
	// JNOTE newNode(n.next)의 버전이 n.skipNext의 버전보다 최신이기 때문에 순환이 생기는 것으로 보인다.
	if newNode != nil && n.skipNext != nil && newNode.rangeStart.Compare(n.skipNext.rangeStart) > 0 {
		// fmt.Printf("substituteSkipWith: %s by (%5d %5d)\n", newNode.Key(), by.Version.BlockNum, by.Version.TxNum)
		// panic("!!!")
		newNode, n.skipNext = n.skipNext, newNode
	}
	linkSkip(n.skipPrev, newNode)
	linkSkip(newNode, n.skipNext)
	n.skipNext = nil
	n.skipPrev = nil

	return newNode
}

func (this *TxElement) addDependencies() {
	n := this.parentNode
	if n.next != nil {
		for _, nextTx := range n.next.elements {
			nextTx.AddDependency(this.Transaction)
		}
	}
	if n.prev != nil {
		for _, prevTx := range n.prev.elements {
			this.AddDependency(prevTx.Transaction)
		}
	}
}

func (node *TxNode) Key() string {
	if node == nil {
		return "(nil)"
	}
	var t = "R"

	if node.isWrite {
		t = "W"
	}
	return fmt.Sprintf("(%s %5d %5d ~ %5d %5d)", t, node.rangeStart.BlockNum, node.rangeStart.TxNum, node.rangeEnd.BlockNum, node.rangeEnd.TxNum)
}

func (l *skipList) findClosestNode(tx *deptype.Transaction) (skipClosest, closest *TxNode) {
	skipClosest = l.root
	for skipClosest != nil && skipClosest.skipNext != nil && skipClosest.skipNext.rangeStart.Compare(tx.Version) <= 0 {
		skipClosest = skipClosest.skipNext
	}

	closest = skipClosest
	for closest != nil && closest.next != nil && closest.next.rangeStart.Compare(tx.Version) <= 0 {
		closest = closest.next
	}

	return skipClosest, closest
}

func (l *skipList) First() *TxNode {
	var dumbStart *TxNode
	var count = 0

	for x := l.root; x.next != nil; x = x.next {
		if len(x.next.elements) == 0 {
			dumbStart = x.next
			count++
			continue
		}
		if dumbStart != nil {
			link(l.root, x.next)
			linkSkip(l.root, x.next)
			fmt.Printf("??? %d [%s] <- [%s] <- [%s]\n", count, x.prev.Key(), x.prev.Key(), x.Key())
		}
		return x.next
	}
	return nil
}

func (n *TxNode) appendNewNode(tx *deptype.Transaction, isWrite bool) *TxElement {
	newElem := &TxElement{Transaction: tx}
	newNode := &TxNode{
		elements:   map[string]*TxElement{tx.TxID: newElem},
		rangeStart: tx.Version,
		rangeEnd:   tx.Version,
		isWrite:    isWrite}
	newElem.parentNode = newNode

	if n.rangeEnd.Compare(newNode.rangeStart) <= 0 {
		link(newNode, n.next)
		link(n, newNode)
	} else {
		n1, n2 := n.split(newNode.rangeStart)
		link(n1, newNode)
		link(newNode, n2)
	}

	return newElem
}

func (n *TxNode) add(tx *deptype.Transaction) *TxElement {
	newElem := &TxElement{Transaction: tx, parentNode: n}
	n.elements[tx.TxID] = newElem
	if n.rangeStart.Compare(tx.Version) > 0 {
		n.rangeStart = tx.Version
	}
	if n.rangeEnd.Compare(tx.Version) < 0 {
		n.rangeEnd = tx.Version
	}
	return newElem
}

func (n *TxNode) updateSkips(newNode *TxNode) {
	switch {
	case n.rangeStart.BlockNum == newNode.rangeStart.BlockNum:
		return
	case n.skipNext == nil:
		linkSkip(n, newNode)
		return
	case n.skipNext.rangeStart.BlockNum == newNode.rangeStart.BlockNum:
		if newNode != n.skipNext.substituteSkipWith(newNode) {
			panic("newNode != substituteSkipWith")
		}
		return
	default:
		linkSkip(newNode, n.skipNext)
		linkSkip(n, newNode)
	}
}

func (n *TxNode) split(height *version.Height) (*TxNode, *TxNode) {
	newNode := &TxNode{
		next:     n.next,
		prev:     n,
		rangeEnd: n.rangeEnd,
		elements: make(map[string]*TxElement),
		isWrite:  n.isWrite}

	n.rangeEnd = nil
	for txId, e := range n.elements {
		if e.Version.Compare(height) > 0 {
			newNode.elements[txId] = e
			e.parentNode = newNode
			if newNode.rangeStart == nil || newNode.rangeStart.Compare(e.Version) > 0 {
				newNode.rangeStart = e.Version
			}
		} else {
			if n.rangeEnd == nil || n.rangeEnd.Compare(e.Version) < 0 {
				n.rangeEnd = e.Version
			}
		}
	}

	for txId, _ := range newNode.elements {
		delete(n.elements, txId)
	}

	return n, newNode
}

func (this *TxNode) setPrev(prev *TxNode) {
	defer func() { this.prev = prev }()
	if prev != nil {
		for _, thisElement := range this.elements {
			newDependencies := map[string]*deptype.Transaction{}
			for _, prevElement := range prev.elements {
				if prevElement.TxID == thisElement.TxID {
					return
				}
				newDependencies[prevElement.TxID] = prevElement.Transaction
			}
			thisElement.Dependencies = newDependencies
		}

	}
}

func linkSkip(n1 *TxNode, n2 *TxNode) {
	if n1 != nil {
		n1.skipNext = n2
	}
	if n2 != nil {
		n2.skipPrev = n1
	}
}

func link(n1 *TxNode, n2 *TxNode) {
	if n1 != nil {
		n1.next = n2
	}
	if n2 != nil {
		n2.setPrev(n1)
	}
}
