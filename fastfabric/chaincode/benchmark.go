package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/protos/peer"
)

type InfoCC struct {
}

func (cc *InfoCC) Init(stub shim.ChaincodeStubInterface) peer.Response {
	if err := stub.PutState("MASTER", []byte("JJORIPING")); err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(nil)
}

func (cc *InfoCC) Invoke(stub shim.ChaincodeStubInterface) peer.Response {
	var f, args = stub.GetFunctionAndParameters()
	var R = ""

	switch f {
	case "createAccount":
		return createAccount(stub, args)
	case "send":
		switch len(args) {
		case 3:
			return send(stub, 2, args)
		case 5:
			return send(stub, 4, args)
		case 7:
			return send(stub, 6, args)
		}
	case "send1":
		return sendOne(stub, args)
	case "send2":
		return send(stub, 2, args)
	case "send4":
		return send(stub, 4, args)
	case "send6":
		return send(stub, 6, args)
	case "send8":
		return send(stub, 8, args)
	case "query":
		return query(stub, args)
	case "readwrite4":
		return readwrite(stub, 4, args)
	case "readwrite8":
		return readwrite(stub, 8, args)
	case "fabric++":
		return sendFabricPP(stub, args)
	case "ping":
		R = "pong"
	}
	if R == "" {
		return shim.Error(fmt.Sprintf("[ERR] createAccount, send, send2, send4, send6, fabric++, query, ping. (Got %s)", f))
	}
	return shim.Success([]byte(R))
}

func createAccount(stub shim.ChaincodeStubInterface, args []string) peer.Response {
	// Initialize the chaincode
	var err error
	firstAccount, err := strconv.Atoi(args[0])
	accountCount, err := strconv.Atoi(args[1])
	if err != nil {
		return shim.Error("Expecting integer value for number of accounts")
	}
	initValue, err := strconv.Atoi(args[2])
	if err != nil {
		return shim.Error("Expecting integer value for default account value")
	}
	// Write the state to the ledger

	for i := firstAccount; i < firstAccount+accountCount; i++ {
		err = stub.PutState(strconv.Itoa(i), []byte(strconv.Itoa(initValue)))
		if err != nil {
			return shim.Error(err.Error())
		}
	}

	return shim.Success(nil)
}

func main() {
	if err := shim.Start(new(InfoCC)); err != nil {
		panic("[ERR] Starting InfoCC: " + err.Error())
	}
	fmt.Println("InfoCC 시작")
}
func add(target []byte, amount int) []byte {
	var value, err = strconv.Atoi(string(target))

	if err != nil {
		return []byte("NaN")
	}
	return []byte(strconv.Itoa(value + amount))
}

func sendOne(stub shim.ChaincodeStubInterface, args []string) peer.Response {
	var v []byte
	var err error

	if v, err = stub.GetState(args[0]); err != nil {
		return shim.Error(fmt.Sprintf("[ERR] GetState: %s", err))
	}

	if err := stub.PutState(args[0], add(v, 1)); err != nil {
		return shim.Error(fmt.Sprintf("[ERR] PutState: %s", err))
	}

	return shim.Success([]byte("OK"))
}


func send(stub shim.ChaincodeStubInterface, level int, args []string) peer.Response {
	if len(args) != level+1 {
		return shim.Error("[ERR] send <FROM> <TO...> <AMOUNT>")
	}
	var values = map[int][]byte{}
	var amount int
	var err error

	if amount, err = strconv.Atoi(args[level]); err != nil {
		return shim.Error("[ERR] Atoi: " + args[level])
	}
	for i := 0; i < level; i++ {
		var v []byte

		if v, err = stub.GetState(args[i]); err != nil {
			return shim.Error(fmt.Sprintf("[ERR] GetState(%d): %s", i, err))
		}
		if v == nil {
			v = []byte{'0'}
		}
		values[i] = v
	}
	values[0] = add(values[0], -amount*(level-1))
	for i := 1; i < level; i++ {
		values[i] = add(values[i], amount)
	}
	for i := 0; i < level; i++ {
		if err := stub.PutState(args[i], values[i]); err != nil {
			return shim.Error(fmt.Sprintf("[ERR] PutState(%d): %s", i, err))
		}
	}
	return shim.Success([]byte("OK"))
}

func readwrite(stub shim.ChaincodeStubInterface, level int, args []string) peer.Response {
	var err error
	var values = map[int][]byte{}
	for i := 0; i < level; i++ {
		var v []byte

		if v, err = stub.GetState(args[i]); err != nil {
			return shim.Error(fmt.Sprintf("[ERR] GetState(%d): %s", i, err))
		}
		if v == nil {
			v = []byte{'0'}
		}
		values[i] = v
	}

	for i := level; i < level * 2; i++ {
		if err := stub.PutState(args[i], values[i-level]); err != nil {
			return shim.Error(fmt.Sprintf("[ERR] PutState(%d): %s", i, err))
		}
	}
	return shim.Success([]byte("OK"))
}


func sendFabricPP(stub shim.ChaincodeStubInterface, args []string) peer.Response {
	if len(args) != 1 {
		return shim.Error("[ERR] sendFabricPP <INDEX>")
	}
	const N = 10000
	const RW = 4
	const HR = 0.4
	const HW = 0.1
	const HSS = 0.01
	const HH = int(N * HSS)
	var argTable = map[string]bool{}
	var err error
	// var index, err = strconv.Atoi(args[0])
	var random = rand.New(rand.NewSource(time.Now().UnixNano()))

	if err != nil {
		return shim.Error("[ERR] Atoi: " + args[0])
	}
	for i := 0; i < RW; i++ {
		for {
			var key string

			if i < RW/2 {
				if random.Float64() < HR {
					key = fmt.Sprint(random.Intn(HH))
				} else {
					key = fmt.Sprint(HH + random.Intn(N-HH))
				}
			} else {
				if random.Float64() < HW {
					key = fmt.Sprint(random.Intn(HH))
				} else {
					key = fmt.Sprint(HH + random.Intn(N-HH))
				}
			}
			if _, ok := argTable[key]; ok {
				continue
			}
			argTable[key] = true
			if i < RW/2 {
				if _, err = stub.GetState(key); err != nil {
					return shim.Error(fmt.Sprintf("[ERR] GetState(%d): %s", i, err))
				}
			} else {
				if err = stub.PutState(key, []byte(fmt.Sprint(rand.Intn(100)))); err != nil {
					return shim.Error(fmt.Sprintf("[ERR] PutState(%d): %s", i, err))
				}
			}
			break
		}
	}
	return shim.Success([]byte("OK"))
}
func query(stub shim.ChaincodeStubInterface, args []string) peer.Response {
	if len(args) != 1 {
		return shim.Error("[ERR] query <KEY>")
	}
	var R []byte
	var err error

	if R, err = stub.GetState(args[0]); err != nil {
		return shim.Error(fmt.Sprintf("[ERR] GetState(%s): %s", args[0], err))
	}
	if R == nil {
		R = []byte("undefined")
	}
	return shim.Success(R)
}
