package main

import (
	"antidote"
	"bufio"
	"crdt"
	"fmt"
	rand "math/rand"
	"net"
	"os"
	"sort"
	"time"
	"tools"

	proto "github.com/golang/protobuf/proto"
)

const (
	testCrdtType = antidote.CRDTType_LWWREG
	bucket       = "bkt"
	maxId        = 20
	maxValue     = 2000
	targetTrans  = 20000
	//targetTrans = 30
	//writeProb = 0
	writeProb = 0.7
	//writeProb      = 1
	addProb        = 0.5
	minOpsPerTrans = 3
	maxOpsPerTrans = 10
	//maxOpsPerTrans = 4
	maxSleepTime = 50
	//maxSleepTime = 800
	nClients = 2
	//nClients         = 1
	beforeStartSleep = 2000
	//sleepBeforeVerify = 4000
	//Use smaller values only for single replica debugging
	//sleepBeforeVerify = 500
	sleepBeforeVerify = 6000
	isMax             = crdt.IS_MAX //Used for MaxMinCRDT
)

var (
	keys = []string{"topk1"}
	//keys = []string{"topk1", "topk2"}
	//keys = [4]string{"counter1", "counter2", "counter3", "counter4"}
	//buckets = [2][]byte{[]byte("bkt1"), []byte("bkt2")}
	//buckets = []string{"bkt", "bkt1", "bkt2"}
	buckets = []string{"bkt"}
	elems   = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	mapKeys = []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	servers = []string{"127.0.0.1:8087", "127.0.0.1:8088"}
	//servers = []string{"127.0.0.1:8087"}
	reader = bufio.NewReader(os.Stdin)
)

/*
Plan: execute "x" transactions, each one doing a certain number of updates and reads (maybe use chance?).
Give some random (but low) delay between transactions. Repeat if they get aborted.
*/

func main() {
	//connection, err := net.Dial("tcp", "127.0.0.1:8087")
	//tools.CheckErr("Network connection establishment err", err)

	rand.Seed(time.Now().UTC().UnixNano())
	//transactionCycle(connection)

	//for i := 0; i < 1; i++ {
	/*
		for i := 0; i < nClients; i++ {
			conn, err := net.Dial("tcp", servers[i%len(servers)])
			tools.CheckErr("Network connection establishment err", err)
			go transactionCycle(i, conn)
		}
		fmt.Println("Click enter once transactions stop happening.")
		reader.ReadString('\n')
	*/

	benchmark()
	//testSet(connection)
	//testStaticUpdate(connection)
	//time.Sleep(time.Duration(1000) * time.Millisecond)
	//testStaticRead(connection, antidote.CRDTType_COUNTER, 10)
}

func testStaticRead(connection net.Conn, crdtType antidote.CRDTType, nReads int) (receivedProto proto.Message) {
	reads := createReadObjectParams(crdtType, nReads)
	proto := antidote.CreateStaticReadObjs(reads)
	antidote.SendProto(antidote.StaticReadObjs, proto, connection)
	//fmt.Println("Proto sent! Waiting for reply.")

	//Wait for reply
	antidote.ReceiveProto(connection)
	//protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	//fmt.Println("Received type, proto: ", protoType, receivedProto)

	return
}

func testRead(connection net.Conn, transId []byte, crdtType antidote.CRDTType, nReads int) (receivedProto proto.Message) {
	reads := createReadObjectParams(crdtType, nReads)
	proto := antidote.CreateReadObjsFromArray(transId, reads)
	antidote.SendProto(antidote.ReadObjs, proto, connection)
	//fmt.Println("Proto sent! Waiting for reply.")

	//Wait for reply
	antidote.ReceiveProto(connection)
	//protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	//fmt.Println("Received type, proto: ", protoType, receivedProto)

	return
}

func createReadObjectParams(crdtType antidote.CRDTType, nReads int) (reads []antidote.ReadObjectParams) {
	reads = make([]antidote.ReadObjectParams, nReads)
	for i := 0; i < len(reads); i++ {
		rndKey, rndBucket := getRandomLocationParams()
		reads[i] = antidote.ReadObjectParams{
			KeyParams: antidote.CreateKeyParams(rndKey, crdtType, rndBucket),
		}
	}
	return
}

func testStaticUpdate(connection net.Conn) (receivedProto proto.Message) {
	updates := make([]antidote.UpdateObjectParams, 1)
	inc := rand.Int31n(100)
	for i := 0; i < len(updates); i++ {
		rndKey, rndBucket := getRandomLocationParams()
		fmt.Println("Incrementing with:", inc)
		updates[i] = antidote.UpdateObjectParams{
			KeyParams:  antidote.CreateKeyParams(rndKey, antidote.CRDTType_COUNTER, rndBucket),
			UpdateArgs: crdt.Increment{Change: inc},
		}
	}

	proto := antidote.CreateStaticUpdateObjs(updates)
	antidote.SendProto(antidote.StaticUpdateObjs, proto, connection)
	fmt.Println("Proto sent! Waiting for reply.")

	//Wait for reply
	protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Received type, proto: ", protoType, receivedProto)

	return
}

func testSet(connection net.Conn) {
	adds := make([]crdt.UpdateArguments, 3)
	for i := 0; i < len(adds); i++ {
		adds[i] = crdt.Add{Element: crdt.Element(fmt.Sprint(rand.Uint64()))}
	}
	testGenericStaticUpdate(connection, antidote.CRDTType_ORSET, adds)

	testStaticRead(connection, antidote.CRDTType_ORSET, 5)

	rems := make([]crdt.UpdateArguments, 6)
	for i := 0; i < len(rems); i++ {
		rems[i] = crdt.Remove{Element: crdt.Element(fmt.Sprint(rand.Uint64()))}
	}
	testGenericStaticUpdate(connection, antidote.CRDTType_ORSET, rems)

	testStaticRead(connection, antidote.CRDTType_ORSET, 5)

	addAll := make([]crdt.UpdateArguments, 1)
	var rndElems []crdt.Element
	for i := 0; i < len(addAll); i++ {
		rndElems = make([]crdt.Element, 5)
		for j := 0; j < len(rndElems); j++ {
			rndElems[j] = crdt.Element(fmt.Sprint(rand.Uint64()))
		}
		addAll[i] = crdt.AddAll{Elems: rndElems}
	}
	testGenericStaticUpdate(connection, antidote.CRDTType_ORSET, addAll)

	testStaticRead(connection, antidote.CRDTType_ORSET, 5)

	remAll := make([]crdt.UpdateArguments, 2)
	for i := 0; i < len(remAll); i++ {
		rndElems = make([]crdt.Element, 2)
		for j := 0; j < len(rndElems); j++ {
			rndElems[j] = crdt.Element(fmt.Sprint(rand.Uint64()))
		}
		remAll[i] = crdt.RemoveAll{Elems: rndElems}
	}
	testGenericStaticUpdate(connection, antidote.CRDTType_ORSET, remAll)

	testStaticRead(connection, antidote.CRDTType_ORSET, 5)
}

func testGenericStaticUpdate(connection net.Conn, crdtType antidote.CRDTType, args []crdt.UpdateArguments) {
	updates := createUpdateObjectParams(crdtType, args)
	proto := antidote.CreateStaticUpdateObjs(updates)
	antidote.SendProto(antidote.StaticUpdateObjs, proto, connection)
	//fmt.Println("Proto update sent! Waiting for reply.")

	//Wait for reply
	antidote.ReceiveProto(connection)
	//protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	//fmt.Println("Received type, proto: ", protoType, receivedProto)
}

func testGenericUpdate(connection net.Conn, transId []byte, crdtType antidote.CRDTType, args []crdt.UpdateArguments) {
	updates := createUpdateObjectParams(crdtType, args)
	proto := antidote.CreateUpdateObjsFromArray(transId, updates)
	antidote.SendProto(antidote.UpdateObjs, proto, connection)
	//fmt.Println("Proto update sent! Waiting for reply.")

	//Wait for reply
	antidote.ReceiveProto(connection)
	//protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	//fmt.Println("Received type, proto: ", protoType, receivedProto)
}

func createUpdateObjectParams(crdtType antidote.CRDTType, args []crdt.UpdateArguments) (updates []antidote.UpdateObjectParams) {
	updates = make([]antidote.UpdateObjectParams, len(args))
	for i := 0; i < len(args); i++ {
		rndKey, rndBucket := getRandomLocationParams()
		//fmt.Println("Generating update op to key, bucket", rndKey, rndBucket)
		updates[i] = antidote.UpdateObjectParams{
			KeyParams:  antidote.CreateKeyParams(rndKey, crdtType, rndBucket),
			UpdateArgs: args[i],
		}
	}
	return
}

func transactionCycle(id int, connection net.Conn) {
	fmt.Println("Sleeping a bit before starting...")
	time.Sleep(time.Duration(beforeStartSleep) * time.Millisecond)

	for nDone := 0; nDone < targetTrans; nDone++ {
		fmt.Println("Starting transaction...")
		//Send start transaction
		startTrans := antidote.CreateStartTransaction(nil)
		//reader.ReadString('\n')
		antidote.SendProto(antidote.StartTrans, startTrans, connection)

		//Receive transaction ID
		_, receivedProto, _ := antidote.ReceiveProto(connection)
		startTransResp := receivedProto.(*antidote.ApbStartTransactionResp)
		transId := startTransResp.GetTransactionDescriptor()

		fmt.Println("Starting to send operations...")
		createAndSendOps(connection, transId)

		fmt.Println("Sending commit...")
		commitTrans := antidote.CreateCommitTransaction(transId)
		antidote.SendProto(antidote.CommitTrans, commitTrans, connection)

		//Receive reply, check if it is commit or abort?
		_, receivedProto, _ = antidote.ReceiveProto(connection)
		commitReply := receivedProto.(*antidote.ApbCommitResp)
		if !commitReply.GetSuccess() {
			fmt.Println("Commit failed, retrying...")
			nDone--
			fmt.Println("(actually, for now, we're actually exiting...")
			os.Exit(1)
		} else {
			fmt.Println("Commit", nDone, "out of", targetTrans, "done.")
		}

		//Always sleep a bit
		toSleep := rand.Intn(maxSleepTime)
		fmt.Println("Sleeping for", toSleep, "ms")
		time.Sleep(time.Duration(toSleep) * time.Millisecond)
	}

	fmt.Println("Finish!")
	connection.Close()

	if id == 0 {
		verifyReplication(testCrdtType)
	}
}

func debugWithCounter(connection net.Conn, transId []byte) {
	counterKeys := [3]string{"counter1", "counter2", "counter3"}
	protoType, writeProto := antidote.UpdateObjs, createCounterWrite(transId, counterKeys[0])
	fmt.Println("Sending write for", counterKeys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), writeProto, connection)
	replyType, replyProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType, replyProto)

	protoType, readProto := antidote.ReadObjs, createRead(transId, counterKeys[0], antidote.CRDTType_COUNTER)
	fmt.Println("Sending read for", counterKeys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), readProto, connection)
	replyType1, replyProto1, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType1, replyProto1)

	protoType, readProto = antidote.ReadObjs, createRead(transId, counterKeys[1], antidote.CRDTType_COUNTER)
	fmt.Println("Sending read for", counterKeys[1])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), readProto, connection)
	replyType2, replyProto2, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType2, replyProto2)
	//reader.ReadString('\n')
}

func debugWithTopk(connection net.Conn, transId []byte) {

	protoType, writeProto := antidote.UpdateObjs, getNextWrite(transId, keys[0], antidote.CRDTType_TOPK)
	fmt.Println("Sending write for", keys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), writeProto, connection)
	replyType, replyProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType, replyProto)

	/*
		protoType, readProto := antidote.ReadObjs, getNextRead(transId, keys[1])
		fmt.Println("Sending read for", keys[1])
		//reader.ReadString('\n')
		antidote.SendProto(byte(protoType), readProto, connection)
		replyType1, replyProto1, _ := antidote.ReceiveProto(connection)
		fmt.Println("Reply type, proto:", replyType1, replyProto1)
	*/

	protoType, readProto := antidote.ReadObjs, getNextRead(transId, keys[0], antidote.CRDTType_TOPK)
	fmt.Println("Sending read for", keys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), readProto, connection)
	replyType2, replyProto2, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType2, replyProto2)
	//reader.ReadString('\n')

	/*
		fmt.Println("Sending commit")
		commitTrans := antidote.CreateCommitTransaction(transId)
		antidote.SendProto(antidote.CommitTrans, commitTrans, connection)
		fmt.Println("Commit sent.")
		_, receivedProto, _ := antidote.ReceiveProto(connection)
		commitReply := receivedProto.(*antidote.ApbCommitResp)
		fmt.Println("Commit success:", commitReply.GetSuccess())
	*/
}

func createAndSendOps(connection net.Conn, transId []byte) {
	nOps := rand.Intn(maxOpsPerTrans-minOpsPerTrans+1) + minOpsPerTrans
	for currOpN := 0; currOpN < nOps; currOpN++ {
		//fmt.Println("Step", currOpN, "out of", nOps)
		protoType, opProto := getNextOp(transId)
		//fmt.Println("Generated proto", opProto)
		//fmt.Println("Sending proto op")
		//reader.ReadString('\n')
		antidote.SendProto(protoType, opProto, connection)
		//fmt.Println("Receiving proto op")
		//replyType, replyProto, _ := antidote.ReceiveProto(connection)
		replyType, _, _ := antidote.ReceiveProto(connection)
		if replyType == antidote.ErrorReply {
			fmt.Println("Received ApbErrorResp!")
			os.Exit(1)
		}
		//fmt.Println("Received reply op proto", replyProto)
		//For now, ignore.
	}
	//fmt.Println("nOps done")
}

func getNextOp(transId []byte) (protoType byte, protoBuf proto.Message) {
	key := keys[rand.Intn(len(keys))]
	if rand.Float32() < writeProb {
		//fmt.Println("Next proto is a write.")
		protoType, protoBuf = antidote.UpdateObjs, getNextWrite(transId, key, testCrdtType)
	} else {
		//fmt.Println("Next proto is a read.")
		protoType, protoBuf = antidote.ReadObjs, getNextRead(transId, key, testCrdtType)
	}
	if protoBuf == nil {
		fmt.Println("Warning - nil protoBuf on getNextOp!")
	}
	return
}

func getNextWrite(transId []byte, key string, crdtType antidote.CRDTType) (updateBuf *antidote.ApbUpdateObjects) {
	var writeBuf proto.Message
	switch crdtType {
	case antidote.CRDTType_TOPK:
		writeBuf = antidote.CreateTopkUpdate(rand.Intn(maxId), rand.Intn(maxValue))

	case antidote.CRDTType_TOPK_RMV:
		rndPlayer, rndValue := rand.Intn(maxId), rand.Intn(maxValue)
		isAdd := rand.Float32() < 0.5
		writeBuf = antidote.CreateTopKRmvUpdate(isAdd, rndPlayer, rndValue)

	case antidote.CRDTType_ORSET:
		rndElem := elems[rand.Intn(len(elems))]
		if rand.Float32() < 0.5 {
			writeBuf = antidote.CreateSetUpdate(antidote.ApbSetUpdate_ADD, []string{rndElem})
		} else {
			writeBuf = antidote.CreateSetUpdate(antidote.ApbSetUpdate_REMOVE, []string{rndElem})
		}

	case antidote.CRDTType_COUNTER:
		//Negative inc as dec
		writeBuf = antidote.CreateCounterUpdate(rand.Intn(maxValue*2) - maxValue)

	case antidote.CRDTType_LWWREG:
		writeBuf = antidote.CreateRegisterUpdate(fmt.Sprint(rand.Intn(maxValue)))

	case antidote.CRDTType_RRMAP:
		key := mapKeys[rand.Intn(len(mapKeys))]
		value := crdt.Element(elems[rand.Intn(len(elems))])
		isAdd := rand.Float32() < 0.5
		if isAdd {
			writeBuf = antidote.CreateMapUpdate(isAdd, map[string]crdt.Element{key: value}, nil)
		} else {
			writeBuf = antidote.CreateMapUpdate(isAdd, nil, map[string]struct{}{key: struct{}{}})
		}

	case antidote.CRDTType_AVG:
		writeBuf = antidote.CreateAvgUpdate(rand.Int63n(maxValue), 1)

	case antidote.CRDTType_MAXMIN:
		writeBuf = antidote.CreateMaxMinUpdate(rand.Int63n(maxValue), isMax)
	}

	return antidote.CreateUpdateObjs(transId, key, crdtType, bucket, writeBuf)
}

func getNextRead(transId []byte, key string, crdtType antidote.CRDTType) (readBuf *antidote.ApbReadObjects) {
	return createRead(transId, key, crdtType)
}

func createRead(transId []byte, key string, crdtType antidote.CRDTType) (readBuf *antidote.ApbReadObjects) {
	return antidote.CreateReadObjs(transId, key, crdtType, bucket)
}

func createCounterWrite(transId []byte, key string) (updateBuf *antidote.ApbUpdateObjects) {
	return antidote.CreateUpdateObjs(transId, key, antidote.CRDTType_COUNTER, bucket, antidote.CreateCounterUpdate(5))
}

func getRandomLocationParams() (key string, bucket string) {
	return keys[rand.Intn(len(keys))], buckets[rand.Intn(len(buckets))]
}

//For every key combination, checks if all servers have the same results.
//Only works for sets as of now.
//TODO: Divide this in submethods
func verifyReplication(crdtType antidote.CRDTType) {
	//Wait for replication
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	fmt.Println("Verifying convergence...")
	//Change the "1" when we start supporting multiple crdt types
	nObjects := len(keys) * len(buckets) * 1
	//server (serverID) -> bucket -> key -> crdtType?
	//results := make(map[int]map[string]map[string]map[antidote.CRDTType]crdt.State)
	//results := make([]crdt.State, 0, nObjects)
	results := make(map[int][]crdt.State)
	//orderedRequests := make([]antidote.KeyParams, nObjects)

	//Send query and obtain results
	for serverID, serverString := range servers {
		conn, _ := net.Dial("tcp", serverString)
		results[serverID] = make([]crdt.State, 0, nObjects)

		//Preparing and sending the query
		readParams := make([]antidote.ReadObjectParams, 0, nObjects)
		for _, bucket := range buckets {
			for _, key := range keys {
				readParams = append(readParams, antidote.ReadObjectParams{KeyParams: antidote.CreateKeyParams(key, crdtType, bucket)})
			}
		}
		fmt.Println("Requesting read for:", readParams)
		proto := antidote.CreateStaticReadObjs(readParams)
		antidote.SendProto(antidote.StaticReadObjs, proto, conn)

		//Receiving reply and decoding it
		_, reply, _ := antidote.ReceiveProto(conn)
		typedReply := reply.(*antidote.ApbStaticReadObjectsResp)
		fmt.Println("Received proto reply with objects:", typedReply.GetObjects())
		for _, objProto := range typedReply.GetObjects().GetObjects() {
			results[serverID] = append(results[serverID], antidote.ConvertProtoObjectToAntidoteState(objProto, crdtType))
		}
		fmt.Println("")
	}

	//Print reads
	for i, _ := range results[0] {
		fmt.Printf("Object %d states: \n", i)
		for serverId, results := range results {
			fmt.Printf("\tServer%d: %s\n", serverId, tools.StateToString(results[i]))
		}
	}

	ok, reason := true, ""
	//Compare the results
	firstServerResults := results[0]
	//First compare if all arrays have the same length. If they don't, they aren't equal for sure.
	for _, serverResult := range results {
		if len(serverResult) != len(firstServerResults) {
			ok = false
			break
		}
	}
	if !ok {
		fmt.Println("Results don't match - number of objects in each replica is different!")
		return
	}
	//Now compare each object
	for i := 1; i < len(results) && ok; i++ {
		for j, state := range firstServerResults {
			fmt.Println("Checking equality")
			ok, reason = checkStatesEquality(state, results[i][j])
			if !ok {
				fmt.Println("States not equal.")
				fmt.Println(reason)
				break
			}
		}
	}
	if ok {
		fmt.Println("Results match - success!")
	}
}

func checkStatesEquality(firstState crdt.State, secondState crdt.State) (ok bool, reason string) {
	firstType := fmt.Sprintf("%T", firstState)
	secondType := fmt.Sprintf("%T", secondState)
	fmt.Printf("Types: %s, %s.\n", firstType, secondType)
	if firstType != secondType {
		return false, "Results don't match - different CRDT/state types!"
	}

	ok, reason = false, "Unsupported CRDT type - can't compare states"
	switch typedFirstState := firstState.(type) {
	case crdt.CounterState:
		ok, reason = checkCounterStatesEquality(typedFirstState, secondState.(crdt.CounterState))
	case crdt.RegisterState:
		ok, reason = checkRegisterStatesEquality(typedFirstState, secondState.(crdt.RegisterState))
	case crdt.SetAWValueState:
		ok, reason = checkSetStatesEquality(typedFirstState, secondState.(crdt.SetAWValueState))
	case crdt.SetAWLookupState:
		ok, reason = checkSetLookupStatesEquality(typedFirstState, secondState.(crdt.SetAWLookupState))
	case crdt.MapEntryState:
		ok, reason = checkMapStatesEquality(typedFirstState, secondState.(crdt.MapEntryState))
	case crdt.MapKeysState:
		ok, reason = checkMapKeysStatesEquality(typedFirstState, secondState.(crdt.MapKeysState))
	case crdt.MapHasKeyState:
		ok, reason = checkMapHasKeyStatesEquality(typedFirstState, secondState.(crdt.MapHasKeyState))
	case crdt.MapGetValueState:
		ok, reason = checkMapGetValueStatesEquality(typedFirstState, secondState.(crdt.MapGetValueState))
	case crdt.TopKValueState:
		ok, reason = checkTopKStatesEquality(typedFirstState, secondState.(crdt.TopKValueState))
	case crdt.AvgState:
		ok, reason = checkAvgStatesEquality(typedFirstState, secondState.(crdt.AvgState))
	case crdt.MaxMinState:
		ok, reason = checkMaxMinStatesEquality(typedFirstState, secondState.(crdt.MaxMinState))
	}
	return ok, ""
}

func checkCounterStatesEquality(firstState crdt.CounterState, secondState crdt.CounterState) (ok bool, reason string) {
	return firstState.Value == secondState.Value, "Different counter values!"
}

func checkRegisterStatesEquality(firstState crdt.RegisterState, secondState crdt.RegisterState) (ok bool, reason string) {
	return firstState.Value == secondState.Value, "Different register values!"
}

func checkSetStatesEquality(firstState crdt.SetAWValueState, secondState crdt.SetAWValueState) (ok bool, reason string) {
	if len(firstState.Elems) != len(secondState.Elems) {
		return false, "Results don't match - number of elements in one of the set states is different!"
	}
	sort.Slice(firstState.Elems, func(i, j int) bool { return firstState.Elems[i] < firstState.Elems[j] })
	sort.Slice(secondState.Elems, func(i, j int) bool { return secondState.Elems[i] < secondState.Elems[j] })
	for i, elem := range firstState.Elems {
		if secondState.Elems[i] != elem {
			return false, "Results don't match - different elements in one of the set states!"
		}
	}
	return true, ""
}

func checkSetLookupStatesEquality(firstState crdt.SetAWLookupState, secondState crdt.SetAWLookupState) (ok bool, reason string) {
	return firstState.HasElem == secondState.HasElem, "Different lookup results!"
}

func checkMapStatesEquality(firstState crdt.MapEntryState, secondState crdt.MapEntryState) (ok bool, reason string) {
	if len(firstState.Values) != len(secondState.Values) {
		return false, "Results don't match - number of keys in one of the Map states is different!"
	}
	for key, elem := range firstState.Values {
		if otherElem, has := secondState.Values[key]; !has || elem != otherElem {
			return false, "Results don't match - missing a key or its element is different!"
		}
	}
	return true, ""
}

func checkMapKeysStatesEquality(firstState crdt.MapKeysState, secondState crdt.MapKeysState) (ok bool, reason string) {
	if len(firstState.Keys) != len(secondState.Keys) {
		return false, "Results don't match - number of keys in one of the Map states is different!"
	}
	for i, key := range firstState.Keys {
		if secondState.Keys[i] != key {
			return false, "Results don't match - missing a key!"
		}
	}
	return true, ""
}

func checkMapHasKeyStatesEquality(firstState crdt.MapHasKeyState, secondState crdt.MapHasKeyState) (ok bool, reason string) {
	return firstState.HasKey == secondState.HasKey, "Different hasKey results!"
}

func checkMapGetValueStatesEquality(firstState crdt.MapGetValueState, secondState crdt.MapGetValueState) (ok bool, reason string) {
	return firstState.Value == secondState.Value, "Different elements for the same key!"
}

func checkTopKStatesEquality(firstState crdt.TopKValueState, secondState crdt.TopKValueState) (ok bool, reason string) {
	if len(firstState.Scores) != len(secondState.Scores) {
		return false, "Results don't match - number of scores in one of the TopKRmv states is different!"
	}
	sort.Slice(firstState.Scores, func(i, j int) bool { return firstState.Scores[i].Id < firstState.Scores[j].Id })
	sort.Slice(secondState.Scores, func(i, j int) bool { return secondState.Scores[i].Id < secondState.Scores[j].Id })
	for i, pair := range firstState.Scores {
		if secondState.Scores[i] != pair {
			return false, "Resulds don't match - different scores in one of the TopKRmv states!"
		}
	}
	return true, ""
}

func checkAvgStatesEquality(firstState crdt.AvgState, secondState crdt.AvgState) (ok bool, reason string) {
	return firstState.Value == secondState.Value, "Different averages!"
}

func checkMaxMinStatesEquality(firstState crdt.MaxMinState, secondState crdt.MaxMinState) (ok bool, reason string) {
	return firstState.Value == secondState.Value, "Different max/min!"
}

func benchmark() {
	keys = generateRandomKeys(10)
	buckets = generateRandomBuckets(1)
	elems := generateRandomElems(100)

	rand.Seed(0)
	nOpsRange := maxOpsPerTrans - minOpsPerTrans
	finishChan := make(chan int64)
	totalTime := int64(0)

	for i := 0; i < nClients; i++ {
		conn, err := net.Dial("tcp", servers[i%len(servers)])
		tools.CheckErr("Network connection establishment err", err)
		go executeStaticBenchmark(i, conn, keys, elems, nOpsRange, finishChan)
		//go executeNonStaticBenchmark(i, conn, keys, elems, nOpsRange, finishChan)
	}

	for i := 0; i < nClients; i++ {
		totalTime += <-finishChan
	}

	//nano -> ms
	estimatedTime := totalTime / (nClients * 1000000)
	fmt.Println("Time elapsed (ms):", estimatedTime)
	fmt.Println("Estimated nOps:", ((maxOpsPerTrans + minOpsPerTrans) / 2))
	fmt.Println("Number of transactions:", targetTrans)
	fmt.Println("Txns/s (total):", targetTrans*1000*nClients/(estimatedTime))
	fmt.Println("Txns/s (per client):", targetTrans*1000/estimatedTime)
}

func executeNonStaticBenchmark(clientID int, connection net.Conn, keys []string, elements []crdt.Element, nOpsRange int, finishChan chan int64) {
	startTime := time.Now().UTC().UnixNano()
	var previousTs []byte = nil
	var updArgs []crdt.UpdateArguments
	for nDone := 0; nDone < targetTrans; nDone += 1 {
		//Start txn
		antidote.SendProto(antidote.StartTrans, antidote.CreateStartTransaction(previousTs), connection)
		_, receivedProto, _ := antidote.ReceiveProto(connection)
		startTransResp := receivedProto.(*antidote.ApbStartTransactionResp)
		previousTs = startTransResp.GetTransactionDescriptor()

		nOps := rand.Intn(nOpsRange) + minOpsPerTrans
		nWrites := int(float32(nOps) * writeProb)
		nReads := nOps - nWrites

		if nWrites > 0 {
			switch testCrdtType {
			case antidote.CRDTType_COUNTER:
				updArgs = getRandomCounterUpdateArgs(nWrites)
			case antidote.CRDTType_LWWREG:
				updArgs = getRandomRegisterUpdateArgs(nWrites)
			case antidote.CRDTType_ORSET:
				updArgs = getRandomSetUpdateArgs(nWrites, elements)
			case antidote.CRDTType_RRMAP:
				updArgs = getRandomMapUpdateArgs(nWrites, keys, elements)
			case antidote.CRDTType_TOPK_RMV:
				updArgs = getRandomTopKRmvUpdateArgs(nWrites)
			case antidote.CRDTType_AVG:
				updArgs = getRandomAvgUpdateArgs(nWrites)
			case antidote.CRDTType_MAXMIN:
				updArgs = getRandomMaxMinUpdateArgs(nWrites)
			}
			testGenericUpdate(connection, previousTs, testCrdtType, updArgs)
		}
		if nReads > 0 {
			//fmt.Printf("Client %d preparing read.\n", clientID)
			testRead(connection, previousTs, testCrdtType, nReads)
		}
		//fmt.Printf("Client %d finished processing txn %d.\n", clientID, nDone)
		//Send commit, wait for reply and continue
		//fmt.Printf("Client %d sending commit for txn %d.\n", clientID, nDone)
		commitTxn := antidote.CreateCommitTransaction(previousTs)
		antidote.SendProto(antidote.CommitTrans, commitTxn, connection)

		//fmt.Printf("Client %d waiting for reply for txn %d.\n", clientID, nDone)
		//Receive reply, check if it is commit or abort?
		if (nDone+1)%(targetTrans/10) == 0 {
			fmt.Printf("Client %d completed txn number %d.\n", clientID, nDone+1)
		}
		_, receivedProto, _ = antidote.ReceiveProto(connection)
		commitReply := receivedProto.(*antidote.ApbCommitResp)
		previousTs = commitReply.GetCommitTime()
	}
	totalTime := time.Now().UTC().UnixNano() - startTime
	if clientID == 0 {
		verifyReplication(testCrdtType)
	}
	finishChan <- totalTime
}

func executeStaticBenchmark(clientID int, connection net.Conn, keys []string, elements []crdt.Element, nOpsRange int, finishChan chan int64) {
	startTime := time.Now().UTC().UnixNano()
	var updArgs []crdt.UpdateArguments
	for nDone := 0; nDone < targetTrans; nDone += 1 {

		nOps := rand.Intn(nOpsRange) + minOpsPerTrans
		nWrites := int(float32(nOps) * writeProb)
		nReads := nOps - nWrites

		if nWrites > 0 {
			switch testCrdtType {
			case antidote.CRDTType_COUNTER:
				updArgs = getRandomCounterUpdateArgs(nWrites)
			case antidote.CRDTType_LWWREG:
				updArgs = getRandomRegisterUpdateArgs(nWrites)
			case antidote.CRDTType_ORSET:
				updArgs = getRandomSetUpdateArgs(nWrites, elements)
			case antidote.CRDTType_RRMAP:
				updArgs = getRandomMapUpdateArgs(nWrites, keys, elements)
			case antidote.CRDTType_TOPK_RMV:
				updArgs = getRandomTopKRmvUpdateArgs(nWrites)
			case antidote.CRDTType_AVG:
				updArgs = getRandomAvgUpdateArgs(nWrites)
			case antidote.CRDTType_MAXMIN:
				updArgs = getRandomMaxMinUpdateArgs(nWrites)
			}
			//fmt.Println(clientID, "Update")
			testGenericStaticUpdate(connection, testCrdtType, updArgs)
			//fmt.Println(clientID, "Finish update")
			//Send writes
		}

		if nReads > 0 {
			//fmt.Println(clientID, "Read")
			testStaticRead(connection, testCrdtType, nReads)
			//fmt.Println(clientID, "Finish read")
		}
		//fmt.Printf("Client %d finished processing txn %d.\n", clientID, nDone)
		if (nDone+1)%(targetTrans/10) == 0 {
			fmt.Printf("Client %d completed txn number %d.\n", clientID, nDone+1)
		}
	}
	totalTime := time.Now().UTC().UnixNano() - startTime
	if clientID == 0 {
		verifyReplication(testCrdtType)
	}
	finishChan <- totalTime
}

func getRandomCounterUpdateArgs(nWrites int) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	for i := 0; i < nWrites; i++ {
		updArgs[i] = crdt.Increment{Change: rand.Int31n(maxValue*2) - maxValue}
	}
	return
}

func getRandomRegisterUpdateArgs(nWrites int) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	for i := 0; i < nWrites; i++ {
		updArgs[i] = crdt.SetValue{NewValue: string(rand.Int31n(maxValue))}
	}
	return
}

func getRandomSetUpdateArgs(nWrites int, elements []crdt.Element) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	for i := 0; i < nWrites; i++ {
		if rand.Float64() < addProb {
			updArgs[i] = crdt.Add{Element: elements[rand.Intn(len(elements))]}
		} else {
			updArgs[i] = crdt.Remove{Element: elements[rand.Intn(len(elements))]}
		}
	}
	return
}

func getRandomMapUpdateArgs(nWrites int, keys []string, elements []crdt.Element) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	for i := 0; i < nWrites; i++ {
		if rand.Float64() < addProb {
			updArgs[i] = crdt.MapAdd{Key: keys[rand.Intn(len(keys))], Value: elements[rand.Intn(len(elements))]}
		} else {
			updArgs[i] = crdt.MapRemove{Key: keys[rand.Intn(len(keys))]}
		}
	}
	return
}

func getRandomTopKRmvUpdateArgs(nWrites int) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	var id, value int32
	for i := 0; i < nWrites; i++ {
		id = rand.Int31n(maxId)
		if rand.Float64() < addProb {
			value = rand.Int31n(maxValue)
			updArgs[i] = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: id, Score: value}}
		} else {
			updArgs[i] = crdt.TopKRemove{Id: id}
		}
	}
	return
}

func getRandomAvgUpdateArgs(nWrites int) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	for i := 0; i < nWrites; i++ {
		updArgs[i] = crdt.AddValue{Value: rand.Int63n(maxValue)}
	}
	return
}

func getRandomMaxMinUpdateArgs(nWrites int) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	//Code repetition on purpose, for efficiency.
	if isMax {
		for i := 0; i < nWrites; i++ {
			updArgs[i] = crdt.MaxAddValue{Value: rand.Int63n(maxValue)}
		}
	} else {
		for i := 0; i < nWrites; i++ {
			updArgs[i] = crdt.MinAddValue{Value: rand.Int63n(maxValue)}
		}
	}
	return
}

func generateRandomKeys(number int) (rndKeys []string) {
	rndKeys = make([]string, number)
	for i := 0; i < number; i++ {
		rndKeys[i] = fmt.Sprint(rand.Intn(1000))
	}
	return rndKeys
}

func generateRandomBuckets(number int) (rndBuckets []string) {
	return generateRandomKeys(number)
}

func generateRandomElems(number int) (rndElems []crdt.Element) {
	rndElems = make([]crdt.Element, number)
	for i := 0; i < number; i++ {
		rndElems[i] = crdt.Element(fmt.Sprint(rand.Intn(1000)))
	}
	return rndElems
}

//Note: For now this only knows how to print sets.
/*
func printDetailedReadResult(protoBuf proto.Message) {
	switch protoBuf.(type) {
	case *antidote.ApbStaticReadObjectsResp:
		fmt.Println("Detailed received proto: ")
		readProtos := protoBuf.(*antidote.ApbStaticReadObjectsResp).Objects.Objects
		for _, currProto := range readProtos {
			if currProto.GetSet() != nil {
				elements := crdt.ByteMatrixToElementArray(currProto.GetSet().GetValue())
				for _, elem := range elements {
					fmt.Print(string(elem), ",")
				}
			} else if currProto.GetCounter() != nil {
				fmt.Print(currProto.GetCounter().GetValue())
			} else {
				fmt.Print("unsupported")
			}
			fmt.Print("; ")
		}
		fmt.Println()
	case *antidote.ApbErrorResp:
		fmt.Println("Error - tried to print detailed read result but the proto is an ApbErrorResp.")
	default:
		fmt.Println("Error on detailed read print - proto isn't a ApbStaticReadObjectsResp or ApbErrorResp.")
	}
}
*/
