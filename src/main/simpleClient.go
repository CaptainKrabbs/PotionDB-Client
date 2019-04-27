package main

import (
	"antidote"
	"bufio"
	"crdt"
	"fmt"
	rand "math/rand"
	"net"
	"os"
	"time"
	"tools"

	proto "github.com/golang/protobuf/proto"
)

const (
	bucket      = "bkt"
	maxId       = 50000000
	maxValue    = 50
	targetTrans = 50
	//targetTrans = 10
	writeProb = 0.7
	//writeProb = 0.5
	minOpsPerTrans = 3
	maxOpsPerTrans = 10
	//maxOpsPerTrans = 4
	maxSleepTime      = 100
	nClients          = 3
	beforeStartSleep  = 2000
	sleepBeforeVerify = 4000
)

var (
	//keys = [1]string{"topk1"}
	keys = [2]string{"topk1", "topk2"}
	//keys = [4]string{"counter1", "counter2", "counter3", "counter4"}
	//buckets = [2][]byte{[]byte("bkt1"), []byte("bkt2")}
	buckets = [2]string{"bkt1", "bkt2"}
	//buckets = [1]string{"bkt"}
	elems   = [...]string{"a", "b", "c", "d", "e"}
	servers = [...]string{"127.0.0.1:8087", "127.0.0.1:8088"}
	reader  = bufio.NewReader(os.Stdin)
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
	for i := 0; i < nClients; i++ {
		conn, err := net.Dial("tcp", servers[i%len(servers)])
		tools.CheckErr("Network connection establishment err", err)
		go transactionCycle(i, conn)
	}
	fmt.Println("Click enter once transactions stop happening.")
	reader.ReadString('\n')

	//testSet(connection)
	//testStaticUpdate(connection)
	//time.Sleep(time.Duration(1000) * time.Millisecond)
	//testStaticRead(connection, antidote.CRDTType_COUNTER, 10)
}

//TODO: Test this and testStaticUpdate() with antidote
func testStaticRead(connection net.Conn, crdtType antidote.CRDTType, nReads int) (receivedProto proto.Message) {
	reads := make([]antidote.ReadObjectParams, nReads)
	for i := 0; i < len(reads); i++ {
		rndKey, rndBucket := getRandomLocationParams()
		reads[i] = antidote.ReadObjectParams{
			KeyParams: antidote.CreateKeyParams(rndKey, crdtType, rndBucket),
		}
	}

	proto := antidote.CreateStaticReadObjs(reads)
	antidote.SendProto(antidote.StaticReadObjs, proto, connection)
	fmt.Println("Proto sent! Waiting for reply.")

	//Wait for reply
	protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Received type, proto: ", protoType, receivedProto)

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
	testGenericUpdate(connection, antidote.CRDTType_ORSET, adds)

	testStaticRead(connection, antidote.CRDTType_ORSET, 5)

	rems := make([]crdt.UpdateArguments, 6)
	for i := 0; i < len(rems); i++ {
		rems[i] = crdt.Remove{Element: crdt.Element(fmt.Sprint(rand.Uint64()))}
	}
	testGenericUpdate(connection, antidote.CRDTType_ORSET, rems)

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
	testGenericUpdate(connection, antidote.CRDTType_ORSET, addAll)

	testStaticRead(connection, antidote.CRDTType_ORSET, 5)

	remAll := make([]crdt.UpdateArguments, 2)
	for i := 0; i < len(remAll); i++ {
		rndElems = make([]crdt.Element, 2)
		for j := 0; j < len(rndElems); j++ {
			rndElems[j] = crdt.Element(fmt.Sprint(rand.Uint64()))
		}
		remAll[i] = crdt.RemoveAll{Elems: rndElems}
	}
	testGenericUpdate(connection, antidote.CRDTType_ORSET, remAll)

	testStaticRead(connection, antidote.CRDTType_ORSET, 5)
}

func testGenericUpdate(connection net.Conn, crdtType antidote.CRDTType, args []crdt.UpdateArguments) {
	updates := make([]antidote.UpdateObjectParams, len(args))
	for i := 0; i < len(args); i++ {
		rndKey, rndBucket := getRandomLocationParams()
		fmt.Println("Generating update op to key, bucket", rndKey, rndBucket)
		updates[i] = antidote.UpdateObjectParams{
			KeyParams:  antidote.CreateKeyParams(rndKey, crdtType, rndBucket),
			UpdateArgs: args[i],
		}
	}

	proto := antidote.CreateStaticUpdateObjs(updates)
	antidote.SendProto(antidote.StaticUpdateObjs, proto, connection)
	fmt.Println("Proto update sent! Waiting for reply.")

	//Wait for reply
	protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Received type, proto: ", protoType, receivedProto)
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
		verifyReplication()
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
		//_, replyProto, _ := antidote.ReceiveProto(connection)
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
		protoType, protoBuf = antidote.UpdateObjs, getNextWrite(transId, key, antidote.CRDTType_ORSET)
	} else {
		//fmt.Println("Next proto is a read.")
		protoType, protoBuf = antidote.ReadObjs, getNextRead(transId, key, antidote.CRDTType_ORSET)
	}
	if protoBuf == nil {
		fmt.Println("Warning - nil protoBuf on getNextOp!")
	}
	return
}

func getNextWrite(transId []byte, key string, crdtType antidote.CRDTType) (updateBuf *antidote.ApbUpdateObjects) {
	switch crdtType {
	case antidote.CRDTType_TOPK:
		rndPlayer, rndValue := rand.Intn(maxId), rand.Intn(maxValue)
		//rndPlayer, rndValue := 1, 887
		topkWrite := antidote.CreateTopkUpdate(rndPlayer, rndValue)
		updateBuf = antidote.CreateUpdateObjs(transId, key, antidote.CRDTType_TOPK, bucket, topkWrite)
		//updateBuf = antidote.CreateUpdateObjs(transId, "topk1", antidote.CRDTType_TOPK, "bkt", topkWrite)
		//fmt.Println("Write transId:", updateBuf.GetTransactionDescriptor())
		//fmt.Println("Values sent:", rndPlayer, rndValue)
	case antidote.CRDTType_ORSET:
		rndElem := elems[rand.Intn(len(elems))]
		var setWrite *antidote.ApbSetUpdate
		if rand.Float32() < 0.5 {
			setWrite = antidote.CreateSetUpdate(antidote.ApbSetUpdate_ADD, []string{rndElem})
		} else {
			setWrite = antidote.CreateSetUpdate(antidote.ApbSetUpdate_REMOVE, []string{rndElem})
		}
		updateBuf = antidote.CreateUpdateObjs(transId, key, antidote.CRDTType_ORSET, bucket, setWrite)
	}
	return
}

func getNextRead(transId []byte, key string, crdtType antidote.CRDTType) (readBuf *antidote.ApbReadObjects) {
	return createRead(transId, key, crdtType)
}

func createRead(transId []byte, key string, crdtType antidote.CRDTType) (readBuf *antidote.ApbReadObjects) {
	//fmt.Println("Creating read")
	readBuf = antidote.CreateReadObjs(transId, key, crdtType, bucket)
	//fmt.Println("Read transId:", readBuf.GetTransactionDescriptor())
	return
}

func createCounterWrite(transId []byte, key string) (updateBuf *antidote.ApbUpdateObjects) {
	//fmt.Println("Creating write")
	write := antidote.CreateCounterUpdate(5)
	updateBuf = antidote.CreateUpdateObjs(transId, key, antidote.CRDTType_COUNTER, bucket, write)
	//fmt.Println("Write transId:", updateBuf.GetTransactionDescriptor())
	return
}

func getRandomLocationParams() (key string, bucket string) {
	key, bucket = keys[rand.Intn(len(keys))], buckets[rand.Intn(len(buckets))]
	return
}

//For every key combination, checks if all servers have the same results.
//Only works for sets as of now.
//TODO: Divide this in submethods
func verifyReplication() {
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	//Change the "1" when we start supporting multiple crdt types
	nObjects := len(keys) * len(buckets) * 1
	//Wait for replication
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	//server (serverID) -> bucket -> key -> crdtType?
	//results := make(map[int]map[string]map[string]map[antidote.CRDTType]crdt.State)
	//results := make([]crdt.State, 0, nObjects)
	results := make(map[int][]crdt.State)
	//orderedRequests := make([]antidote.KeyParams, nObjects)

	//Prepare the results map
	/*
		for serverID, _ := range servers {
			bucketMap := make(map[string]map[string]map[antidote.CRDTType]crdt.State)
			for _, bucket := range buckets {
				keyMap := make(map[string]map[antidote.CRDTType]crdt.State)
				for _, key := range keys {
					keyMap[key] = make(map[antidote.CRDTType]crdt.State)
				}
				bucketMap[bucket] = keyMap
			}
			results[serverID] = bucketMap
		}
	*/

	//Send query and obtain results
	for serverID, serverString := range servers {
		conn, _ := net.Dial("tcp", serverString)
		results[serverID] = make([]crdt.State, 0, nObjects)

		//Preparing and sending the query
		readParams := make([]antidote.ReadObjectParams, 0, nObjects)
		for _, bucket := range buckets {
			for _, key := range keys {
				readParams = append(readParams, antidote.ReadObjectParams{KeyParams: antidote.CreateKeyParams(key, antidote.CRDTType_ORSET, bucket)})
			}
		}
		proto := antidote.CreateStaticReadObjs(readParams)
		antidote.SendProto(antidote.StaticReadObjs, proto, conn)

		//Receiving reply and decoding it
		_, reply, _ := antidote.ReceiveProto(conn)
		typedReply := reply.(*antidote.ApbStaticReadObjectsResp)
		serverResults := results[serverID]
		for _, objProto := range typedReply.GetObjects().GetObjects() {
			//TODO: Take in consideration the CRDT type here
			allObjsBytes := objProto.GetSet().GetValue()
			setState := crdt.SetAWValueState{Elems: make([]crdt.Element, len(allObjsBytes))}
			//Convert byte[][] back to strings
			for i, objBytes := range allObjsBytes {
				setState.Elems[i] = crdt.Element(objBytes)
			}
			serverResults = append(serverResults, setState)
		}
		//func CreateStaticReadObjs(readParams []ReadObjectParams) (protobuf *ApbStaticReadObjects)
	}

	ok := true
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
	for i := 1; i < len(results) && !ok; i++ {
		for j, state := range firstServerResults {
			//TODO: Consider CRDT types
			otherState := results[i][j]
			setState := state.(crdt.SetAWValueState)
			otherSetState := otherState.(crdt.SetAWValueState)
			if len(setState.Elems) != len(otherSetState.Elems) {
				fmt.Println("Results don't match - number of elements in one of the set states is different")
				ok = false
				break
			}
			for k, elem := range setState.Elems {
				if otherSetState.Elems[k] != elem {
					fmt.Println("Results don't match - different elements in one of the set states!")
					ok = false
					break
				}
			}
			if !ok {
				break
			}
		}
	}
	if ok {
		fmt.Println("Results match - success!")
	}
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
