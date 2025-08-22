package main

import (
	"bufio"
	"fmt"
	"math"
	rand "math/rand"
	"net"
	"os"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	antidote "potionDB/potionDB/components"
	"potionDB/potionDB/utilities"
	"potionDB_client/src/clientLib"
	"sort"
	"sqlToKeyValue/src/sql"
	"time"

	//pb "github.com/golang/protobuf/proto"

	pb "google.golang.org/protobuf/proto"
)

type BenchmarkArgs struct {
	clientID  int
	nOpsRange int

	elements  []crdt.Element
	keys      []string
	crdtTypes []proto.CRDTType
}

const (
	bucket   = "bkt"
	maxId    = 20
	maxValue = 2000
	//targetTrans  = 2000
	targetTrans = 50
	//targetTrans = 1
	//writeProb = 0
	//writeProb = 0.7
	writeProb      = 1
	addProb        = 0.5
	rrMapAddProb   = 1
	minOpsPerTrans = 3
	maxOpsPerTrans = 10
	//maxOpsPerTrans = 4
	maxSleepTime = 300
	//maxSleepTime = 50
	nClients = 4
	//nClients         = 1
	beforeStartSleep = 2000
	//sleepBeforeVerify = 4000
	//Use smaller values only for single replica debugging
	//sleepBeforeVerify = 2000
	sleepBeforeVerify = 10000
	isMax             = crdt.IS_MAX //Used for MaxMinCRDT
	maxEmbUpdates     = 5           //Max number of embedded CRDTs to be updated/removed
)

var (
	//testCrdtType = proto.CRDTType_TOPK_RMV
	//testCrdtType = proto.CRDTType_RRMAP
	//testCrdtType = proto.CRDTType_COUNTER
	testCrdtType = proto.CRDTType_NOOP
	keys         = []string{"topk1"}
	//keys = []string{"topk1", "topk2"}
	//keys = [4]string{"counter1", "counter2", "counter3", "counter4"}
	//buckets = [2][]byte{[]byte("bkt1"), []byte("bkt2")}
	//buckets = []string{"bkt", "bkt1", "bkt2"}
	buckets = []string{"bkt"}
	elems   = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	//mapKeys = []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	mapKeys = []string{"1", "2", "3", "4"}
	servers = []string{"127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089"}
	//servers = []string{"127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089", "127.0.0.1:8090", "127.0.0.1:8091"}
	//servers = []string{"127.0.0.1:8087"}
	//servers = []string{"localhost:8087"}
	//servers = []string{"localhost:8087", "localhost:8088"}
	//servers = []string{"127.0.0.1:8087", "127.0.0.1:8088"}
	//servers  = []string{"127.0.0.1:8087"}
	embTypes = []proto.CRDTType{proto.CRDTType_TOPK_RMV,
		proto.CRDTType_ORSET, proto.CRDTType_MAXMIN}
	//embTypes = []antidote.CRDTType{proto.CRDTType_LWWREG}
	reader = bufio.NewReader(os.Stdin)
)

/*
Plan: execute "x" transactions, each one doing a certain number of updates and reads (maybe use chance?).
Give some random (but low) delay between transactions. Repeat if they get aborted.
*/

func main() {
	//connection, err := net.Dial("tcp", "127.0.0.1:8087")
	//utilities.CheckErr("Network connection establishment err", err)

	rand.Seed(time.Now().UTC().UnixNano())
	//transactionCycle(connection)
	/*
		for i := 0; i < nClients; i++ {
			conn, err := net.Dial("tcp", servers[i%len(servers)])
			utilities.CheckErr("Network connection establishment err", err)
			go transactionCycle(i, conn)
		}*/

	//fmt.Println("Click enter once transactions stop happening.")
	//reader.ReadString('\n')

	//listTypes := []proto.CRDTType{proto.CRDTType_COUNTER, proto.CRDTType_LWWREG, proto.CRDTType_ORSET,
	//proto.CRDTType_ORMAP, proto.CRDTType_TOPK_RMV, proto.CRDTType_AVG, proto.CRDTType_MAXMIN}
	//listTypes := []proto.CRDTType{proto.CRDTType_RRMAP}
	/*
		for _, crdtType := range listTypes {
			testCrdtType = crdtType
			fmt.Println("Testing", testCrdtType)
			benchmark()
		}
	*/
	//benchmark()
	//testSet(connection)
	//time.Sleep(time.Duration(1000) * time.Millisecond)
	//testStaticRead(connection, antidote.CRDTType_COUNTER, 10)

	conns := make([]net.Conn, len(servers))
	for i := 0; i < len(servers); i++ {
		conn, err := net.Dial("tcp", servers[i])
		utilities.CheckErr("Network connection establishment err", err)
		conns[i] = conn
	}

	//testStaticUpdate(conns[0])
	//testRemoteOps(conns)
	//sophiaTestRemote(conns)
	//sophiaTestMix2(conns)

	//time.Sleep(5000 * time.Millisecond)
	//testStaticRead(conns[1], testCrdtType, 1)
	//testStaticRead(conns[2], testCrdtType, 2)

	//testBoundedCounter(conns)
	//testFlags(conns)
	//testTopK(conns)
	//testNoOp(conns)
	testNoOp2(conns)
	//sophiaTXN4Wait(conns)
	select {}
}

func testSQL(conns []net.Conn) {
	clientTestCreateTable(conns, sql.Table)
}

func clientTestCreateTable(conns []net.Conn, sqlCode string) {
	clientLib.CreateTable(conns[0], sqlCode)
}

func sophiaTestRemote(conns []net.Conn) {
	//Remote
	keyParams := crdt.MakeKeyParams("some_other_key_counter", proto.CRDTType_COUNTER, "R1")
	//Local
	//keyParams := crdt.MakeKeyParams("some_other_key_counter", proto.CRDTType_COUNTER, "some_bucket")
	readParams := []crdt.ReadObjectParams{{KeyParams: keyParams}}
	updParams := []crdt.UpdateObjectParams{{KeyParams: keyParams, UpdateArgs: crdt.Increment{Change: 3}}}

	fmt.Println("[TXN1]")
	txnId := clientLib.StartTxn(conns[0])
	readReply := clientLib.Read(conns[0], txnId, readParams)
	fmt.Println("[MY_TEST_KEY2]", 8087, readReply.GetObjects()[0].GetCounter().GetValue())
	clientLib.Update(conns[0], txnId, updParams)
	readReply = clientLib.Read(conns[0], txnId, readParams)
	fmt.Println("[MY_TEST_KEY2]", 8087, readReply.GetObjects()[0].GetCounter().GetValue())
	clientLib.Update(conns[0], txnId, updParams)
	readReply = clientLib.Read(conns[0], txnId, readParams)
	fmt.Println("[MY_TEST_KEY2]", 8087, readReply.GetObjects()[0].GetCounter().GetValue())
	txnId = clientLib.CommitTxn(conns[0], txnId)

	//time.Sleep(5000 * time.Millisecond)

	fmt.Println("[TXN2]")
	txnId = clientLib.StartTxn(conns[0])
	readReply = clientLib.Read(conns[0], txnId, readParams)
	fmt.Println("[MY_TEST_KEY2]", 8087, readReply.GetObjects()[0].GetCounter().GetValue())
	txnId = clientLib.CommitTxn(conns[0], txnId)
	//time.Sleep(5000 * time.Millisecond)

	fmt.Println("[TXN3]")
	staticReadReply := clientLib.StaticRead(conns[0], txnId, readParams)
	txnId = staticReadReply.GetCommittime().GetCommitTime()
	fmt.Println("[MY_TEST_KEY2]", 8087, staticReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	updParams[0].UpdateArgs = crdt.Increment{Change: 2}
	updReply := clientLib.StaticUpdate(conns[0], txnId, updParams)
	txnId = updReply.GetCommitTime()
	staticReadReply = clientLib.StaticRead(conns[0], txnId, readParams)
	txnId = staticReadReply.GetCommittime().GetCommitTime()
	fmt.Println("[MY_TEST_KEY2]", 8087, staticReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	//time.Sleep(5000 * time.Millisecond)

	fmt.Println("[TXN4]")
	updParams[0].UpdateArgs = crdt.Increment{Change: 3}
	updReply = clientLib.StaticUpdate(conns[0], txnId, updParams)
	txnId = updReply.GetCommitTime()
	staticReadReply = clientLib.StaticRead(conns[0], txnId, readParams)
	txnId = staticReadReply.GetCommittime().GetCommitTime()
	fmt.Println("[MY_TEST_KEY2]", 8087, staticReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())

	fmt.Println("Finish")
}

func sophiaTestMix2(conns []net.Conn) {
	conn := conns[0]
	key1, key2, key3 := crdt.MakeKeyParams("some_key_counter", proto.CRDTType_COUNTER, "R2"),
		crdt.MakeKeyParams("some_other_key_counter", proto.CRDTType_COUNTER, "R1"),
		crdt.MakeKeyParams("some_key_orset", proto.CRDTType_ORSET, "R2")

	//key1, key2, key3 := crdt.MakeKeyParams("some_key_counter", proto.CRDTType_COUNTER, "some_bucket"),
	//	crdt.MakeKeyParams("some_other_key_counter", proto.CRDTType_COUNTER, "some_bucket"),
	//	crdt.MakeKeyParams("some_key_orset", proto.CRDTType_ORSET, "some_bucket")
	allRead := []crdt.ReadObjectParams{{KeyParams: key1}, {KeyParams: key2}, {KeyParams: key3}}
	firstRead := []crdt.ReadObjectParams{{KeyParams: key1}}
	firstTwoRead := []crdt.ReadObjectParams{{KeyParams: key1}, {KeyParams: key2}}
	thirdRead := []crdt.ReadObjectParams{{KeyParams: key3}}

	upd1, upd2, upd3 := crdt.Increment{Change: 2}, crdt.Increment{Change: 3}, crdt.AddAll{Elems: []crdt.Element{"val1", "val2"}}
	updAll := []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: upd1}, {KeyParams: key2, UpdateArgs: upd2}, {KeyParams: key3, UpdateArgs: upd3}}
	firstUpd := []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: upd1}}
	secondUpd := []crdt.UpdateObjectParams{{KeyParams: key2, UpdateArgs: upd2}}

	fmt.Println("[TXN1]")
	txnId := clientLib.StartTxn(conn)
	readReply := clientLib.Read(conn, txnId, firstRead)
	readReply = clientLib.Read(conn, txnId, firstTwoRead)
	objs := readReply.GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY2]", 8087, objs[1].GetCounter().GetValue())
	readReply = clientLib.Read(conn, txnId, thirdRead)
	objs = readReply.GetObjects()
	fmt.Println("[MY_TEST_KEY3]", 8087, objs[0].GetSet().GetValue())
	clientLib.Update(conn, txnId, updAll)
	readReply = clientLib.Read(conn, txnId, allRead)
	objs = readReply.GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY2]", 8087, objs[1].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY3]", 8087, objs[2].GetSet().GetValue())
	txnId = clientLib.CommitTxn(conn, txnId)

	fmt.Println("[TXN2]")
	txnId = clientLib.StartTxn(conn)
	readReply = clientLib.Read(conn, txnId, firstTwoRead)
	objs = readReply.GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY2]", 8087, objs[1].GetCounter().GetValue())
	upd1 = crdt.Increment{Change: 8}
	clientLib.Update(conn, txnId, firstUpd)
	readReply = clientLib.Read(conn, txnId, firstRead)
	objs = readReply.GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	txnId = clientLib.CommitTxn(conn, txnId)

	fmt.Println("[TXN3]")
	txnId = clientLib.StartTxn(conn)
	upd1 = crdt.Increment{Change: 5}
	clientLib.Update(conn, txnId, firstUpd)
	readReply = clientLib.Read(conn, txnId, firstRead)
	objs = readReply.GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	txnId = clientLib.CommitTxn(conn, txnId)

	fmt.Println("[TXN4]")
	staticReadReply := clientLib.StaticRead(conn, txnId, firstTwoRead)
	txnId = staticReadReply.GetCommittime().GetCommitTime()
	objs = staticReadReply.GetObjects().GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY2]", 8087, objs[1].GetCounter().GetValue())
	upd1, upd2, upd3 = crdt.Increment{Change: 3}, crdt.Increment{Change: 2}, crdt.AddAll{Elems: []crdt.Element{"val3"}}
	txnId = clientLib.StaticUpdate(conn, txnId, updAll).GetCommitTime()
	staticReadReply = clientLib.StaticRead(conn, txnId, allRead)
	txnId = staticReadReply.GetCommittime().GetCommitTime()
	objs = staticReadReply.GetObjects().GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY2]", 8087, objs[1].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY3]", 8087, objs[2].GetSet().GetValue())

	fmt.Println("[TXN5]")
	upd2 = crdt.Increment{Change: 3}
	txnId = clientLib.StaticUpdate(conn, txnId, secondUpd).GetCommitTime()
	staticReadReply = clientLib.StaticRead(conn, txnId, allRead)
	txnId = staticReadReply.GetCommittime().GetCommitTime()
	objs = staticReadReply.GetObjects().GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY2]", 8087, objs[1].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY3]", 8087, objs[2].GetSet().GetValue())
}

func sophiaTXN4Wait(conns []net.Conn) {
	conn := conns[0]
	key1, key2 := crdt.MakeKeyParams("some_key_counter", proto.CRDTType_COUNTER, "R1"),
		crdt.MakeKeyParams("some_other_key_counter", proto.CRDTType_COUNTER, "R2")

	//key1, key2, key3 := crdt.MakeKeyParams("some_key_counter", proto.CRDTType_COUNTER, "some_bucket"),
	//	crdt.MakeKeyParams("some_other_key_counter", proto.CRDTType_COUNTER, "some_bucket"),
	//	crdt.MakeKeyParams("some_key_orset", proto.CRDTType_ORSET, "some_bucket")
	allRead := []crdt.ReadObjectParams{{KeyParams: key1}, {KeyParams: key2}}
	firstTwoRead := []crdt.ReadObjectParams{{KeyParams: key1}, {KeyParams: key2}}

	//upd1 := crdt.Increment{Change: 2}
	upd2 := crdt.Increment{Change: 3}

	//updAll := []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: upd1}, {KeyParams: key2, UpdateArgs: upd2}}
	secondUpd := []crdt.UpdateObjectParams{{KeyParams: key2, UpdateArgs: upd2}}

	fmt.Println("[TXN4]")
	staticReadReply := clientLib.StaticRead(conn, nil, firstTwoRead)
	txnId := staticReadReply.GetCommittime().GetCommitTime()
	objs := staticReadReply.GetObjects().GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY2]", 8087, objs[1].GetCounter().GetValue())
	txnId = clientLib.StaticUpdate(conn, txnId, secondUpd).GetCommitTime()

	//wait
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)

	staticReadReply = clientLib.StaticRead(conn, txnId, allRead)
	txnId = staticReadReply.GetCommittime().GetCommitTime()
	objs = staticReadReply.GetObjects().GetObjects()
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY2]", 8087, objs[1].GetCounter().GetValue())

	staticReadReply = clientLib.StaticRead(conns[1], txnId, allRead)
	txnId = staticReadReply.GetCommittime().GetCommitTime()
	objs = staticReadReply.GetObjects().GetObjects()
	fmt.Println("[Read from replica with R2]")
	fmt.Println("[MY_TEST_KEY1]", 8087, objs[0].GetCounter().GetValue())
	fmt.Println("[MY_TEST_KEY2]", 8087, objs[1].GetCounter().GetValue())
}

// Assumes a list of connections and two or more buckets
func testRemoteOps(conns []net.Conn) {
	txnId := clientLib.StartTxn(conns[0])

	read := []crdt.ReadObjectParams{{KeyParams: crdt.MakeKeyParams("key1", proto.CRDTType_COUNTER, "R1")}}
	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, read), conns[0])
	antidote.ReceiveProto(conns[0])
	fmt.Println("Read reply received")
	clientLib.CommitTxn(conns[0], txnId)
	fmt.Println("Commit finished")
}

func testNoOp(conns []net.Conn) {
	addArtistSam := &crdt.AddArtist{ArtistName: "Sam"}
	addAlbum1 := &crdt.AddAlbum{AlbumName: "A1", ArtistName: "Sam"}
	addAlbum2 := &crdt.AddAlbum{AlbumName: "A2", ArtistName: "Sam"}
	updArtistSam := &crdt.UpdArtist{ArtistName: "Sam"}
	rmvArtistSam := &crdt.RmvArtist{ArtistName: "Sam"}

	conn := conns[0]
	key1 := crdt.MakeKeyParams("noop_replica1_key", proto.CRDTType_NOOP, "R1")

	var upd0 crdt.Operation = &crdt.DetermineStateOp{NewStateCode: (&crdt.MusicState{}).GetStateCode()}

	allRead := []crdt.ReadObjectParams{{KeyParams: key1}}
	nextUpd := []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: upd0}}
	//secondUpd := []crdt.UpdateObjectParams{{KeyParams: key2, UpdateArgs: upd2}}

	fmt.Println("[TXN1]")
	txnId := []byte{}
	//Settings state hasn't been done yet
	displayBothReps(conns, &txnId, allRead)
	
	fmt.Println("New proposed state code:", (&crdt.MusicState{}).GetStateCode())

	txnId = clientLib.StaticUpdate(conn, txnId, nextUpd).GetCommitTime()

	//wait before reading
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	fmt.Println("Finished waiting")
	displayBothReps(conns, &txnId, allRead)

	//addArtistSam
	fmt.Println("Next Operation:", addArtistSam.String())
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addArtistSam}}
	txnId = clientLib.StaticUpdate(conn, txnId, nextUpd).GetCommitTime()

	//wait before reading
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	displayBothReps(conns, &txnId, allRead)


	//AddAlbums to Sam: A1 (Replica1), A2 (Replica2)
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addAlbum1}}
	txnId = clientLib.StaticUpdate(conn, txnId, nextUpd).GetCommitTime()
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addAlbum2}}
	txnId = clientLib.StaticUpdate(conns[1], txnId, nextUpd).GetCommitTime()
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	displayBothReps(conns, &txnId, allRead)

	//UpdArtist and RmvArtist targeted at Sam
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: updArtistSam}}
	txnId = clientLib.StaticUpdate(conn, txnId, nextUpd).GetCommitTime()
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: rmvArtistSam}}
	txnId = clientLib.StaticUpdate(conns[1], txnId, nextUpd).GetCommitTime()

	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	displayBothReps(conns, &txnId, allRead)
}

//requires 3 connections
func testNoOp2(conns []net.Conn) {
addArtistSam := &crdt.AddArtist{ArtistName: "Sam"}
	addAlbum1 := &crdt.AddAlbum{AlbumName: "A1", ArtistName: "Sam"}
	addAlbum2 := &crdt.AddAlbum{AlbumName: "A2", ArtistName: "Sam"}
	//updArtistSam := &crdt.UpdArtist{ArtistName: "Sam"}
	rmvArtistSam := &crdt.RmvArtist{ArtistName: "Sam"}

	addArtistFred := &crdt.AddArtist{ArtistName: "Fred"}
	addAlbumFred := &crdt.AddAlbum{AlbumName: "The Great Pretender", ArtistName: "Fred"}
	rmvArtistFred := &crdt.RmvArtist{ArtistName: "Fred"}

	addArtistMonke := &crdt.AddArtist{ArtistName: "Monke"}
	addAlbumMonke1 :=  &crdt.AddAlbum{AlbumName: "Tearing felt", ArtistName: "Monke"}
	addAlbumMonke2 :=  &crdt.AddAlbum{AlbumName: "Winding up", ArtistName: "Monke"}
	addAlbumMonke3 :=  &crdt.AddAlbum{AlbumName: "Cymbal clatter", ArtistName: "Monke"}

	conn := conns[0]
	key1 := crdt.MakeKeyParams("noop_replica1_key", proto.CRDTType_NOOP, "R1")

	var upd0 crdt.Operation = &crdt.DetermineStateOp{NewStateCode: (&crdt.MusicState{}).GetStateCode()}

	allRead := []crdt.ReadObjectParams{{KeyParams: key1}}
	nextUpd := []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: upd0}}
	//secondUpd := []crdt.UpdateObjectParams{{KeyParams: key2, UpdateArgs: upd2}}

	fmt.Println("[TXN1]")
	txnId := []byte{}
	//Settings state hasn't been done yet
	displayBothReps(conns, &txnId, allRead)
	
	fmt.Println("New proposed state code:", (&crdt.MusicState{}).GetStateCode())

	txnId = clientLib.StaticUpdate(conn, txnId, nextUpd).GetCommitTime()

	//wait before reading
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	fmt.Println("Finished waiting")
	displayBothReps(conns, &txnId, allRead)

	//addArtistSam
	fmt.Println("Next Operation:", addArtistSam.String())
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addArtistSam}}
	txnId = clientLib.StaticUpdate(conn, txnId, nextUpd).GetCommitTime()

	//wait before reading
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	displayBothReps(conns, &txnId, allRead)


	//AddAlbums to Sam: A1 (Replica1), A2 (Replica2)
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addAlbum1}}
	txnId = clientLib.StaticUpdate(conn, txnId, nextUpd).GetCommitTime()
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addAlbum2}}
	txnId = clientLib.StaticUpdate(conns[1], txnId, nextUpd).GetCommitTime()
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	displayBothReps(conns, &txnId, allRead)

	fmt.Println("AddArtist Fred")
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addArtistFred}}
	txnId = clientLib.StaticUpdate(conns[1], txnId, nextUpd).GetCommitTime()

	fmt.Println("AddArtist Monke, and AddAlbumMonke1")
	//(if simultaneous album shouldn't save due to not meeting preconditions, if sequential it will be saved)
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addArtistMonke}}
	txnId = clientLib.StaticUpdate(conn, txnId, nextUpd).GetCommitTime()
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addAlbumMonke1}}
	txnId = clientLib.StaticUpdate(conns[1], txnId, nextUpd).GetCommitTime()
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	displayBothReps(conns, &txnId, allRead)

	fmt.Println("RmvArtist Sam, AddAlbumMonke2, AddAlbumFred")
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: rmvArtistSam}}
	txnId = clientLib.StaticUpdate(conn, txnId, nextUpd).GetCommitTime()
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addAlbumMonke2}}
	txnId = clientLib.StaticUpdate(conns[1], txnId, nextUpd).GetCommitTime()
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addAlbumFred}}
	txnId = clientLib.StaticUpdate(conns[2], txnId, nextUpd).GetCommitTime()
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	displayBothReps(conns, &txnId, allRead)

	fmt.Println("RmvArtistFred, AddAlbumMonke3")
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: addAlbumMonke3}}
	txnId = clientLib.StaticUpdate(conns[1], txnId, nextUpd).GetCommitTime()
	nextUpd = []crdt.UpdateObjectParams{{KeyParams: key1, UpdateArgs: rmvArtistFred}}
	txnId = clientLib.StaticUpdate(conns[2], txnId, nextUpd).GetCommitTime()
	time.Sleep(time.Duration(sleepBeforeVerify) * time.Millisecond)
	displayBothReps(conns, &txnId, allRead)
}

func displayBothReps(conns []net.Conn, txnId *[]byte, reads []crdt.ReadObjectParams) {
	for i, conn := range conns {
		fmt.Println("Rep", i+1)
		staticReadReply := clientLib.StaticRead(conn, *txnId, reads)
		*txnId = staticReadReply.GetCommittime().GetCommitTime()
		objs := staticReadReply.GetObjects().GetObjects()
		displayNoOpStateData(objs)
	}
}

func displayNoOpStateData(objs []*proto.ApbReadObjectResp) {
	for i, obj := range objs {
		fmt.Printf("[MY_TEST_KEY%v] %v StateCode: %v\n", i, 8087, obj.GetNoop().GetStateCode())
		switch obj.GetNoop().GetStateCode() {
		case (&crdt.MusicState{}).GetStateCode():
			crdt.PrintMusicStateFromBytes(obj.GetNoop().GetStateData())
		}
	}
}

func testStaticRead(connection net.Conn, crdtType proto.CRDTType, nReads int) (receivedProto pb.Message) {
	reads := createReadObjectParams(crdtType, nReads)
	proto := antidote.CreateStaticReadObjs(nil, reads)
	antidote.SendProto(antidote.StaticReadObjs, proto, connection)
	fmt.Println("Proto sent! Waiting for reply.")

	//Wait for reply
	//antidote.ReceiveProto(connection)
	protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Received type, proto: ", protoType, receivedProto)

	return
}

func testRead(connection net.Conn, transId []byte, crdtType proto.CRDTType, nReads int) (receivedProto pb.Message) {
	reads := createReadObjectParams(crdtType, nReads)
	proto := antidote.CreateReadObjs(transId, reads)
	antidote.SendProto(antidote.ReadObjs, proto, connection)
	//fmt.Println("Proto sent! Waiting for reply.")

	//Wait for reply
	antidote.ReceiveProto(connection)
	//protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	//fmt.Println("Received type, proto: ", protoType, receivedProto)

	return
}

func createReadObjectParams(crdtType proto.CRDTType, nReads int) (reads []crdt.ReadObjectParams) {
	reads = make([]crdt.ReadObjectParams, nReads)
	for i := 0; i < len(reads); i++ {
		rndKey, rndBucket := getRandomLocationParams()
		reads[i] = crdt.ReadObjectParams{
			KeyParams: crdt.MakeKeyParams(rndKey, crdtType, rndBucket),
		}
	}
	return
}

func testStaticUpdate(connection net.Conn) (receivedProto pb.Message) {
	updates := make([]crdt.UpdateObjectParams, 1)
	inc := 1 + rand.Int31n(100)
	rndKey, rndBucket := getRandomLocationParams()
	fmt.Println("Incrementing with:", inc)
	updates[0] = crdt.UpdateObjectParams{
		KeyParams:  crdt.MakeKeyParams(rndKey, proto.CRDTType_COUNTER, rndBucket),
		UpdateArgs: crdt.Increment{Change: inc},
	}

	proto := antidote.CreateStaticUpdateObjs(nil, updates)
	antidote.SendProto(antidote.StaticUpdateObjs, proto, connection)
	fmt.Println("Proto sent! Waiting for reply.")

	//Wait for reply
	protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Received type, proto: ", protoType, receivedProto)

	return
}

func testSet(connection net.Conn) {
	fmt.Println("Testing set add")
	adds := make([]crdt.UpdateArguments, 3)
	for i := 0; i < len(adds); i++ {
		adds[i] = crdt.Add{Element: crdt.Element(fmt.Sprint(rand.Uint64()))}
	}
	testGenericStaticUpdate(connection, proto.CRDTType_ORSET, adds)

	testStaticRead(connection, proto.CRDTType_ORSET, 5)

	fmt.Println("Testing set remove")
	rems := make([]crdt.UpdateArguments, 6)
	for i := 0; i < len(rems); i++ {
		rems[i] = crdt.Remove{Element: crdt.Element(fmt.Sprint(rand.Uint64()))}
	}
	testGenericStaticUpdate(connection, proto.CRDTType_ORSET, rems)

	testStaticRead(connection, proto.CRDTType_ORSET, 5)

	fmt.Println("Testing set addAll")
	addAll := make([]crdt.UpdateArguments, 1)
	var rndElems []crdt.Element
	for i := 0; i < len(addAll); i++ {
		rndElems = make([]crdt.Element, 5)
		for j := 0; j < len(rndElems); j++ {
			rndElems[j] = crdt.Element(fmt.Sprint(rand.Uint64()))
		}
		addAll[i] = crdt.AddAll{Elems: rndElems}
	}
	testGenericStaticUpdate(connection, proto.CRDTType_ORSET, addAll)

	testStaticRead(connection, proto.CRDTType_ORSET, 5)

	fmt.Println("Testing set remAll")
	remAll := make([]crdt.UpdateArguments, 2)
	for i := 0; i < len(remAll); i++ {
		rndElems = make([]crdt.Element, 2)
		for j := 0; j < len(rndElems); j++ {
			rndElems[j] = crdt.Element(fmt.Sprint(rand.Uint64()))
		}
		remAll[i] = crdt.RemoveAll{Elems: rndElems}
	}
	testGenericStaticUpdate(connection, proto.CRDTType_ORSET, remAll)

	testStaticRead(connection, proto.CRDTType_ORSET, 5)
}

func testGenericStaticUpdate(connection net.Conn, crdtType proto.CRDTType, args []crdt.UpdateArguments) {
	updates := createUpdateObjectParams(crdtType, args)
	proto := antidote.CreateStaticUpdateObjs(nil, updates)
	antidote.SendProto(antidote.StaticUpdateObjs, proto, connection)
	//fmt.Println("Proto update sent! Waiting for reply.")

	//Wait for reply
	antidote.ReceiveProto(connection)
	//protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	//fmt.Println("Received type, proto: ", protoType, receivedProto)
}

func testGenericUpdate(connection net.Conn, transId []byte, crdtType proto.CRDTType, args []crdt.UpdateArguments) {
	updates := createUpdateObjectParams(crdtType, args)
	proto := antidote.CreateUpdateObjs(transId, updates)
	antidote.SendProto(antidote.UpdateObjs, proto, connection)
	//fmt.Println("Proto update sent! Waiting for reply.")

	//Wait for reply
	antidote.ReceiveProto(connection)
	//protoType, receivedProto, _ := antidote.ReceiveProto(connection)
	//fmt.Println("Received type, proto: ", protoType, receivedProto)
}

func createUpdateObjectParams(crdtType proto.CRDTType, args []crdt.UpdateArguments) (updates []crdt.UpdateObjectParams) {
	updates = make([]crdt.UpdateObjectParams, len(args))
	for i := 0; i < len(args); i++ {
		rndKey, rndBucket := getRandomLocationParams()
		//fmt.Println("Generating update op to key, bucket", rndKey, rndBucket)
		updates[i] = crdt.UpdateObjectParams{
			KeyParams:  crdt.MakeKeyParams(rndKey, crdtType, rndBucket),
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
		startTransResp := receivedProto.(*proto.ApbStartTransactionResp)
		transId := startTransResp.GetTransactionDescriptor()

		fmt.Println("Starting to send operations...")
		createAndSendOps(connection, transId)

		fmt.Println("Sending commit...")
		commitTrans := antidote.CreateCommitTransaction(transId)
		antidote.SendProto(antidote.CommitTrans, commitTrans, connection)

		//Receive reply, check if it is commit or abort?
		_, receivedProto, _ = antidote.ReceiveProto(connection)
		commitReply := receivedProto.(*proto.ApbCommitResp)
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
	//protoType, writeProto := antidote.UpdateObjs, createCounterWrite(transId, counterKeys[0])
	protoType, writeProto := antidote.UpdateObjs, getNextWrite(transId, counterKeys[0], proto.CRDTType_COUNTER)
	fmt.Println("Sending write for", counterKeys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), writeProto, connection)
	replyType, replyProto, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType, replyProto)

	protoType, readProto := antidote.ReadObjs, createRead(transId, counterKeys[0], proto.CRDTType_COUNTER)
	fmt.Println("Sending read for", counterKeys[0])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), readProto, connection)
	replyType1, replyProto1, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType1, replyProto1)

	protoType, readProto = antidote.ReadObjs, createRead(transId, counterKeys[1], proto.CRDTType_COUNTER)
	fmt.Println("Sending read for", counterKeys[1])
	//reader.ReadString('\n')
	antidote.SendProto(byte(protoType), readProto, connection)
	replyType2, replyProto2, _ := antidote.ReceiveProto(connection)
	fmt.Println("Reply type, proto:", replyType2, replyProto2)
	//reader.ReadString('\n')
}

func debugWithTopk(connection net.Conn, transId []byte) {

	protoType, writeProto := antidote.UpdateObjs, getNextWrite(transId, keys[0], proto.CRDTType_TOPK)
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

	protoType, readProto := antidote.ReadObjs, getNextRead(transId, keys[0], proto.CRDTType_TOPK)
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

func testTopK(conns []net.Conn) {
	fmt.Println("*****TOPK (Top w/o removals) Test*****")
	entries, keyParams := make(map[int32]int32), crdt.MakeKeyParams("topknormv", proto.CRDTType_TOPK, "INDEX")
	for len(entries) < 300 {
		key, value := rand.Int31n(500), rand.Int31n(100)
		entries[key] = value
	}
	entriesSlice := make([]int32, len(entries))
	updP := make([]crdt.UpdateObjectParams, len(entries))
	i := 0
	data := &[]byte{}
	for id, value := range entries {
		updP[i] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: id, Score: value, Data: data}}}
		entriesSlice[i] = id
		i++
	}
	fmt.Printf("Sending %d TopKAdd updates\n", len(entries))
	txnId := clientLib.StaticUpdate(conns[0], nil, updP).GetCommitTime()
	time.Sleep(20 * time.Millisecond)
	readP := []crdt.ReadObjectParams{{KeyParams: keyParams}}
	readReply := clientLib.StaticRead(conns[0], txnId, readP).GetObjects().GetObjects()[0]
	state := crdt.ReadRespProtoToAntidoteState(readReply, proto.CRDTType_TOPK, proto.READType_FULL).(crdt.TopKValueState)

	sort.Slice(entriesSlice, func(i, j int) bool {
		return entries[entriesSlice[i]] > entries[entriesSlice[j]] ||
			(entries[entriesSlice[i]] == entries[entriesSlice[j]] && entriesSlice[i] > entriesSlice[j])
	})
	sort.Slice(state.Scores, func(i, j int) bool {
		return state.Scores[i].Score > state.Scores[j].Score ||
			(state.Scores[i].Score == state.Scores[j].Score && state.Scores[i].Id > state.Scores[j].Id)
	})

	fmt.Printf("Read result (len: %d):\n", len(state.Scores))
	fmt.Print("[")
	for _, topKScore := range state.Scores {
		fmt.Printf("{%d:%d}, ", topKScore.Id, topKScore.Score)
	}
	fmt.Println("]")
	fmt.Println("Entries sent:")
	fmt.Println("[")
	for _, id := range entriesSlice {
		fmt.Printf("{%d:%d}, ", id, entries[id])
	}
	fmt.Println("]")
	fmt.Println("Sleeping for a while and then requesting read on another replica.")
	time.Sleep(10000 * time.Millisecond)
	readReply = clientLib.StaticRead(conns[1], nil, readP).GetObjects().GetObjects()[0]
	state = crdt.ReadRespProtoToAntidoteState(readReply, proto.CRDTType_TOPK, proto.READType_FULL).(crdt.TopKValueState)
	sort.Slice(state.Scores, func(i, j int) bool {
		return state.Scores[i].Score > state.Scores[j].Score ||
			(state.Scores[i].Score == state.Scores[j].Score && state.Scores[i].Id > state.Scores[j].Id)
	})
	fmt.Println("Remote replica read result:")
	fmt.Print("[")
	for _, topKScore := range state.Scores {
		fmt.Printf("{%d:%d}, ", topKScore.Id, topKScore.Score)
	}
	fmt.Println("]")
}

/*
Tests all flag types (EW, DW, LWW)
Test 1: default read
Test 2: turn true, read
Test 3: turn false, read
Test 4: false for EW, true for DW
Test 5: 4x true, 1x false, read
Test 6: false for EW, true for DW
Test 6: 4x false, 1x true, read
Test 7: false for EW, true for DW
Test 8: 3x true, 2x false, read
*/
func testFlags(conns []net.Conn) {
	crdtTypes := []proto.CRDTType{proto.CRDTType_FLAG_EW, proto.CRDTType_FLAG_DW, proto.CRDTType_FLAG_LWW}
	resetFlag := []bool{false, true, false}
	flags := [][]bool{{true, true, false, true, true}, {false, true, false, false, false}, {true, false, true, false, true}}
	txnIDs := make([][]byte, len(conns))
	//readResults := make([]bool, len(conns))
	readResultsString := ""
	concurrencyTexts := []string{"4x true, 1x false", "1x true, 4x false", "3x true, 2x false"}

	for i, crdtType := range crdtTypes {
		printFlag := "[" + fmt.Sprintf("%v", crdtType) + "]"
		key := crdt.MakeKeyParams("flag", crdtType, "INDEX")
		readP := []crdt.ReadObjectParams{{KeyParams: key}}
		readReply := clientLib.StaticRead(conns[0], nil, readP)
		fmt.Println("##########Test: ", crdtType, "##########")
		fmt.Println("*****Initial Read*****")
		fmt.Println(printFlag, readReply.GetObjects().GetObjects()[0].GetFlag().GetValue())

		trueUpdParams := []crdt.UpdateObjectParams{{KeyParams: key, UpdateArgs: crdt.EnableFlag{}}}
		falseUpdParams := []crdt.UpdateObjectParams{{KeyParams: key, UpdateArgs: crdt.DisableFlag{}}}

		txnId := clientLib.StaticUpdate(conns[0], nil, trueUpdParams).GetCommitTime()
		time.Sleep(20 * time.Millisecond)
		readReply = clientLib.StaticRead(conns[0], txnId, readP)
		fmt.Println("*****Turn true*****")
		fmt.Println(printFlag, readReply.GetObjects().GetObjects()[0].GetFlag().GetValue())

		txnId = clientLib.StaticUpdate(conns[0], nil, falseUpdParams).GetCommitTime()
		time.Sleep(20 * time.Millisecond)
		readReply = clientLib.StaticRead(conns[0], txnId, readP)
		fmt.Println("*****Turn false*****")
		fmt.Println(printFlag, readReply.GetObjects().GetObjects()[0].GetFlag().GetValue())

		//Do concurrency tests: reset + concurrency
		for j, concFlags := range flags {
			//Reset
			txnId = staticUpdateFlag(conns[0], txnId, trueUpdParams, falseUpdParams, resetFlag[i])
			readReply = clientLib.StaticRead(conns[0], txnId, readP)
			fmt.Println("*****Reset (turn ", resetFlag[i], ")*****")
			fmt.Println(printFlag, readReply.GetObjects().GetObjects()[0].GetFlag().GetValue())
			//Wait for replication
			time.Sleep(5000 * time.Millisecond)

			//Concurrency test
			for k, flag := range concFlags {
				txnIDs[k] = staticUpdateFlag(conns[k], txnIDs[k], trueUpdParams, falseUpdParams, flag)
			}
			//Wait for replication
			time.Sleep(5000 * time.Millisecond)
			//Check read everywhere
			readResultsString = ""
			for k, conn := range conns {
				readResultsString += fmt.Sprintf("%v, ", clientLib.StaticRead(conn, txnIDs[k], readP).GetObjects().GetObjects()[0].GetFlag().GetValue())
			}
			fmt.Println("*****Concurrency test: ", concurrencyTexts[j])
			fmt.Println(printFlag, readResultsString)
			fmt.Println("(Expected: ", flagExpectedConcurrency(crdtType), ")")
		}
	}
	fmt.Println("Flag test complete")
}

func staticUpdateFlag(conn net.Conn, txnId []byte,
	trueUpdParams, falseUpdParams []crdt.UpdateObjectParams, isTrue bool) (newTxnId []byte) {
	if isTrue {
		return clientLib.StaticUpdate(conn, nil, trueUpdParams).GetCommitTime()
	}
	return clientLib.StaticUpdate(conn, nil, falseUpdParams).GetCommitTime()
}

func flagExpectedConcurrency(crdtType proto.CRDTType) string {
	if crdtType == proto.CRDTType_FLAG_EW {
		return "true"
	}
	if crdtType == proto.CRDTType_FLAG_DW {
		return "false"
	}
	if crdtType == proto.CRDTType_FLAG_LWW {
		return "clock-based"
	}
	return "error"
}

func testBoundedCounter(conns []net.Conn) {
	bc1Key := crdt.MakeKeyParams("bc1", proto.CRDTType_FATCOUNTER, "INDEX")
	bc2Key := crdt.MakeKeyParams("bc2", proto.CRDTType_FATCOUNTER, "INDEX")

	var bc1StartUpd crdt.UpdateArguments = crdt.SetCounterBound{Bound: 0, CompEq: false, InitialValue: 100}
	var bc2StartUpd crdt.UpdateArguments = crdt.SetCounterBound{Bound: -10, CompEq: true, InitialValue: 10}

	bc1UpdParams := []crdt.UpdateObjectParams{{KeyParams: bc1Key, UpdateArgs: bc1StartUpd}}
	bc2UpdParams := []crdt.UpdateObjectParams{{KeyParams: bc2Key, UpdateArgs: bc2StartUpd}}

	bc1TxnId := clientLib.StaticUpdate(conns[0], nil, bc1UpdParams).GetCommitTime()
	bc2TxnId := clientLib.StaticUpdate(conns[1], nil, bc2UpdParams).GetCommitTime()

	bc1ReadParams := []crdt.ReadObjectParams{{KeyParams: bc1Key}}
	bc2ReadParams := []crdt.ReadObjectParams{{KeyParams: bc2Key}}

	time.Sleep(200 * time.Millisecond)

	bc1ReadReply := clientLib.StaticRead(conns[0], bc1TxnId, bc1ReadParams)
	bc2ReadReply := clientLib.StaticRead(conns[1], bc2TxnId, bc2ReadParams)

	fmt.Println("*****Initial Read*****")
	fmt.Println("[BC1][R1]", bc1ReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	fmt.Println("[BC2][R2]", bc2ReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())

	//Try to do unallowed decrements in other replicas
	bc1StartUpd = crdt.Decrement{Change: 10}
	bothBcUpdParams := []crdt.UpdateObjectParams{{KeyParams: bc1Key, UpdateArgs: bc1StartUpd}, {KeyParams: bc2Key, UpdateArgs: bc1StartUpd}}
	bothReadParams := []crdt.ReadObjectParams{{KeyParams: bc1Key}, {KeyParams: bc2Key}}
	failedTxnId := clientLib.StaticUpdate(conns[2], nil, bothBcUpdParams).GetCommitTime()
	failedReadReply := clientLib.StaticRead(conns[2], failedTxnId, bothReadParams)

	fmt.Println()
	fmt.Println("*****Read after decrement without permissions*****")
	fmt.Println("[BC1][R3]", failedReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	fmt.Println("[BC2][R3]", failedReadReply.GetObjects().GetObjects()[1].GetCounter().GetValue())
	fmt.Println("Expected: 0, 0")

	//Sleep, try to do some decrements, observe result
	time.Sleep(5000 * time.Millisecond)
	//Request -10 on all replicas
	bc1UpdParams[0].UpdateArgs = bc1StartUpd
	fmt.Printf("[BC1]Requesting update on all replicas with args: %v+\n", bc1UpdParams[0].UpdateArgs)
	txnIDs := make([][]byte, len(conns))
	for i, conn := range conns {
		txnIDs[i] = clientLib.StaticUpdate(conn, nil, bc1UpdParams).GetCommitTime()
	}

	time.Sleep(5000 * time.Millisecond)
	bc1ReadReply = clientLib.StaticRead(conns[0], txnIDs[0], bc1ReadParams)
	fmt.Println()
	fmt.Println("*****Read after decrement with permissions*****")
	fmt.Println("[BC1][R1]", bc1ReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	fmt.Println("Expected: 50")

	//Try to do a decrement above limit and another on limit
	bc1StartUpd, bc2StartUpd = crdt.Decrement{Change: 15}, crdt.Decrement{Change: 7}
	bothBcUpdParams[0], bothBcUpdParams[1] = crdt.UpdateObjectParams{KeyParams: bc1Key, UpdateArgs: bc1StartUpd},
		crdt.UpdateObjectParams{KeyParams: bc1Key, UpdateArgs: bc2StartUpd}
	bc1TxnId = clientLib.StaticUpdate(conns[0], nil, bothBcUpdParams).GetCommitTime()
	bc1ReadReply = clientLib.StaticRead(conns[0], nil, bc1ReadParams)
	fmt.Println()
	fmt.Println("*****Read after 2 decrements - failed and successful one*****")
	fmt.Println("[BC1][R1]", bc1ReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	fmt.Println("Expected: 43")

	//Try to decrement 5 on the first replica. It will fail.
	//Wait a while, try again, and observe that it works
	//(This tests the background permission exchange)
	bc1StartUpd = crdt.Decrement{Change: 5}
	bc1UpdParams[0].UpdateArgs = bc1StartUpd
	clientLib.StaticUpdate(conns[0], bc1TxnId, bc1UpdParams)
	bc1ReadReply = clientLib.StaticRead(conns[0], nil, bc1ReadParams)
	fmt.Println()
	fmt.Println("*****Read on replica with low permissions - failed update****")
	fmt.Println("[BC1][R1]", bc1ReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	fmt.Println("Expected: 43")
	fmt.Println("Sleeping for a while, waiting for background permissions exchange...")
	time.Sleep(5000 * time.Millisecond)
	clientLib.StaticUpdate(conns[0], bc1TxnId, bc1UpdParams)
	bc1ReadReply = clientLib.StaticRead(conns[0], nil, bc1ReadParams)
	fmt.Println("Redone update.")
	fmt.Println("[BC1][R1]", bc1ReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	fmt.Println("Expected: 38")

	//2nd counter
	//Try to decrement below a replica's share, fail
	//Afterwards, decrement each replica's full share.
	//Wait for background permissions exchange, note that it still fails.
	bc2StartUpd = crdt.Decrement{Change: 5}
	bc2UpdParams[0].UpdateArgs = bc2StartUpd
	bc2TxnId = clientLib.StaticUpdate(conns[0], nil, bc2UpdParams).GetCommitTime()
	bc2ReadReply = clientLib.StaticRead(conns[0], nil, bc2ReadParams)
	fmt.Println("*****Read after decrement above permissions*****")
	fmt.Println("[BC2]", bc2ReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	fmt.Println("Expected: 10")
	bc2StartUpd = crdt.Decrement{Change: 4}
	bc2UpdParams[0].UpdateArgs = bc2StartUpd
	for i, conn := range conns {
		txnIDs[i] = clientLib.StaticUpdate(conn, nil, bc2UpdParams).GetCommitTime()
	}
	time.Sleep(5000 * time.Millisecond)
	bc2ReadReply = clientLib.StaticRead(conns[0], nil, bc2ReadParams)
	fmt.Println("*****Read after decrementing the max in every replica*****")
	fmt.Println("[BC2]", bc2ReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	fmt.Println("Expected: -10 (counter's bound)")
	fmt.Println("Sleeping, will try to decrement again (should not work)")
	bc2StartUpd = crdt.Decrement{Change: 1}
	bc2UpdParams[0].UpdateArgs = bc2StartUpd
	for i, conn := range conns {
		txnIDs[i] = clientLib.StaticUpdate(conn, nil, bc2UpdParams).GetCommitTime()
	}
	time.Sleep(5000 * time.Millisecond)
	bc2ReadReply = clientLib.StaticRead(conns[0], nil, bc2ReadParams)
	fmt.Println("*****Read after attempting further decrements in every replica*****")
	fmt.Println("[BC2]", bc2ReadReply.GetObjects().GetObjects()[0].GetCounter().GetValue())
	fmt.Println("Expected: -10")
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

func getNextOp(transId []byte) (protoType byte, protoBuf pb.Message) {
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

func getNextWrite(transId []byte, key string, crdtType proto.CRDTType) (updateBuf *proto.ApbUpdateObjects) {
	fmt.Println("Generating ApbUpdateObjects for", crdtType.String())
	var upd crdt.UpdateArguments
	switch crdtType {
	case proto.CRDTType_TOPK:
		upd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: rand.Int31n(maxId), Score: rand.Int31n(maxValue)}}

	case proto.CRDTType_TOPK_RMV:
		rndPlayer, rndValue := rand.Int31n(maxId), rand.Int31n(maxValue)
		isAdd := rand.Float32() < addProb
		if isAdd {
			upd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: rndPlayer, Score: rndValue}}
		} else {
			upd = crdt.TopKRemove{Id: rndPlayer}
		}

	case proto.CRDTType_ORSET:
		rndElem := elems[rand.Intn(len(elems))]
		if rand.Float32() < addProb {
			upd = crdt.Add{Element: crdt.Element(rndElem)}
		} else {
			upd = crdt.Remove{Element: crdt.Element(rndElem)}
		}

	case proto.CRDTType_COUNTER:
		//Negative inc as dec
		upd = crdt.Increment{Change: rand.Int31n(maxValue*2) - maxValue}

	case proto.CRDTType_LWWREG:
		upd = crdt.SetValue{NewValue: fmt.Sprint(rand.Intn(maxValue))}

	case proto.CRDTType_ORMAP:
		key := mapKeys[rand.Intn(len(mapKeys))]
		value := crdt.Element(elems[rand.Intn(len(elems))])
		isAdd := rand.Float32() < addProb
		if isAdd {
			upd = crdt.MapAdd{Key: key, Value: value}
		} else {
			upd = crdt.MapRemove{Key: key}
		}

	case proto.CRDTType_RRMAP:
		//NOTE: This is potencially very unefficient - use this only for testing/debugging!
		nOps := 1 + rand.Intn(maxEmbUpdates)
		isAdd := rand.Float32() < rrMapAddProb
		adds := make(map[string]*proto.ApbUpdateOp)
		rems := make(map[string]struct{})

		if isAdd {
			fmt.Println("Generating add for embMap")
			for len(adds) < nOps {
				innerType := embTypes[rand.Intn(len(embTypes))]
				fmt.Println("Generated CRDTType for emb:", innerType)
				innerKey := mapKeys[rand.Intn(len(mapKeys))] + innerType.String()
				if _, hasAdd := adds[innerKey]; !hasAdd {
					adds[innerKey] = getNextWrite(transId, innerKey, innerType).GetUpdates()[0]
				} else {
					fmt.Println("Ignored latest", innerType, "as there was already an entry with the same key")
				}
			}
			fmt.Println("Checking adds")
			for key, value := range adds {
				fmt.Println(key, ":", value.GetBoundobject().GetType().String())
			}
		} else {
			fmt.Println("Generating remove for embMap")
			for len(rems) < nOps {
				innerKey := mapKeys[rand.Intn(len(mapKeys))] + embTypes[rand.Intn(len(embTypes))].String()
				if _, hasRem := rems[innerKey]; !hasRem {
					rems[innerKey] = struct{}{}
				}
			}
		}
		return &proto.ApbUpdateObjects{
			TransactionDescriptor: transId,
			Updates: []*proto.ApbUpdateOp{{
				Boundobject: &proto.ApbBoundObject{Key: []byte(key), Type: &crdtType, Bucket: []byte(bucket)},
				Operation:   &proto.ApbUpdateOperation{Mapop: crdt.CreateMapUpdateFromProto(isAdd, adds, rems)},
			}}}
		//writeBuf = crdt.CreateMapUpdateFromProto(isAdd, adds, rems)

	case proto.CRDTType_AVG:
		upd = crdt.AddValue{Value: rand.Int63n(maxValue)}

	case proto.CRDTType_MAXMIN:
		if isMax {
			upd = crdt.MaxAddValue{Value: rand.Int63n(maxValue)}
		} else {
			upd = crdt.MinAddValue{Value: rand.Int63n(maxValue)}
		}
	}

	updParams := []crdt.UpdateObjectParams{{KeyParams: crdt.MakeKeyParams(key, crdtType, bucket), UpdateArgs: upd}}
	return antidote.CreateUpdateObjs(transId, updParams)
	//fmt.Println("Finished generating ApbUpdateObjects for", crdtType.String())
	//return antidote.CreateUpdateObjs(transId, key, crdtType, bucket, writeBuf)
}

func getNextRead(transId []byte, key string, crdtType proto.CRDTType) (readBuf *proto.ApbReadObjects) {
	return createRead(transId, key, crdtType)
}

func createRead(transId []byte, key string, crdtType proto.CRDTType) (readBuf *proto.ApbReadObjects) {
	return antidote.CreateSingleReadObjs(transId, key, crdtType, bucket)
}

func getRandomLocationParams() (key string, bucket string) {
	return keys[rand.Intn(len(keys))], buckets[rand.Intn(len(buckets))]
}

// For every key combination, checks if all servers have the same results.
// Only works for sets as of now.
// TODO: Divide this in submethods
func verifyReplication(crdtType proto.CRDTType) {
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
		readParams := make([]crdt.ReadObjectParams, 0, nObjects)
		for _, bucket := range buckets {
			for _, key := range keys {
				readParams = append(readParams, crdt.ReadObjectParams{KeyParams: crdt.MakeKeyParams(key, crdtType, bucket)})
			}
		}
		fmt.Println("Requesting read for:", readParams)
		protobuf := antidote.CreateStaticReadObjs(nil, readParams)
		antidote.SendProto(antidote.StaticReadObjs, protobuf, conn)

		//Receiving reply and decoding it
		_, reply, _ := antidote.ReceiveProto(conn)
		typedReply := reply.(*proto.ApbStaticReadObjectsResp)
		fmt.Println("Received proto reply with objects:", typedReply.GetObjects())
		for _, objProto := range typedReply.GetObjects().GetObjects() {
			results[serverID] = append(results[serverID], crdt.ReadRespProtoToAntidoteState(objProto, crdtType, proto.READType_FULL))
		}
		fmt.Println("")
	}

	//Print reads
	for i := range results[0] {
		fmt.Printf("Object %d states: \n", i)
		for serverId, results := range results {
			fmt.Printf("\tServer%d: %s\n", serverId, utilities.StateToString(results[i]))
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
	case crdt.EmbMapEntryState:
		ok, reason = checkEmbMapStatesEquality(typedFirstState, secondState.(crdt.EmbMapEntryState))
	case crdt.EmbMapHasKeyState:
		ok, reason = checkEmbMapHasKeyStatesEquality(typedFirstState, secondState.(crdt.EmbMapHasKeyState))
	case crdt.EmbMapGetValueState:
		ok, reason = checkEmbMapGetValueStatesEquality(typedFirstState, secondState.(crdt.EmbMapGetValueState))
	case crdt.EmbMapKeysState:
		ok, reason = checkEmbMapKeysStatesEquality(typedFirstState, secondState.(crdt.EmbMapKeysState))
	case crdt.TopKValueState:
		ok, reason = checkTopKStatesEquality(typedFirstState, secondState.(crdt.TopKValueState))
	case crdt.AvgState:
		ok, reason = checkAvgStatesEquality(typedFirstState, secondState.(crdt.AvgState))
	case crdt.MaxMinState:
		ok, reason = checkMaxMinStatesEquality(typedFirstState, secondState.(crdt.MaxMinState))
	}
	return
}

func checkCounterStatesEquality(firstState crdt.CounterState, secondState crdt.CounterState) (ok bool, reason string) {
	return firstState == secondState, "Different counter values!"
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

func checkEmbMapStatesEquality(firstState crdt.EmbMapEntryState, secondState crdt.EmbMapEntryState) (ok bool, reason string) {
	if len(firstState.States) != len(secondState.States) {
		return false, "Results don't match - number of keys in one of the Map states is different!"
	}
	for key, state := range firstState.States {
		otherEmbState, has := secondState.States[key]
		if !has {
			return false, "Results don't match - missing a key!"
		}
		ok, reason = checkStatesEquality(state, otherEmbState)
		if !ok {
			return false, "Results don't match - different values for the same key! Inner reason: " + reason
		}
	}
	return true, ""
}

func checkEmbMapHasKeyStatesEquality(firstState crdt.EmbMapHasKeyState, secondState crdt.EmbMapHasKeyState) (ok bool, reason string) {
	return firstState.HasKey == secondState.HasKey, "Different hasKey results!"
}

func checkEmbMapGetValueStatesEquality(firstState crdt.EmbMapGetValueState, secondState crdt.EmbMapGetValueState) (ok bool, reason string) {
	if firstState.State.GetCRDTType() != secondState.State.GetCRDTType() {
		return false, "Results don't match - different CRDTTypes in getValue!"
	}
	return checkStatesEquality(firstState.State, secondState.State)
}

func checkEmbMapKeysStatesEquality(firstState crdt.EmbMapKeysState, secondState crdt.EmbMapKeysState) (ok bool, reason string) {
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
	nOpsRange := maxOpsPerTrans - minOpsPerTrans
	elems := generateRandomElems(100)

	rand.Seed(0)
	finishChan := make(chan int64)
	totalTime := int64(0)

	for i := 0; i < nClients; i++ {
		conn, err := net.Dial("tcp", servers[i%len(servers)])
		utilities.CheckErr("Network connection establishment err", err)
		args := &BenchmarkArgs{
			clientID:  i,
			nOpsRange: nOpsRange,
			crdtTypes: embTypes,
			keys:      keys,
			elements:  elems,
		}
		go executeStaticBenchmark(conn, args, finishChan)
		//go executeNonStaticBenchmark(conn, args, finishChan)
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

	//Comment this when actually benchmarking
	verifyReplication(testCrdtType)
}

func executeNonStaticBenchmark(connection net.Conn, args *BenchmarkArgs, finishChan chan int64) {
	startTime := time.Now().UTC().UnixNano()
	var previousTs []byte = nil
	//var updArgs []crdt.UpdateArguments
	for nDone := 0; nDone < targetTrans; nDone += 1 {
		//Start txn
		antidote.SendProto(antidote.StartTrans, antidote.CreateStartTransaction(previousTs), connection)
		_, receivedProto, _ := antidote.ReceiveProto(connection)
		startTransResp := receivedProto.(*proto.ApbStartTransactionResp)
		previousTs = startTransResp.GetTransactionDescriptor()

		nOps := rand.Intn(args.nOpsRange) + minOpsPerTrans
		nWrites := int(float32(nOps) * writeProb)
		nReads := nOps - nWrites

		if nWrites > 0 {
			testGenericUpdate(connection, previousTs, testCrdtType, getRandomUpdateArgs(nWrites, testCrdtType, args))
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
		if nDone+1%(int(math.Max(1, targetTrans/10))) == 0 {
			fmt.Printf("Client %d completed txn number %d.\n", args.clientID, nDone+1)
		}
		_, receivedProto, _ = antidote.ReceiveProto(connection)
		commitReply := receivedProto.(*proto.ApbCommitResp)
		previousTs = commitReply.GetCommitTime()
	}
	totalTime := time.Now().UTC().UnixNano() - startTime
	finishChan <- totalTime
}

func executeStaticBenchmark(connection net.Conn, args *BenchmarkArgs, finishChan chan int64) {
	startTime := time.Now().UTC().UnixNano()
	//var updArgs []crdt.UpdateArguments
	for nDone := 0; nDone < targetTrans; nDone += 1 {

		nOps := rand.Intn(args.nOpsRange) + minOpsPerTrans
		nWrites := int(float32(nOps) * writeProb)
		nReads := nOps - nWrites

		if nWrites > 0 {
			testGenericStaticUpdate(connection, testCrdtType, getRandomUpdateArgs(nWrites, testCrdtType, args))
			//fmt.Println(clientID, "Update")
			//testGenericStaticUpdate(connection, testCrdtType, updArgs)
			//fmt.Println(clientID, "Finish update")
			//Send writes
		}

		if nReads > 0 {
			//fmt.Println(clientID, "Read")
			testStaticRead(connection, testCrdtType, nReads)
			//fmt.Println(clientID, "Finish read")
		}
		//fmt.Printf("Client %d finished processing txn %d.\n", clientID, nDone)
		if nDone+1%(int(math.Max(1, targetTrans/10))) == 0 {
			fmt.Printf("Client %d completed txn number %d.\n", args.clientID, nDone+1)
		}
	}
	totalTime := time.Now().UTC().UnixNano() - startTime
	finishChan <- totalTime
}

func getRandomUpdateArgs(nWrites int, crdtType proto.CRDTType, args *BenchmarkArgs) (updArgs []crdt.UpdateArguments) {
	switch crdtType {
	case proto.CRDTType_COUNTER:
		updArgs = getRandomCounterUpdateArgs(nWrites)
	case proto.CRDTType_LWWREG:
		updArgs = getRandomRegisterUpdateArgs(nWrites)
	case proto.CRDTType_ORSET:
		updArgs = getRandomSetUpdateArgs(nWrites, args)
	case proto.CRDTType_ORMAP:
		updArgs = getRandomMapUpdateArgs(nWrites, args)
	case proto.CRDTType_RRMAP:
		updArgs = getRandomEmbMapUpdateArgs(nWrites, args)
	case proto.CRDTType_TOPK_RMV:
		updArgs = getRandomTopKRmvUpdateArgs(nWrites)
	case proto.CRDTType_AVG:
		updArgs = getRandomAvgUpdateArgs(nWrites)
	case proto.CRDTType_MAXMIN:
		updArgs = getRandomMaxMinUpdateArgs(nWrites)
	}
	return
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

func getRandomSetUpdateArgs(nWrites int, args *BenchmarkArgs) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	for i := 0; i < nWrites; i++ {
		if rand.Float64() < addProb {
			updArgs[i] = crdt.Add{Element: args.elements[rand.Intn(len(args.elements))]}
		} else {
			updArgs[i] = crdt.Remove{Element: args.elements[rand.Intn(len(args.elements))]}
		}
	}
	return
}

func getRandomMapUpdateArgs(nWrites int, args *BenchmarkArgs) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	for i := 0; i < nWrites; i++ {
		if rand.Float64() < addProb {
			updArgs[i] = crdt.MapAdd{Key: args.keys[rand.Intn(len(args.keys))], Value: args.elements[rand.Intn(len(args.elements))]}
		} else {
			updArgs[i] = crdt.MapRemove{Key: args.keys[rand.Intn(len(args.keys))]}
		}
	}
	return
}

func getRandomEmbMapUpdateArgs(nWrites int, args *BenchmarkArgs) (updArgs []crdt.UpdateArguments) {
	updArgs = make([]crdt.UpdateArguments, nWrites)
	for i := 0; i < nWrites; i++ {
		if rand.Float64() < addProb {
			//crdtType := args.crdtTypes[rand.Intn(len(args.crdtTypes))]
			//fmt.Println("Using inner type", crdtType)
			//TODO: Actually need to restrict this - we can generate updates of two different CRDT types for the same key!
			//For now, I replaced crdtType with testCrdtType
			//This can still crash if the first update is remove (ORMap) while doing RRMap.
			updArgs[i] = crdt.EmbMapUpdate{
				Key: args.keys[rand.Intn(len(args.keys))],
				Upd: getRandomUpdateArgs(1, testCrdtType, args)[0],
			}
			fmt.Printf("Actually generated: %T %v\n", updArgs[i].(crdt.EmbMapUpdate).Upd, updArgs[i].(crdt.EmbMapUpdate).Upd)
		} else {
			updArgs[i] = crdt.MapRemove{Key: args.keys[rand.Intn(len(args.keys))]}
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
