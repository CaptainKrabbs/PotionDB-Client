package clientLib

import (
	"net"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	antidote "potionDB/potionDB/components"
	"sqlToKeyValue/src/parser"
	"sqlToKeyValue/src/sql"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
)

func StartTxn(conn net.Conn) (txnId []byte) {
	antidote.SendProto(antidote.StartTrans, antidote.CreateStartTransaction(nil), conn)
	//Receive transaction ID
	_, receivedProto, _ := antidote.ReceiveProto(conn)
	startTransResp := receivedProto.(*proto.ApbStartTransactionResp)
	return startTransResp.GetTransactionDescriptor()
}

func CommitTxn(conn net.Conn, txnId []byte) (newTxnId []byte) {
	commitTrans := antidote.CreateCommitTransaction(txnId)
	antidote.SendProto(antidote.CommitTrans, commitTrans, conn)
	_, receivedProto, _ := antidote.ReceiveProto(conn)
	commitReply := receivedProto.(*proto.ApbCommitResp)
	return commitReply.GetCommitTime()
}

func Read(conn net.Conn, txnId []byte, readParams []crdt.ReadObjectParams) (reply *proto.ApbReadObjectsResp) {
	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conn)
	_, rep, _ := antidote.ReceiveProto(conn)
	return rep.(*proto.ApbReadObjectsResp)
}

func Update(conn net.Conn, txnId []byte, updParams []crdt.UpdateObjectParams) {
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conn)
	antidote.ReceiveProto(conn)
}

func StaticRead(conn net.Conn, previousTxnId []byte, readParams []crdt.ReadObjectParams) (reply *proto.ApbStaticReadObjectsResp) {
	antidote.SendProto(antidote.StaticReadObjs, antidote.CreateStaticReadObjs(previousTxnId, readParams), conn)
	_, rep, _ := antidote.ReceiveProto(conn)
	return rep.(*proto.ApbStaticReadObjectsResp)
}

func StaticUpdate(conn net.Conn, previousTxnId []byte, updParams []crdt.UpdateObjectParams) (reply *proto.ApbCommitResp) {
	antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(previousTxnId, updParams), conn)
	_, rep, _ := antidote.ReceiveProto(conn)
	return rep.(*proto.ApbCommitResp)
}

func CreateTable(conn net.Conn, sqlCode string) {
	parser := getAntlrParser(sqlCode)
	listener := sql.MakeCreateTableListener(parser)
	antlr.ParseTreeWalkerDefault.Walk(listener, parser.Start())
	antidote.SendProto(antidote.SQLTyped, listener.ToProtobuf(), conn)
	antidote.ReceiveProto(conn) //TODO
}

func getAntlrParser(sqlCode string) *parser.ViewSQLParser {
	is := antlr.NewInputStream(sqlCode)
	lexer := parser.NewViewSQLLexer(is)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	return parser.NewViewSQLParser(stream)
}
