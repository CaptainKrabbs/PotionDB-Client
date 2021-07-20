package clientLib

import (
	"net"
	"potionDB/src/antidote"
	"potionDB/src/proto"
)

func StartTxn(conn net.Conn) (txnId []byte) {
	antidote.SendProto(antidote.StartTrans, antidote.CreateStartTransaction(nil), conn)
	//Receive transaction ID
	_, receivedProto, _ := antidote.ReceiveProto(conn)
	startTransResp := receivedProto.(*proto.ApbStartTransactionResp)
	return startTransResp.GetTransactionDescriptor()
}

func CommitTxn(conn net.Conn, txnId []byte) (success bool) {
	commitTrans := antidote.CreateCommitTransaction(txnId)
	antidote.SendProto(antidote.CommitTrans, commitTrans, conn)
	_, receivedProto, _ := antidote.ReceiveProto(conn)
	commitReply := receivedProto.(*proto.ApbCommitResp)
	return commitReply.GetSuccess()
}
