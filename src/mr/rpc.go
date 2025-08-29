package mr

import (
	"os"
	"strconv"
)

// ------------------------
// RPC method name constants
// Used to avoid hard-coded strings
// ------------------------

const (
	RPCConnect = "CoordinatorRPC.Connect"
)

// ------------------------
// Args and reply type definitions for RPC methods
// ------------------------

// ConnectArgs is an argument type of RPC method "CoordinatorRPC.Connect"
type ConnectArgs struct{}

// ConnectReply is an return type of RPC method "CoordinatorRPC.Connect"
type ConnectReply struct {
	WorkerID int
}

func CoordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
