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
	CoordinatorService = "CoordinatorService"
	SignalService      = "SignalService"
)

const (
	CoordinatorConnect  = "CoordinatorService.Connect"
	CoordinatorSchedule = "CoordinatorService.Schedule"
)

const (
	// An example of signal service method name.
	SignalPing = "SignalService.Ping"
)

// ------------------------
// Args and reply type definitions for general RPC methods
// ------------------------

// ConnectArgs is an argument type of RPC method "CoordinatorService.Connect"
type ConnectArgs struct{}

// ConnectReply is an return type of RPC method "CoordinatorService.Connect"
type ConnectReply struct {
	Profile WorkerProfile
}

// ScheduleArgs is an argument type of RPC method "CoordinatorService.Connect"
type ScheduleArgs struct {
	WorkerID int
}

// ScheduleReply is an return type of RPC method "CoordinatorService.Connect"
type ScheduleReply struct {
	Task Job
}

// ------------------------
// Args and reply type definitions for signaling
// ------------------------

// PingArgs is an argument type of RPC method "SignalService.Ping"
type PingArgs struct {
	WorkerID int
}

// PingReply is an return type of RPC method "SignalService.Ping"
type PingReply struct {
	Resp PingResponse
}

func CoordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
