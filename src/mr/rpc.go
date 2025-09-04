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
	CoordinatorConnect  = "CoordinatorService.Connect"
	CoordinatorSchedule = "CoordinatorService.Schedule"
)

const (
	// An example of signal service method name.
	SignalAction = "SignalService.Action"
)

// ------------------------
// Args and reply type definitions for general RPC methods
// ------------------------

// ConnectArgs is an argument type of RPC method "CoordinatorRPC.Connect"
type ConnectArgs struct{}

// ConnectReply is an return type of RPC method "CoordinatorRPC.Connect"
type ConnectReply struct {
	Profile WorkerProfile
}

// ScheduleArgs is an argument type of RPC method "CoordinatorRPC.Connect"
type ScheduleArgs struct {
	WorkerID int
}

// ScheduleReply is an return type of RPC method "CoordinatorRPC.Connect"
type ScheduleReply struct {
	Task Job
}

// ------------------------
// Args and reply type definitions for signaling
// ------------------------

// ActionArgs ...
type ActionArgs struct{}

// ActionReply ...
type ActionReply struct{}

func CoordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
