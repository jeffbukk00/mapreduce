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
	CoordinatorConnect        = "CoordinatorService.Connect"
	CoordinatorSchedule       = "CoordinatorService.Schedule"
	CoordinatorFetchInputPath = "CoordinatorService.FetchInputPath"
	coordinatorCommitoutput   = "CoordinatorService.CommitOutput"
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

// ConnectReply is a return type of RPC method "CoordinatorService.Connect"
type ConnectReply struct {
	Profile WorkerProfile
}

// ScheduleArgs is an argument type of RPC method "CoordinatorService.Schedule"
type ScheduleArgs struct {
	Profile WorkerProfile
}

// ScheduleReply is a return type of RPC method "CoordinatorService.Schedule"
type ScheduleReply struct {
	Task AssignedTask
}

// FetchInputPathArgs is an argument type of RPC method "CoordinatorService.FetchInputPath"
type FetchInputPathArgs struct {
	Profile     WorkerProfile
	Task        AssignedTask
	WhatToFetch []int
}

// FetchInputPathReply is a return type of RPC method "CoordinatorService.FetchInputPath"
type FetchInputPathReply struct {
	InputPaths []Input
}

// CommitOutputArgs is an argument type of RPC method "CoordinatorService.CommitOutput"
type CommitOutputArgs struct {
	Profile    WorkerProfile
	Task       AssignedTask
	OutputPath []string
}

// CommitOutputReply is a return type of RPC method "CoordinatorService.CommitOutput"
type CommitOutputReply struct{}

// ------------------------
// Args and reply type definitions for signaling
// ------------------------

// PingArgs is an argument type of RPC method "SignalService.Ping"
type PingArgs struct {
	WorkerID int
}

// PingReply is a return type of RPC method "SignalService.Ping"
type PingReply struct {
	Resp PingResponse
}

func CoordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
