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
	ShuffleService     = "ShuffleService"
)

const (
	CoordinatorConnect        = "CoordinatorService.Connect"
	CoordinatorSchedule       = "CoordinatorService.Schedule"
	CoordinatorFetchInputPath = "CoordinatorService.FetchInputPath"
	coordinatorCommitoutput   = "CoordinatorService.CommitOutput"
)

const (
	SignalPing = "SignalService.Ping"
)

const (
	ShuffleFetch = "ShuffleService.FetchForShuffle"
)

// ------------------------
// Args and reply type definitions for general RPC methods
// ------------------------

// ConnectArgs is an argument type of RPC method "CoordinatorService.Connect"
type ConnectArgs struct {
}

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
	InputPaths []Path
}

// CommitOutputArgs is an argument type of RPC method "CoordinatorService.CommitOutput"
type CommitOutputArgs struct {
	Profile    WorkerProfile
	Task       AssignedTask
	OutputPath []Path
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

// ------------------------
// Args and reply type definitions for shuffling
// ------------------------

// FetchForShuffleArgs is an argument type of RPC method "ShuffleService.FetchForShuffle"
type FetchForShuffleArgs struct {
	Filename string
	Offset   int64
	Size     int
}

// FetchForShuffleReply is a return type of RPC method "ShuffleService.FetchForShuffle"
type FetchForShuffleReply struct {
	Data   []byte
	Offset int64
	EOF    bool
}

func CoordinatorSock() string {
	s := "/var/tmp/mr-coord-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func ShuffleServerSock() string {
	s := "/var/tmp/mr-shuffle-"
	s += strconv.Itoa(os.Getuid()) + "-"
	s += strconv.Itoa(os.Getpid())

	return s
}
