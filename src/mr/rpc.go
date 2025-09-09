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
	CoordinatorSchedule       = "CoordinatorService.Schedule"
	CoordinatorFetchInputPath = "CoordinatorService.FetchInputPath"
)

const (
	// An example of signal service method name.
	SignalPing = "SignalService.Ping"
)

// ------------------------
// Args and reply type definitions for general RPC methods
// ------------------------

// ScheduleArgs is an argument type of RPC method "CoordinatorService.Schedule"
type ScheduleArgs struct {
}

// ScheduleReply is an return type of RPC method "CoordinatorService.Schedule"
type ScheduleReply struct {
	Profile WorkerProfile
	Task    AssignedTask
}

// FetchInputPathArgs is an argument type of RPC method "CoordinatorService.FetchInputPath"
type FetchInputPathArgs struct {
	Profile     WorkerProfile
	Task        AssignedTask
	WhatToFetch []int
}

// FetchInputPathReply is an return type of RPC method "CoordinatorService.FetchInputPath"
type FetchInputPathReply struct {
	InputPaths []Input
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
