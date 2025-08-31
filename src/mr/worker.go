package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// ------------------------
// Private constants
// ------------------------
type connectionState int

const (
	Unconnected connectionState = iota
	Connected
)

type jobProgressionState int

const (
	Unscheduled jobProgressionState = iota
	Scheduled
	InputFetched
	TaskProcessed
	OutputCommitted
)

// ------------------------
// Type definitions for user-defined map and reduce functions
// ------------------------

// KeyValue represents a KV type definition for representing the map function's intermidiate KV outputs.
type KeyValue struct {
	Key   string
	Value string
}

// mapFunc defines the signature for a user-defined map function.
// reduceFunc defines the signature for a user-defined reduce function.
// These are dynamically loaded through the Go plugin system.
type mapFunc func(string, string) []KeyValue
type reduceFunc func(string, []string) string

// ------------------------
// Job-related type definitions
// ------------------------
type job struct {
	class            TaskClass
	seq              int
	progressionState jobProgressionState
	inputPath        []string
	requiredInputs   int
}

// ------------------------
// Worker-related type definitions
// ------------------------
type workerProfile struct {
	id              int
	connectionState connectionState
}

type workerContext struct {
	profile workerProfile
	mapf    mapFunc
	reducef reduceFunc
}

type Worker struct {
	context    workerContext
	currentJob job
	rpcClient  *WorkerRPC
}

func (w *Worker) eventLoop() {
	log.Println("Starts the eventloop")

	for {
		if w.context.profile.connectionState == Unconnected {
			w.rpcClient.connectRPC()
		}

		for {
			time.Sleep(30 * time.Second)
		}
	}
}

// ------------------------
// RPC client definitions for a worker. Separation of networking concerns.
// ------------------------

// WorkerRPC represents a RPC client.
type WorkerRPC struct {
	w *Worker
}

func (wrpc *WorkerRPC) connectRPC() {
	args := AcceptWorkerArgs{}

	reply := AcceptWorerReply{}

	ok := wrpc.call(RPCAcceptWorker, &args, &reply)

	if !ok {
		log.Println("<ERROR> Failed to connect to the coordinator")
		return
	}

	wrpc.w.context.profile.id = reply.WorkerID
	wrpc.w.context.profile.connectionState = Connected

	prefix := fmt.Sprintf("[ WORKER | PID: %d | ID: %d ] ", os.Getpid(), reply.WorkerID)
	log.SetPrefix(prefix)
	log.Printf("<INFO> Worker %d is Connected", reply.WorkerID)
}

func (wrpc *WorkerRPC) call(rpcname string, args interface{}, reply interface{}) bool {

	sockname := CoordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// ------------------------
// Initialization of a worker process
// ------------------------

// MakeWorker initializes data structures for internal service state. This is the actual entry point of the worker process.
func MakeWorker(mapf mapFunc, reducef reduceFunc) {
	// Logger initialization
	prefix := fmt.Sprintf("[ WORKER | PID: %d | ID: unassigned ] ", os.Getpid())
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix(prefix)

	w := Worker{
		context: workerContext{
			profile: workerProfile{
				connectionState: Unconnected,
			},

			mapf:    mapf,
			reducef: reducef,
		},
		currentJob: job{
			progressionState: Unscheduled,
		},
		rpcClient: &WorkerRPC{},
	}

	w.rpcClient.w = &w

	log.Println("<INFO> Initialized the worker")

	w.eventLoop()
}

// ------------------------
//
//	Utility functions
//
// ------------------------
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
