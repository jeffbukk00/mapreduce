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
type connectionStatus int

const (
	Unconnected connectionStatus = iota
	Connected
)

type jobProgressionStatus int

const (
	Unscheduled jobProgressionStatus = iota
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
	class             TaskClass
	seq               int
	progressionStatus jobProgressionStatus
	inputPath         []string
	requiredInputs    int
}

// ------------------------
// Worker-related type definitions
// ------------------------
type workerProfile struct {
	id int
}

type workerState struct {
	profile          workerProfile
	connectionStatus connectionStatus
	currentJob       job
	mapf             mapFunc
	reducef          reduceFunc
}

type Worker struct {
	state workerState

	cmdChan chan func()
}

func (w *Worker) Run() {
	for cmd := range w.cmdChan {
		cmd()
	}
}

type getConnectionStatusResp struct {
	status connectionStatus
	err    error
}

func (w *Worker) GetConnectionStatus() getConnectionStatusResp {
	respChan := make(chan getConnectionStatusResp, 1)

	w.cmdChan <- func() {

		// Required: error cases
		// Invariants:
		// Retry:

		respChan <- getConnectionStatusResp{status: w.state.connectionStatus, err: nil}
	}

	return <-respChan
}

type assignJobReq struct {
	workerID int
}

type assignJobResp struct {
	err error
}

func (w *Worker) AssignJob(req assignJobReq) assignJobResp {
	resp := make(chan assignJobResp, 1)

	w.cmdChan <- func() {

		// Required: error cases
		// Invariants:
		// Retry:

		w.state.profile.id = req.workerID
		w.state.connectionStatus = Connected

		resp <- assignJobResp{err: nil}
	}

	return <-resp
}

// ------------------------
// RPC client definitions for a worker. Separation of networking concerns.
// ------------------------

// WorkerRPC represents a RPC client.
type WorkerRPC struct {
	w *Worker
}

// Eventloop iterates to call a series of RPC requests to the coordinator
// in the strict order.
// Worker state transitions internally by each RPC call.
// Main thread runs the event loop.
// It is independent from the actor thread, which
// accesses and modifies private worker state.
func (wrpc *WorkerRPC) Eventloop() {

	log.Println("<INFO> Start the event loop")
	for {
		wrpc.ConnectRPC()

		// ...

		for {
			time.Sleep(30 * time.Second) // Simulates a loop.
		}
	}

	// log.Println("<INFO> Terminate the event loop")
}

func (wrpc *WorkerRPC) ConnectRPC() {
	respFromGetConnectionStatus := wrpc.w.GetConnectionStatus()

	if respFromGetConnectionStatus.err != nil {
		// Required: error cases
		// Invariants:
		// Retry:

	}

	if respFromGetConnectionStatus.err == nil && respFromGetConnectionStatus.status == Connected {
		return
	}

	args := AcceptWorkerArgs{}
	reply := AcceptWorkerReply{}

	ok := wrpc.call(RPCAcceptWorker, &args, &reply)

	if !ok {
		log.Println("<ERROR> Failed to connect to the coordinator")
		return
	}

	respFromAssignJob := wrpc.w.AssignJob(assignJobReq{workerID: reply.WorkerID})

	if respFromAssignJob.err != nil {
		// Required: error cases
		// Invariants:
		// Retry:
	}

	// Update the global logger with assigned worker id.
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
		state: workerState{
			profile:          workerProfile{},
			connectionStatus: Unconnected,
			currentJob: job{
				progressionStatus: Unscheduled,
			},
			mapf:    mapf,
			reducef: reducef,
		},

		cmdChan: make(chan func(), 100),
	}

	wRPC := WorkerRPC{w: &w}

	log.Println("<INFO> Initialized the worker")

	go w.Run()

	wRPC.Eventloop()
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
