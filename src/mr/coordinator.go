// Package mr represents for the mapreduce/src/ framework implementation.
package mr

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

// ------------------------
// Private constants
// ------------------------

type taskProgressionStatus int

const (
	Idle taskProgressionStatus = iota
	Pending
	Completed
)

type workerLivenessStatus int

const (
	Alive workerLivenessStatus = iota
	Dead
)

type workerActivenessStatus int

const (
	Inactive workerActivenessStatus = iota // Inactive status represents "unscheduled with any task currently".
	Active
)

// ------------------------
// Task-related type definitions
// ------------------------
type task struct {
	class             TaskClass
	seq               int
	progressionStatus taskProgressionStatus
	inputPath         []string
	requiredInputs    int
	scheduledWith     []*worker // Multiple workers can be scheduled with same task. As backup tasks for speculative execution.

	// A task can be completed by only one worker.
	// This field is not overwritten by backup completions,
	// ensuring deduplication under speculative execution.
	// The only exception is when rescheduling after the
	// original completing worker has failed.
	completedWith *worker
}

// taskSet tracks the state of all tasks in one place.
// The number of tasks is fixed and does not change dynamically.
type taskSet struct {
	mapTasksToComplete    int
	mapTasks              []*task
	reduceTasksToComplete int
	reduceTasks           []*task
}

type taskQueue struct {
	q []*task
}

// ------------------------
// Worker-related type definitions
// ------------------------
type worker struct {
	id               int
	livenessStatus   workerLivenessStatus
	activenessStatus workerActivenessStatus
	scheduledWith    *task
	tasksCompleted   []*task
}

// workerSet tracks the state of all workers in one place.
// It is mainly required by the ping handler, which needs a simple way to look up any worker's state.
type workerSet struct {
	numWorkers int
	workers    []*worker
}

// ------------------------
// Coordinator-related type definitions
// ------------------------

// Private state of a coordinator
type coordinatorState struct {
	taskFiniteSet   taskSet
	workerFiniteSet workerSet
	idleTaskQueue   taskQueue
}

// Coordinator is a high-level abstraction for internal service state and a RPC server. Publicly exposed to the external.
type Coordinator struct {
	state coordinatorState

	stateOpChan chan func()

	coordinatorServer *CoordinatorServer
	workerScanner     *workerScanner
}

func (c *Coordinator) Run() {
	for stateOp := range c.stateOpChan {
		stateOp()
	}
}

func (c *Coordinator) Done() bool {
	ret := false
	return ret
}

type acceptWorkerResp struct {
	profile WorkerProfile
}

func (c *Coordinator) AcceptWorker() acceptWorkerResp {
	respChan := make(chan acceptWorkerResp, 1)

	c.stateOpChan <- func() {
		acceptedWorkerID := c.state.workerFiniteSet.numWorkers

		acceptededWorker := worker{
			id:               acceptedWorkerID,
			livenessStatus:   Alive,
			activenessStatus: Inactive,
			scheduledWith:    nil,
			tasksCompleted:   make([]*task, 0),
		}

		c.state.workerFiniteSet.workers = append(c.state.workerFiniteSet.workers, &acceptededWorker)

		c.state.workerFiniteSet.numWorkers++

		acceptedProfile := WorkerProfile{
			ID: acceptedWorkerID,
		}

		respChan <- acceptWorkerResp{profile: acceptedProfile}
	}

	return <-respChan
}

// ------------------------
// RPC server definitions for a coordinator. Separation of networking concerns.
// ------------------------

// CoordinatorServer is a single listener to handle both of general RPC requests and signaling concerns.
// To separate side-concerns, signaling handlers are registered wrapped in another RPC object.
type CoordinatorServer struct {
	coord *Coordinator

	signalHandler *SignalServer

	rpcServer  *rpc.Server
	httpServer *http.Server
	listener   net.Listener
}

func (cs *CoordinatorServer) server() {

	// Register RPC methods to an instance of RPC server.
	rpcServer := rpc.NewServer()

	if err := rpcServer.RegisterName("CoordinatorService", cs); err != nil {
		log.Fatalf("<FATAL> Coordinator server thread: Failed to register coordinator server RPC methods: %v\n", err)
	}

	if err := rpcServer.RegisterName("SingalService", cs.signalHandler); err != nil {
		log.Fatalf("<FATAL> Coordinator server thread: Failed to register signal handler RPC methods: %v\n", err)
	}

	// Create a custom HTTP mux and mount the RPC server
	mux := http.NewServeMux()
	mux.Handle("/rpc", rpcServer) // all RPC requests go to /rpc

	sockname := CoordinatorSock()
	os.Remove(sockname)
	addr := sockname

	// Create the HTTP server instance
	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Register the lister on a TCP port
	l, err := net.Listen("unix", addr)
	if err != nil {
		log.Fatalf("<FATAL> Coordinator server thread: Failed to register listener: %v\n", err)
	}

	cs.rpcServer = rpcServer
	cs.httpServer = httpServer
	cs.listener = l

	cs.Start()
}

func (cs *CoordinatorServer) Start() {
	go func() {
		log.Println("<INFO> Coordinator server thread: Running on", cs.listener.Addr())
		if err := cs.httpServer.Serve(cs.listener); err != nil && err != http.ErrServerClosed {
			// http.ErrServerClosed: This is not a real failure. This error is returned after http server is closed.
			log.Fatalf("<FATAL> Coordinator server thread: Failed to run HTTP server: %v\n", err)
		}
	}()
}

func (cs *CoordinatorServer) Stop() {
	log.Println("<INFO> Coordinator server thread: Shutting down coordinator server...")
	// *http.Server.Shutdown():
	// 1. The server stops accepting new connections.
	// 2. Idle connections are closed.
	// 3. Active connections (requests in progress) are waited on until they finish.

	// Policies around context package:
	// context.Background() → wait forever, no timeout, no cancel.
	// context.WithTimeout() → wait up to X duration; if not finished, force-close.
	// context.WithCancel() → can cancel early based on some signal.

	ctx := context.Background() // Truly-gracefull termination.

	if err := cs.httpServer.Shutdown(ctx); err != nil {
		log.Println("<INFO> Coordinator server thread: Shutdown error:", err)
	} else {
		log.Println("<INFO> Coordinantor server thread: HTTP/RPC server stopped.")
	}
}

func (cs *CoordinatorServer) Connect(args ConnectArgs, reply *ConnectReply) error {

	resp := cs.coord.AcceptWorker()

	// reply
	reply.Profile = resp.profile

	log.Printf("<INFO> Coordinator RPC server thread: Worker %d is initialized\n", resp.profile.ID)

	return nil
}

type SignalServer struct {
	coord *Coordinator
}

// ------------------------
// Worker scanner type definitions
// ------------------------

type workerScanner struct {
	coord *Coordinator
}

// ------------------------
// Initialization of a coordinator process and a RPC server
// ------------------------

// MakeCoordinator initializes data structures for internal service state. This is the actual entry point of the coordinator process.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Logger initialization
	prefix := fmt.Sprintf("[ COORDINATOR | PID: %d ] ", os.Getpid())
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.SetPrefix(prefix)

	nMap := len(files)

	c := Coordinator{
		state: coordinatorState{
			taskFiniteSet: taskSet{
				mapTasksToComplete:    nMap,
				mapTasks:              make([]*task, nMap),
				reduceTasksToComplete: nReduce,
				reduceTasks:           make([]*task, nReduce),
			},
			workerFiniteSet: workerSet{
				numWorkers: 0,
				workers:    make([]*worker, 0),
			},
			idleTaskQueue: taskQueue{
				q: make([]*task, 0),
			},
		},
		stateOpChan: make(chan func(), 100),
	}

	for i := 0; i < c.state.taskFiniteSet.mapTasksToComplete; i++ {
		mt := &task{
			class:             Map,
			seq:               i,
			progressionStatus: Idle,
			inputPath:         []string{files[i]},
			requiredInputs:    1,
			scheduledWith:     make([]*worker, 0),
			completedWith:     nil,
		}

		c.state.taskFiniteSet.mapTasks[i] = mt

		c.state.idleTaskQueue.q = append(c.state.idleTaskQueue.q, mt)
	}

	for i := 0; i < c.state.taskFiniteSet.reduceTasksToComplete; i++ {
		rt := &task{
			class:             Reduce,
			seq:               i,
			progressionStatus: Idle,
			inputPath:         make([]string, c.state.taskFiniteSet.mapTasksToComplete),
			requiredInputs:    c.state.taskFiniteSet.mapTasksToComplete,
			scheduledWith:     nil,
		}

		c.state.taskFiniteSet.reduceTasks[i] = rt

		c.state.idleTaskQueue.q = append(c.state.idleTaskQueue.q, rt)
	}

	c.coordinatorServer = &CoordinatorServer{
		coord: &c,
	}
	c.workerScanner = &workerScanner{
		coord: &c,
	}

	log.Println("<INFO> Main thread: Initialized")

	go c.Run()

	return &c
}
