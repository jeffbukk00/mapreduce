// Package mr represents for the mapreduce/src/ framework implementation.
package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
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
	scheduledWith     *worker
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
	q *Queue[*task]
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
	mu         sync.Mutex
}

type workerPool struct {
	q *Queue[*worker]
}

// ------------------------
// Coordinator-related type definitions
// ------------------------

// Private state of a coordinator
type coordinatorState struct {
	taskFiniteSet      taskSet
	workerFiniteSet    workerSet
	idleTaskQueue      taskQueue
	inactiveWorkerPool workerPool
}

// Coordinator is a high-level abstraction for internal service state and a RPC server. Publicly exposed to the external.
type Coordinator struct {
	state coordinatorState
}

func (c *Coordinator) Done() bool {
	ret := false
	return ret
}

// ------------------------
// RPC server definitions for a coordinator. Separation of networking concerns.
// ------------------------

// CoordinatorRPC represents a RPC server. It is passed to the Go's built-in RPC package("net/rpc").
type CoordinatorRPC struct {
	coord *Coordinator
}

func (crpc *CoordinatorRPC) server() {
	if err := rpc.Register(crpc); err != nil {
		log.Fatalf("<FATAL> Failed to register RPC: %v\n", err)
	}

	rpc.HandleHTTP()

	sockname := CoordinatorSock()
	os.Remove(sockname)
	l, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatalf("<FATAL> Failed to listen error: %v\n", err)
	}

	log.Printf("<INFO> Listens on: %s\n", sockname)

	go func() {
		if err := http.Serve(l, nil); err != nil {
			log.Fatalf("<FATAL> HTTP server failed: %v\n", err)
		}
	}()
}

func (crpc *CoordinatorRPC) Connect(args ConnectArgs, reply *ConnectReply) error {
	// Append a newly connected worker to the worker set.
	crpc.coord.state.workerFiniteSet.mu.Lock()
	connectedWorkerID := crpc.coord.state.workerFiniteSet.numWorkers
	connectedWorker := worker{
		id:               connectedWorkerID,
		livenessStatus:   Alive,
		activenessStatus: Inactive,
		scheduledWith:    nil,
		tasksCompleted:   make([]*task, 0),
	}
	crpc.coord.state.workerFiniteSet.workers = append(crpc.coord.state.workerFiniteSet.workers, &connectedWorker)
	crpc.coord.state.workerFiniteSet.numWorkers++
	crpc.coord.state.workerFiniteSet.mu.Unlock()

	// equeue a newly connected worker to the worker pool
	crpc.coord.state.inactiveWorkerPool.q.Enqueue(&connectedWorker)

	// reply
	reply.WorkerID = connectedWorkerID

	log.Printf("<INFO> Worker %d is initialized\n", connectedWorkerID)

	return nil
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
	taskQueueSize := nMap + nReduce
	workerPoolSize := taskQueueSize

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
				q: NewQueue[*task](taskQueueSize), // (the number of map tasks + the number of reduce tasks)
			},
			inactiveWorkerPool: workerPool{
				q: NewQueue[*worker](workerPoolSize), // (the number of workers)
			},
		},
	}

	cRPC := &CoordinatorRPC{
		coord: &c,
	}

	for i := 0; i < c.state.taskFiniteSet.mapTasksToComplete; i++ {
		mt := &task{
			class:             Map,
			seq:               i,
			progressionStatus: Idle,
			inputPath:         []string{files[i]},
			requiredInputs:    1,
			scheduledWith:     nil,
		}

		c.state.taskFiniteSet.mapTasks[i] = mt
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
	}

	log.Println("<INFO> Initialized the coordinator")
	cRPC.server()
	return &c
}
