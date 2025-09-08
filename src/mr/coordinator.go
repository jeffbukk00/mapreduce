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
	"strconv"
	"time"
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

// ------------------------
// Task-related type definitions
// ------------------------
type task struct {
	class             TaskClass
	seq               int
	progressionStatus taskProgressionStatus

	// For map tasks, this is an actual input file path.
	// For reduce tasks, this is a partition id. Concatenated string(map task X's outputPath + reduce task Y's inputPath)
	// can be an actual input file path for single partition of reduce task Y.
	inputPath string

	outputPath string

	scheduledWith *worker
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
	id             int
	livenessStatus workerLivenessStatus

	scheduledWith []*task
	lastPing      time.Time
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

	barrierWithParent   *ThreadLifecycleBarrier
	barrierWithChildren *ThreadLifecycleBarrier
}

func (c *Coordinator) Run() {
	defer c.barrierWithParent.DoneSig()

	log.Println("<INFO> State manager: Intialized")

	c.barrierWithParent.ReadySig()

	c.barrierWithChildren = NewThreadLifeCycleBarrier(2)

	c.coordinatorServer.barrierWithParent = c.barrierWithChildren
	c.workerScanner.barrierWithParent = c.barrierWithChildren

	go c.coordinatorServer.run()
	go c.workerScanner.run()

	c.barrierWithChildren.Ready()

	log.Println("<INFO> Stage manager: Synchronized with children: Coordinator server, Worker scanner")

	for stateOp := range c.stateOpChan {
		stateOp()
	}

	c.barrierWithChildren.Done()

	log.Println("<INFO> State manager: Terminated")
}

func (c *Coordinator) Terminate() {
	c.stateOpChan <- func() {
		go func() {
			go c.coordinatorServer.stop()
			go c.workerScanner.quitWorkerScanner()

			c.barrierWithChildren.Done()

			close(c.stateOpChan)
		}()

	}
}

func (c *Coordinator) DetectWorkerFailure(detectionPoint time.Time) {
	done := make(chan struct{})

	lifetime := time.Duration(WorkerLifetime) * time.Second

	c.stateOpChan <- func() {

		for _, w := range c.state.workerFiniteSet.workers {
			// 1. Scan and detect failed workers
			if w.livenessStatus == Dead {
				continue
			}

			if detectionPoint.Sub(w.lastPing) <= lifetime {
				continue
			}

			// 2. Update the state of failed workers as "Dead"

			w.livenessStatus = Dead

			log.Printf("<INFO> State manager: Worker %d is dead", w.id)

			// 3. Enqueue pending or completed tasks on this failed worker to the task queue.
			// For rescheduling.
			for _, t := range w.scheduledWith {

				t.progressionStatus = Idle
				t.outputPath = ""
				t.scheduledWith = nil

				c.state.idleTaskQueue.q = append(c.state.idleTaskQueue.q, t)

				log.Printf("<INFO> State manager: Task %d will be rescheduled", t.seq)
			}

			w.scheduledWith = nil
		}

		done <- struct{}{}
	}

	<-done
}

type isWorkerDeadReq struct {
	id int
}

type isWorkerDeadResp struct {
	flag bool
}

func (c *Coordinator) isWorkerDead(req isWorkerDeadReq) isWorkerDeadResp {
	resp := make(chan isWorkerDeadResp)

	c.stateOpChan <- func() {
		w := c.state.workerFiniteSet.workers[req.id]

		resp <- isWorkerDeadResp{
			flag: w.livenessStatus == Dead,
		}
	}

	return <-resp
}

type updateLastPingReq struct {
	id int
}

func (c *Coordinator) updateLastPing(req updateLastPingReq) {
	done := make(chan struct{})

	c.stateOpChan <- func() {

		c.state.workerFiniteSet.workers[req.id].lastPing = time.Now()

		log.Printf("<INFO> State manaer: Renew the lifetime of worker %d\n", req.id)

		done <- struct{}{}
	}

	<-done
}

type scheduleTaskResp struct {
	profile WorkerProfile
	task    AssignedTask
	err     error
}

func (c *Coordinator) scheduleTask() scheduleTaskResp {
	respChan := make(chan scheduleTaskResp, 1)

	c.stateOpChan <- func() {
		// Combine 2 different ops into one transaction boundary to save a round trip.
		resp := scheduleTaskResp{}

		// Op1: Accept a worker and assign new worker id to it.
		acceptedWorkerID := c.state.workerFiniteSet.numWorkers

		acceptededWorker := worker{
			id:             acceptedWorkerID,
			livenessStatus: Alive,
		}

		c.state.workerFiniteSet.workers = append(c.state.workerFiniteSet.workers, &acceptededWorker)

		c.state.workerFiniteSet.numWorkers++

		acceptededWorker.lastPing = time.Now()

		acceptedProfile := WorkerProfile{
			IsAssigned: true,
			ID:         acceptedWorkerID,
		}

		resp.profile = acceptedProfile

		// Op2: Assign a task to this worker.

		if len(c.state.idleTaskQueue.q) == 0 {
			log.Println("<Error> State manager: Failed to assign a task because there is no queued task now")

			// Rollback the Op1.
			c.state.workerFiniteSet.workers = c.state.workerFiniteSet.workers[0 : len(c.state.workerFiniteSet.workers)-1]
			c.state.workerFiniteSet.numWorkers--
			resp.profile = WorkerProfile{}
			resp.err = fmt.Errorf("no task to assign now")

			respChan <- resp
			return
		}

		w := c.state.workerFiniteSet.workers[resp.profile.ID]

		enqueuedTask := c.state.idleTaskQueue.q[0]
		c.state.idleTaskQueue.q = c.state.idleTaskQueue.q[1:]

		enqueuedTask.progressionStatus = Pending
		enqueuedTask.scheduledWith = w

		w.scheduledWith = append(w.scheduledWith, enqueuedTask)

		assignedTask := AssignedTask{
			Class: enqueuedTask.class,
			Seq:   enqueuedTask.seq,
		}

		resp.task = assignedTask

		log.Printf("<INFO> State manager: Worker %d is initialized\n", resp.profile.ID)
		log.Printf("<INFO> State manager: %s task %d is scheduled with the worker %d\n",
			TaskClassToString(resp.task.Class), resp.task.Seq, resp.profile.ID)

		respChan <- resp
	}

	return <-respChan
}

// ------------------------
// Coordinator server definitions
// ------------------------

// CoordinatorServer is a single listener to handle both of general RPC requests and signaling concerns.
// To separate side-concerns, signaling handlers are registered wrapped in another RPC object.
type CoordinatorServer struct {
	coord *Coordinator

	signalServer *SignalServer

	rpcServer  *rpc.Server
	httpServer *http.Server
	listener   net.Listener

	barrierWithParent *ThreadLifecycleBarrier
}

func (cs *CoordinatorServer) run() {
	defer cs.barrierWithParent.DoneSig()

	cs.server()

	log.Println("<INFO> Coordinator server: Intialized")

	cs.barrierWithParent.ReadySig()

	cs.start()

	log.Println("<INFO> Coordinator server: Terminated")
}

func (cs *CoordinatorServer) server() {

	// Register RPC methods to an instance of RPC server.
	rpcServer := rpc.NewServer()

	if err := rpcServer.RegisterName(CoordinatorService, cs); err != nil {
		log.Fatalf("<FATAL> Coordinator server: Failed to register coordinator server RPC methods / %v\n", err)
	}

	if err := rpcServer.RegisterName(SignalService, cs.signalServer); err != nil {
		log.Fatalf("<FATAL> Coordinator server: Failed to register signal Server RPC methods / %v\n", err)
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
		log.Fatalf("<FATAL> Coordinator server: Failed to register listener / %v\n", err)
	}

	cs.rpcServer = rpcServer
	cs.httpServer = httpServer
	cs.listener = l
}

func (cs *CoordinatorServer) start() {

	log.Println("<INFO> Coordinator server: Running on", cs.listener.Addr())
	if err := cs.httpServer.Serve(cs.listener); err != nil && err != http.ErrServerClosed {
		// http.ErrServerClosed: This is not a real failure. This error is returned after http server is closed.
		log.Fatalf("<FATAL> Coordinator server: Failed to run HTTP server / %v\n", err)
	}

}

func (cs *CoordinatorServer) stop() {
	log.Println("<INFO> Coordinator server: Shutting down coordinator server...")
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
		log.Println("<INFO> Coordinator server: Shutdown error / ", err)
	} else {
		log.Println("<INFO> Coordinator server: Shutdown Successfully")
	}
}

func (cs *CoordinatorServer) Schedule(args ScheduleArgs, reply *ScheduleReply) error {
	resp := cs.coord.scheduleTask()

	if resp.err != nil {
		return fmt.Errorf("RPC call: CoordinatorService.Schedule: %v", resp.err)
	}

	reply.Profile = resp.profile
	reply.Task = resp.task

	return nil
}

// ------------------------
// Signal server definitions
// ------------------------

// SignalServer is a RPC object wrapping methods handling with pings and notifications.
// Single port: Served by same port with the coordinator server.
type SignalServer struct {
	coord *Coordinator
}

type PingResponseType int

const (
	None = iota
	WorkerDead
)

type PingResponse struct {
	RespType PingResponseType
}

func (ss *SignalServer) Ping(args PingArgs, reply *PingReply) error {

	reply.Resp.RespType = None

	isWorkerDead := ss.coord.isWorkerDead(isWorkerDeadReq{id: args.WorkerID}).flag

	// If worker is dead, do not update a timestamp.
	if isWorkerDead {
		reply.Resp.RespType = WorkerDead
		return nil
	}

	log.Printf("<INFO> Signal server: Got a ping from the worker %d\n", args.WorkerID)

	ss.coord.updateLastPing(updateLastPingReq{
		id: args.WorkerID,
	})

	return nil
}

// ------------------------
// Worker scanner type definitions
// ------------------------

type workerScanner struct {
	coord *Coordinator

	wScannerLoopDone chan struct{}

	barrierWithParent *ThreadLifecycleBarrier
}

func (ws *workerScanner) run() {
	defer ws.barrierWithParent.DoneSig()

	log.Println("<INFO> Worker scanner: Intialized")

	ws.barrierWithParent.ReadySig()

	ws.startWorkerScanner()

	log.Println("<INFO> Worker scanner: Terminated")
}

func (ws *workerScanner) startWorkerScanner() {
	ticker := time.NewTicker(WorkerLifetime * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			startToDetectFailure := time.Now()
			ws.coord.DetectWorkerFailure(startToDetectFailure)
			durationToDetectFailure := time.Since(startToDetectFailure)
			log.Printf("<INFO> Worker scanner: Worker scanning took %v\n", durationToDetectFailure)
		case <-ws.wScannerLoopDone:
			log.Println("<INFO> Worker scanner: Quit the worker scanner loop")
			return
		}
	}
}

func (ws *workerScanner) quitWorkerScanner() {
	ws.wScannerLoopDone <- struct{}{}
}

// ------------------------
// Initialization of a coordinator process and a RPC server
// ------------------------

// MakeCoordinator initializes data structures for internal service state. This is the actual entry point of the coordinator process.
func MakeCoordinator(files []string, nReduce int) {
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
				workers: make([]*worker, 0),
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
			inputPath:         files[i],
		}

		c.state.taskFiniteSet.mapTasks[i] = mt

		c.state.idleTaskQueue.q = append(c.state.idleTaskQueue.q, mt)
	}

	for i := 0; i < c.state.taskFiniteSet.reduceTasksToComplete; i++ {
		rt := &task{
			class:             Reduce,
			seq:               i,
			progressionStatus: Idle,
			inputPath:         strconv.Itoa(i),
		}

		c.state.taskFiniteSet.reduceTasks[i] = rt

		// c.state.idleTaskQueue.q = append(c.state.idleTaskQueue.q, rt)
	}

	c.coordinatorServer = &CoordinatorServer{
		coord: &c,

		signalServer: &SignalServer{
			coord: &c,
		},
	}
	c.workerScanner = &workerScanner{
		coord: &c,
	}

	log.Println("<INFO> Main: Initialized")
	b := NewThreadLifeCycleBarrier(1)

	c.barrierWithParent = b

	go c.Run()

	b.Ready()

	log.Println("<INFO> Main: Synchronized with children: State manager")

	b.Done()

	log.Println("<INFO> Main: Terminated")
}
