package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// ------------------------
// Private constants
// ------------------------
type executionStage int

const (
	StageConnect executionStage = iota
	StageSchedule
	StageFetchInput
	StageProgressTask
	StageCommitOutput
	StageTerminateLoop
)

// ------------------------
// Type definitions for synchronization primitive
// ------------------------

type ThreadLifecycleBarrier struct {
	ready                  sync.WaitGroup
	done                   sync.WaitGroup
	start                  chan struct{}
	numThreadsToBeSyncWith int
}

func NewThreadLifeCycleBarrier(numThreads int) *ThreadLifecycleBarrier {
	newBarrier := ThreadLifecycleBarrier{
		ready:                  sync.WaitGroup{},
		done:                   sync.WaitGroup{},
		start:                  make(chan struct{}),
		numThreadsToBeSyncWith: numThreads,
	}

	newBarrier.ready.Add(newBarrier.numThreadsToBeSyncWith)
	newBarrier.done.Add(newBarrier.numThreadsToBeSyncWith)

	return &newBarrier
}

func (b *ThreadLifecycleBarrier) ReadySig() {
	b.ready.Done()
	<-b.start
}

func (b *ThreadLifecycleBarrier) DoneSig() {
	b.done.Done()
}

func (b *ThreadLifecycleBarrier) Ready() {
	b.ready.Wait()
	close(b.start)
}

func (b *ThreadLifecycleBarrier) Done() {
	b.done.Wait()
}

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
// Job type definitions
// ------------------------

// Job represent the state of currently progressing task in the worker.
// It is scheduled by the coorinator.
type Job struct {
	Class          TaskClass
	Seq            int
	InputPath      []string
	RequiredInputs int
}

// ------------------------
// State manager type definitions
// ------------------------

// WorkerProfile represents worker metatdata assigned by the coordinator.
type WorkerProfile struct {
	ID int
}

type workerState struct {
	profile    WorkerProfile
	currentJob Job
	mapf       mapFunc
	reducef    reduceFunc
}

// Worker owns the shared state.
// No other threads can access or modify the shared state directly.
// It coordinates cross-cutting concerns across all other threads.
// It holds references to these threads and broadcasts updates to them as needed.
type Worker struct {
	state workerState

	stateOpChan chan func()

	executionLoop *executionLoop
	signalHandler *signalHandler
	shuffleServer *shuffleServer

	barrierWithParent   *ThreadLifecycleBarrier
	barrierWithChildren *ThreadLifecycleBarrier
}

func (w *Worker) Run() {
	defer w.barrierWithParent.DoneSig()

	log.Println("<INFO> State manager thread: Intialized")

	w.barrierWithParent.ReadySig()

	// Lifecycle synchronization barrier:
	// Parent thread: State manager thread
	// Child thread: Execution loop thread, Signal handler thread, Shuffle server thread
	b := NewThreadLifeCycleBarrier(3)
	w.barrierWithChildren = b

	w.executionLoop.barrierWithParent = b
	w.signalHandler.barrierWithParent = b
	w.shuffleServer.barrierWithParent = b

	go w.executionLoop.run()
	go w.signalHandler.run()
	go w.shuffleServer.run()

	b.Ready()

	log.Println("<INFO> Stage manager thread: Synchronized with child threads: Execution loop thread, Signal handler thread, Shuffle server thread")

	for stateOp := range w.stateOpChan {
		stateOp()
	}

	b.Done()

	log.Println("<INFO> State manager thread: Terminated")
}

func (w *Worker) Terminate() {

	w.stateOpChan <- func() {
		w.executionLoop.quitLoop()
		w.signalHandler.quitSignalHandler()
		w.shuffleServer.quitShuffleServer()

		w.barrierWithChildren.Done()

		close(w.stateOpChan)
	}

	// No synchronization with the caller.
}

type assignWorkerProfileReq struct {
	profile WorkerProfile
}

func (w *Worker) AssignWorkerProfile(req assignWorkerProfileReq) {
	done := make(chan struct{})

	w.stateOpChan <- func() {

		// Warning: This is a shallow copy.
		// But, there is a guarantee of no shared references.
		w.state.profile = req.profile

		close(done)
	}

	<-done
}

type assignJobReq struct {
	job Job
}

func (w *Worker) AssignJob(req assignJobReq) {
	done := make(chan struct{})

	w.stateOpChan <- func() {

		// Warning: This is a shallow copy.
		// But, there is a guarantee of no shared references.
		w.state.currentJob = req.job

		close(done)
	}

	<-done
}

// ------------------------
// Execution loop type definitions
// ------------------------

// ExecutionLoop loops a sequence of execution stages in strict order.
type executionLoop struct {
	stage        executionStage
	isTerminated bool

	stageChan chan executionStage

	w *Worker

	barrierWithParent *ThreadLifecycleBarrier
}

func (el *executionLoop) run() {

	defer el.barrierWithParent.DoneSig()

	// Buffered Channel
	// It prevents for the state manager thread from being blocked
	el.stageChan = make(chan executionStage, 10)

	log.Println("<INFO> Execution Loop thread: Intialized")

	el.barrierWithParent.ReadySig()

	el.startLoop()

	log.Println("<INFO> Execution loop thread: Terminated")
}

func (el *executionLoop) call(rpcname string, args interface{}, reply interface{}) error {

	sockname := CoordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("<FATAL> Execution loop thread: Connect to the coordinator: ", err)
	}
	defer c.Close() // Open and close a TCP connection per a RPC request.

	err = c.Call(rpcname, args, reply)

	return err
}

func (el *executionLoop) startLoop() {

	log.Println("<INFO> Execution loop thread: Start to loop")

	var e executionStage

	el.stageChan <- StageConnect

	for !el.isTerminated {
		e = <-el.stageChan

		el.stage = e

		switch e {
		case StageConnect:
			log.Println("<INFO> Execution loop thread: Is at the Connect stage")
			el.connect()
		case StageSchedule:
			log.Println("<INFO> Execution loop thread: Is at the Schedule stage")
			el.schedule()
		case StageFetchInput:
			log.Println("<INFO> Execution loop thread: Is at the FetchInput stage")
			el.fetchInput()
		case StageProgressTask:
			log.Println("<INFO> Execution loop thread: Is at the ProgressTask stage")
			el.processTask()
		case StageCommitOutput:
			log.Println("<INFO> Execution loop thread: Is at the CommitOutput stage")
			el.commitOutput()
		case StageTerminateLoop:
			log.Println("<INFO> Execution loop thread: Is at the TerminateLoop stage")
			el.isTerminated = true // Propagate a termination.
		}
	}

	log.Println("<INFO> Execution loop thread: Stop to loop")
}

func (el *executionLoop) connect() {

	args := ConnectArgs{}
	reply := ConnectReply{}

	err := el.call(RPCConnect, &args, &reply)

	if err != nil {
		// Fault tolerance: Retry policy is required.
		log.Println("<ERROR> Execution loop thread: Failed to connect to the coordinator")
		return
	}

	el.w.AssignWorkerProfile(assignWorkerProfileReq{profile: reply.Profile})

	// Update the global logger with assigned worker id.
	prefix := fmt.Sprintf("[ WORKER | PID: %d | ID: %d ] ", os.Getpid(), reply.Profile.ID)
	log.SetPrefix(prefix)
	log.Printf("<INFO> Execution loop thread: Worker %d is Connected", reply.Profile.ID)

	el.stageChan <- StageSchedule
}

func (el *executionLoop) schedule() {
	args := ScheduleArgs{}
	reply := ScheduleReply{}

	err := el.call(RPCSchedule, &args, &reply)

	if err != nil {
		// Fault tolerance: Retry policy is required.
		log.Println("<ERROR> Execution loop thread: Failed to schedule")
		return
	}

	el.w.AssignJob(assignJobReq{job: reply.Task})

	el.stageChan <- StageFetchInput
}

func (el *executionLoop) fetchInput() {
	el.stageChan <- StageProgressTask
}

func (el *executionLoop) processTask() {
	el.stageChan <- StageCommitOutput
}

func (el *executionLoop) commitOutput() {

	el.stageChan <- StageSchedule // Loop. Return to StageSchedule.
}

func (el *executionLoop) quitLoop() {
	// Notified by the signal handler.
	el.stageChan <- StageTerminateLoop
}

// ------------------------
// Signal Handler type definitions
// ------------------------
type signalHandler struct {
	w *Worker

	barrierWithParent *ThreadLifecycleBarrier
}

func (sh *signalHandler) run() {
	defer sh.barrierWithParent.DoneSig()

	log.Println("<INFO> Signal handler thread: Intialized")

	sh.barrierWithParent.ReadySig()

	sh.notifyTerminationToStateManager()

	log.Println("<INFO> Signal handler thread: Terminated")

}

func (sh *signalHandler) quitSignalHandler() {}

func (sh *signalHandler) notifyTerminationToStateManager() {
	time.Sleep(time.Second * 20)
	sh.w.Terminate()
}

// ------------------------
// Type definitions for the shuffle server
// ------------------------
type shuffleServer struct {
	w *Worker

	barrierWithParent *ThreadLifecycleBarrier
}

func (ss *shuffleServer) run() {
	defer ss.barrierWithParent.DoneSig()

	log.Println("<INFO> Shuffle server thread: Intialized")

	ss.barrierWithParent.ReadySig()

	time.Sleep(time.Second * 5)

	log.Println("<INFO> Shuffle server thread: Terminated")
}

func (ss *shuffleServer) quitShuffleServer() {}

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
			profile:    WorkerProfile{},
			currentJob: Job{},
			mapf:       mapf,
			reducef:    reducef,
		},

		stateOpChan: make(chan func(), 100),
	}

	wExecutionLoop := executionLoop{
		w: &w,
	}

	wSignalHandler := signalHandler{
		w: &w,
	}

	wShuffleServer := shuffleServer{
		w: &w,
	}

	w.executionLoop = &wExecutionLoop
	w.signalHandler = &wSignalHandler
	w.shuffleServer = &wShuffleServer

	log.Println("<INFO> Main thread: Initialized")

	// Lifecycle synchronization barrier
	// Parent thread: Main thread
	// Child thread: State manager thread
	b := NewThreadLifeCycleBarrier(1)

	w.barrierWithParent = b

	go w.Run()

	b.Ready()

	log.Println("<INFO> Main thread: Synchronized with child threads: State manager thread")

	b.Done()

	log.Println("<INFO> Main thread: Main thread: Terminated")
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
