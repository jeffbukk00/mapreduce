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
	Class     TaskClass
	Seq       int
	InputPath []string
}

// ------------------------
// State manager type definitions
// ------------------------

// WorkerProfile represents worker metatdata assigned by the coordinator.
type WorkerProfile struct {
	IsAssigned bool
	ID         int
}

type workerState struct {
	profile    WorkerProfile
	stage      executionStage
	currentJob Job
	mapf       mapFunc
	reducef    reduceFunc
}

/*
	Worker(State Manager)

	<MUST NOT>
	1) Warn: Don't being blocked => all serialized ops on the shared state => could be stalled
	2) Fatal: Don't being blocked by sub-threads => could be deadlock

*/

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

	log.Println("<INFO> State manager: Intialized")

	w.barrierWithParent.ReadySig()

	// Lifecycle synchronization barrier:
	// Parent thread: State manager thread
	// Child thread: Execution loop thread, Signal handler thread, Shuffle server thread
	w.barrierWithChildren = NewThreadLifeCycleBarrier(3)

	w.executionLoop.barrierWithParent = w.barrierWithChildren
	w.signalHandler.barrierWithParent = w.barrierWithChildren
	w.shuffleServer.barrierWithParent = w.barrierWithChildren

	go w.executionLoop.run()
	go w.signalHandler.run()
	go w.shuffleServer.run()

	w.barrierWithChildren.Ready()

	log.Println("<INFO> Stage manager: Synchronized with children: Execution loop, Signal handler, Shuffle server")

	for stateOp := range w.stateOpChan {
		stateOp()
	}

	w.barrierWithChildren.Done()

	log.Println("<INFO> State manager: Terminated")
}

func (w *Worker) Terminate() {

	w.stateOpChan <- func() {
		go func() {
			log.Println("<INFO> State manager: Try to terminate all sub-threads")

			go w.executionLoop.quitLoop()
			go w.signalHandler.quitSignalHandler()
			go w.shuffleServer.quitShuffleServer()

			w.barrierWithChildren.Done()

			close(w.stateOpChan)
		}()
	}

	// No synchronization with the caller.
}

type getExecutionStageResp struct {
	currentStage executionStage
}

func (w *Worker) GetExecutionStage() getExecutionStageResp {
	resp := make(chan getExecutionStageResp)

	w.stateOpChan <- func() {
		resp <- getExecutionStageResp{currentStage: w.state.stage}
	}

	return <-resp
}

type updateExecutionStageReq struct {
	updatedStage executionStage
}

func (w *Worker) UpdateExecutionStage(req updateExecutionStageReq) {
	done := make(chan struct{})

	w.stateOpChan <- func() {
		w.state.stage = req.updatedStage

		done <- struct{}{}
	}

	<-done

}

type getWorkerProfileResp struct {
	profile WorkerProfile
}

func (w *Worker) GetWorkerProfile() getWorkerProfileResp {
	resp := make(chan getWorkerProfileResp)

	w.stateOpChan <- func() {

		resp <- getWorkerProfileResp{profile: w.state.profile}

	}

	return <-resp
}

type assignWorkerProfileReq struct {
	profile WorkerProfile
}

func (w *Worker) AssignWorkerProfile(req assignWorkerProfileReq) {
	done := make(chan struct{})

	w.stateOpChan <- func() {

		w.state.profile = req.profile

		close(done)
	}

	<-done
}

type getJobResp struct {
	currentJob Job
}

func (w *Worker) GetJob() getJobResp {
	resp := make(chan getJobResp)

	w.stateOpChan <- func() {
		currentJob := Job{}
		currentJob.Class = w.state.currentJob.Class
		currentJob.Seq = w.state.currentJob.Seq

		currentJob.InputPath = make([]string, len(w.state.currentJob.InputPath)) // Deep copying

		resp <- getJobResp{
			currentJob: currentJob,
		}
	}

	return <-resp
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
	stage executionStage

	stageChan chan executionStage

	w *Worker

	barrierWithParent *ThreadLifecycleBarrier
}

func (el *executionLoop) run() {

	defer el.barrierWithParent.DoneSig()

	// Buffered Channel
	// It prevents for the state manager thread from being blocked
	el.stageChan = make(chan executionStage, 10)

	log.Println("<INFO> Execution Loop: Intialized")

	el.barrierWithParent.ReadySig()

	el.startLoop()

	log.Println("<INFO> Execution loop: Terminated")
}

func (el *executionLoop) call(rpcname string, args interface{}, reply interface{}) error {

	sockname := CoordinatorSock()
	c, err := rpc.DialHTTPPath("unix", sockname, "/rpc")
	if err != nil {
		log.Fatal("<FATAL> Execution loop: Failed to connect to the coordinator: ", err)
	}
	defer c.Close() // Open and close a TCP connection per a RPC request.

	err = c.Call(rpcname, args, reply)

	return err
}

func (el *executionLoop) startLoop() {

	log.Println("<INFO> Execution loop: Start to loop")

	var e executionStage

	el.stageChan <- StageConnect

	for e = range el.stageChan {

		el.stage = e

		switch e {
		case StageConnect:
			el.w.UpdateExecutionStage(updateExecutionStageReq{updatedStage: StageConnect})
			log.Println("<INFO> Execution loop: Is at the Connect stage")
			el.connect()
		case StageSchedule:
			el.w.UpdateExecutionStage(updateExecutionStageReq{updatedStage: StageSchedule})
			log.Println("<INFO> Execution loop: Is at the Schedule stage")
			el.schedule()
		case StageFetchInput:
			el.w.UpdateExecutionStage(updateExecutionStageReq{updatedStage: StageFetchInput})
			log.Println("<INFO> Execution loop: Is at the FetchInput stage")
			el.fetchInput()
		case StageProgressTask:
			el.w.UpdateExecutionStage(updateExecutionStageReq{updatedStage: StageProgressTask})
			log.Println("<INFO> Execution loop: Is at the ProgressTask stage")
			el.processTask()
		case StageCommitOutput:
			el.w.UpdateExecutionStage(updateExecutionStageReq{updatedStage: StageSchedule})
			log.Println("<INFO> Execution loop: Is at the CommitOutput stage")
			el.commitOutput()
		case StageTerminateLoop:
			log.Println("<INFO> Execution loop: Is at the TerminateLoop stage")
			close(el.stageChan) // Propagate a termination.
		}
	}

	log.Println("<INFO> Execution loop: Stop to loop")
}

func (el *executionLoop) connect() {

	args := ConnectArgs{}
	reply := ConnectReply{}

	err := el.call(CoordinatorConnect, &args, &reply)

	if err != nil {
		// Fault tolerance: Retry policy is required.
		log.Println("<ERROR> Execution loop: Failed to connect to the coordinator")
		return
	}

	el.w.AssignWorkerProfile(assignWorkerProfileReq{profile: reply.Profile})

	// Update the global logger with assigned worker id.
	prefix := fmt.Sprintf("[ WORKER | PID: %d | ID: %d ] ", os.Getpid(), reply.Profile.ID)
	log.SetPrefix(prefix)
	log.Printf("<INFO> Execution loop: Worker %d is Connected", reply.Profile.ID)

	el.stageChan <- StageSchedule

}

func (el *executionLoop) schedule() {
	args := ScheduleArgs{}
	reply := ScheduleReply{}

	err := el.call(CoordinatorSchedule, &args, &reply)

	if err != nil {
		// Fault tolerance: Retry policy is required.
		log.Println("<ERROR> Execution loop: Failed to schedule")
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
	el.w.UpdateExecutionStage(updateExecutionStageReq{updatedStage: StageCommitOutput})
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

	pingRespChan chan PingResponse

	pingRespLoopDone   chan struct{}
	pingSenderLoopDone chan struct{}

	connectionToSignal *rpc.Client // This is bidirectional, persistent TCP(HTTP) connection.

	isNotifiedWorkerDead bool

	barrierWithParent   *ThreadLifecycleBarrier
	barrierWithChildren *ThreadLifecycleBarrier
}

func (sh *signalHandler) run() {
	defer sh.barrierWithParent.DoneSig()

	sh.pingRespChan = make(chan PingResponse)
	sh.pingRespLoopDone = make(chan struct{})
	sh.pingSenderLoopDone = make(chan struct{})

	log.Println("<INFO> Signal handler: Intialized")

	sh.initializeConnection()

	// Close the connection for signaling when signal handler itself is terminated.
	defer sh.connectionToSignal.Close()

	sh.barrierWithChildren = NewThreadLifeCycleBarrier(2)

	go sh.runPingSender()
	go sh.runPingRespHandler()

	sh.barrierWithChildren.Ready()

	sh.barrierWithParent.ReadySig()

	sh.barrierWithChildren.Done()

	log.Println("<INFO> Signal handler: Terminated")

}

func (sh *signalHandler) initializeConnection() {
	sockname := CoordinatorSock()
	c, err := rpc.DialHTTPPath("unix", sockname, "/rpc")
	if err != nil {
		log.Fatal("<FATAL> Signal handler: Failed to connect to the coordinator: ", err)
	}
	sh.connectionToSignal = c
}

func (sh *signalHandler) call(rpcname string, args interface{}, reply interface{}) error {
	c := sh.connectionToSignal

	err := c.Call(rpcname, args, reply)

	return err
}

func (sh *signalHandler) sendPing() {

	if sh.w.GetExecutionStage().currentStage == StageConnect || !sh.w.state.profile.IsAssigned {
		// Backoff: Cannot send a ping if the coordinator cannot identify this worker yet.
		log.Println("<ERROR> Signal handler: Cannot send a ping if worker profile is not assigned from the coordinator yet.")
		return
	}

	args := PingArgs{}
	reply := PingReply{}

	args.WorkerID = sh.w.GetWorkerProfile().profile.ID

	log.Println("<INFO> Signal handler: Try to send a ping to the coordinator")

	err := sh.call(SignalPing, args, &reply)

	// Unexpected errors specific to RPC call internal.
	// Not related with ping response types.
	if err != nil {
		log.Printf("<ERROR> Signal handler: Failed to send a ping to the coordinator / %v", err)
		return
	}

	if reply.Resp.RespType == WorkerDead {
		if !sh.isNotifiedWorkerDead {
			sh.isNotifiedWorkerDead = true
		} else {
			// Deduplicate WorkerDead type ping responses.
			// After the first one, do not send a message to the ping response handler
			return
		}

	}

	sh.pingRespChan <- reply.Resp

}

func (sh *signalHandler) runPingSender() {
	defer sh.barrierWithChildren.DoneSig()

	sh.barrierWithChildren.ReadySig()

	ticker := time.NewTicker(PingSendInterval * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sh.sendPing()
		case <-sh.pingSenderLoopDone:
			log.Println("<INFO> Signal handler: Quit the ping sender loop")
			return
		}
	}

}

func (sh *signalHandler) runPingRespHandler() {
	defer sh.barrierWithChildren.DoneSig()

	sh.barrierWithChildren.ReadySig()

	for {
		select {
		case resp := <-sh.pingRespChan:
			switch resp.RespType {
			case None:
				log.Println("<INFO> Signal handler: Ping response: Got a notification None from the coordinator")
			case WorkerDead:
				log.Println("<INFO> Signal handler: Ping response: Got a notification WorkerDead from the coordinator")
				sh.notifyTerminationToStateManager()
			default:
				log.Println("<Error> Signal handler: Ping response: Got a notification Unknown from the coordinator")
			}
		case <-sh.pingRespLoopDone:
			log.Println("<INFO> Signal handler: Quit the ping response loop")
			return
		}
	}

}

func (sh *signalHandler) quitSignalHandler() {
	log.Println("<INFO> Signal handler: Try to quit the signal handler")
	sh.pingRespLoopDone <- struct{}{}
	sh.pingSenderLoopDone <- struct{}{}
}

func (sh *signalHandler) notifyTerminationToStateManager() {
	sh.w.Terminate() // Notify to the state manger to start a graceful termination.
}

// ------------------------
// Type definitions for the shuffle server
// ------------------------
type shuffleServer struct {
	w *Worker

	isTerminated bool

	barrierWithParent *ThreadLifecycleBarrier
}

func (ss *shuffleServer) run() {
	defer ss.barrierWithParent.DoneSig()

	log.Println("<INFO> Shuffle server: Intialized")

	ss.barrierWithParent.ReadySig()

	for !ss.isTerminated {

		time.Sleep(time.Second * 5)
	}

	log.Println("<INFO> Shuffle server: Terminated")
}

func (ss *shuffleServer) quitShuffleServer() {
	ss.isTerminated = true
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
			profile:    WorkerProfile{},
			currentJob: Job{},
			mapf:       mapf,
			reducef:    reducef,
		},

		stateOpChan: make(chan func(), 100),
	}

	w.executionLoop = &executionLoop{
		w: &w,
	}
	w.signalHandler = &signalHandler{
		w: &w,
	}
	w.shuffleServer = &shuffleServer{
		w: &w,
	}

	log.Println("<INFO> Main: Initialized")

	// Lifecycle synchronization barrier
	// Parent thread: Main thread
	// Child thread: State manager thread
	b := NewThreadLifeCycleBarrier(1)

	w.barrierWithParent = b

	go w.Run()

	b.Ready()

	log.Println("<INFO> Main: Synchronized with children: State manager")

	b.Done()

	log.Println("<INFO> Main thread: Terminated")
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
