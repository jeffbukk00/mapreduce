package mr

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strings"
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
	StageFetchInputPath
	StageShuffle
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

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// mapFunc defines the signature for a user-defined map function.
// reduceFunc defines the signature for a user-defined reduce function.
// These are dynamically loaded through the Go plugin system.
type mapFunc func(string, string) []KeyValue
type reduceFunc func(string, []string) string

// AssignedTask represent the state of currently progressing task in the worker.
// It is scheduled by the coorinator.
type AssignedTask struct {
	Class      TaskClass
	Seq        int
	NumInputs  int
	NumOutputs int
}

type Path struct {
	IsInit   bool
	Addr     string
	Filename string
}

// Input is the fetching state for a dedicated path.
type Input struct {
	Path      Path
	IsFetched bool
}

type inputFetchTracker struct {
	isInit                   bool
	inputPathFetchRetryCount int
	inputFetchState          []Input
	whatToFetch              []int
	fetchedInputs            [][]byte
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
	profile           WorkerProfile
	shuffleServerAddr string
	task              AssignedTask
	delayedTasks      []AssignedTask
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

	mapf    mapFunc
	reducef reduceFunc

	stateOpChan chan func()

	executionLoop *executionLoop
	signalHandler *signalHandler
	shuffleServer *ShuffleServer

	barrierWithParent   *ThreadLifecycleBarrier
	barrierWithChildren *ThreadLifecycleBarrier
}

func (w *Worker) run() {
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

func (w *Worker) terminate() {

	w.stateOpChan <- func() {
		go func() {
			log.Println("<INFO> State manager: Try to terminate all sub-threads")

			go w.executionLoop.quitLoop()
			go w.signalHandler.quitSignalHandler()
			go w.shuffleServer.stop()

			w.barrierWithChildren.Done()

			close(w.stateOpChan)
		}()
	}

	// No synchronization with the caller.
}

type getWorkerProfileResp struct {
	profile WorkerProfile
}

func (w *Worker) getWorkerProfile() getWorkerProfileResp {
	resp := make(chan getWorkerProfileResp)

	w.stateOpChan <- func() {

		resp <- getWorkerProfileResp{profile: w.state.profile}

	}

	return <-resp
}

type assignWorkerProfileReq struct {
	profile WorkerProfile
}

func (w *Worker) assignWorkerProfile(req assignWorkerProfileReq) {
	done := make(chan struct{})

	w.stateOpChan <- func() {

		w.state.profile = req.profile

		close(done)
	}

	<-done
}

type getTaskResp struct {
	task AssignedTask
}

func (w *Worker) getTask() getTaskResp {
	resp := make(chan getTaskResp)

	w.stateOpChan <- func() {

		resp <- getTaskResp{
			task: w.state.task,
		}
	}

	return <-resp
}

type getDelayedTaskResp struct {
	task   AssignedTask
	isDone bool
}

func (w *Worker) getDelayedTask() getDelayedTaskResp {
	resp := make(chan getDelayedTaskResp)

	w.stateOpChan <- func() {
		if len(w.state.delayedTasks) > 0 {
			t := w.state.delayedTasks[0]
			w.state.delayedTasks = w.state.delayedTasks[1:]
			resp <- getDelayedTaskResp{
				task:   t,
				isDone: false,
			}
		} else {
			resp <- getDelayedTaskResp{
				isDone: true,
			}
		}

	}

	return <-resp
}

type pushDelayedTaskReq struct {
	task AssignedTask
}

func (w *Worker) pushDelayedTask(req pushDelayedTaskReq) {
	done := make(chan struct{})

	w.stateOpChan <- func() {
		if w.state.delayedTasks == nil {
			w.state.delayedTasks = make([]AssignedTask, 1)
			w.state.delayedTasks[0] = req.task
		} else {
			w.state.delayedTasks = append(w.state.delayedTasks, req.task)
		}

		close(done)
	}

	<-done
}

type assignTaskReq struct {
	task AssignedTask
}

func (w *Worker) assignTask(req assignTaskReq) {
	done := make(chan struct{})

	w.stateOpChan <- func() {

		w.state.task = req.task

		close(done)
	}

	<-done
}

type getMapFuncResp struct {
	mapFunc mapFunc
}

func (w *Worker) getMapFunc() getMapFuncResp {
	respChan :=
		make(chan getMapFuncResp)

	w.stateOpChan <- func() {
		respChan <- getMapFuncResp{mapFunc: w.mapf}
	}

	return <-respChan
}

type getReduceFuncResp struct {
	reduceFunc reduceFunc
}

func (w *Worker) getReduceFunc() getReduceFuncResp {
	respChan :=
		make(chan getReduceFuncResp)

	w.stateOpChan <- func() {
		respChan <- getReduceFuncResp{
			reduceFunc: w.reducef,
		}
	}

	return <-respChan
}

func (w *Worker) pushAddr(addr string) {

	w.stateOpChan <- func() {
		w.state.shuffleServerAddr = addr

	}

}

type getAddrResp struct {
	addr string
}

func (w *Worker) getAddr() getAddrResp {
	respChan := make(chan getAddrResp)

	w.stateOpChan <- func() {
		a := w.state.shuffleServerAddr

		respChan <- getAddrResp{
			addr: a,
		}
	}

	return <-respChan
}

func (w *Worker) clearTask() {
	done := make(chan struct{})

	w.stateOpChan <- func() {

		w.state.task = AssignedTask{}

		close(done)
	}

	<-done
}

// ------------------------
// Execution loop type definitions
// ------------------------

/*
	<Execution loop>

	MUST DO:
	1) Warn: Always return to the loop for retrying(Don't retry in the handler).

*/
// ExecutionLoop loops a sequence of execution stages in strict order.
type executionLoop struct {
	stage     executionStage
	stageChan chan executionStage

	inputFetcher inputFetchTracker

	outputPath []Path

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
			log.Println("<INFO> Execution loop: Is at the Connect stage")
			el.connect()
			time.Sleep(2 * time.Second)
		case StageSchedule:
			log.Println("<INFO> Execution loop: Is at the Schedule stage")
			el.schedule()
			time.Sleep(2 * time.Second)
		case StageFetchInputPath:
			log.Println("<INFO> Execution loop: Is at the FetchInputPath stage")
			el.fetchInputPath()
			time.Sleep(2 * time.Second)
		case StageShuffle:
			log.Println("<INFO> Execution loop: Is at the Shuffle stage")
			el.shuffle()
			time.Sleep(2 * time.Second)
		case StageProgressTask:
			log.Println("<INFO> Execution loop: Is at the ProgressTask stage")
			el.processTask()
			time.Sleep(2 * time.Second)
		case StageCommitOutput:
			log.Println("<INFO> Execution loop: Is at the CommitOutput stage")
			el.commitOutput()
			time.Sleep(2 * time.Second)
		case StageTerminateLoop:
			log.Println("<INFO> Execution loop: Is at the TerminateLoop stage")

		Drain:
			for {
				select {
				case <-el.stageChan:
				default:
					break Drain
				}
			}

			close(el.stageChan) // Propagate a termination.
		}
	}

	log.Println("<INFO> Execution loop: Stop to loop")
}

func (el *executionLoop) clearInputState() {
	el.inputFetcher = inputFetchTracker{} // Clear the input state.
	el.outputPath = nil
}

func (el *executionLoop) connect() {
	args := ConnectArgs{}
	reply := ConnectReply{}

	err := el.call(CoordinatorConnect, &args, &reply)

	if err != nil {
		log.Printf("<ERROR> Execution loop: Failed to connect: %v\n", err)
		// Retry
		el.stageChan <- StageConnect
		return
	}

	el.w.assignWorkerProfile(assignWorkerProfileReq{profile: reply.Profile})

	// Update the global logger with assigned worker id.
	prefix := fmt.Sprintf("[ WORKER | PID: %d | ID: %d ] ", os.Getpid(), reply.Profile.ID)
	log.SetPrefix(prefix)

	log.Printf("<INFO> Execution loop: Worker %d is Connected", reply.Profile.ID)

	el.stageChan <- StageSchedule
}

func (el *executionLoop) schedule() {
	var assignedTask AssignedTask

	args := ScheduleArgs{}
	reply := ScheduleReply{}

	args.Profile = el.w.getWorkerProfile().profile

	err := el.call(CoordinatorSchedule, &args, &reply)

	if err != nil {
		delayedTask := el.w.getDelayedTask()

		if !delayedTask.isDone {
			assignedTask = delayedTask.task
		} else {
			log.Printf("<ERROR> Execution loop: Failed to schedule: %v\n", err)
			// Backoff
			time.Sleep(time.Second * 5)
			// Retry
			el.stageChan <- StageSchedule
			return
		}
	} else {
		assignedTask = reply.Task
	}

	el.w.assignTask(assignTaskReq{task: assignedTask})

	log.Printf("<INFO> Execution loop: %s task %d is scheduled\n", TaskClassToString(assignedTask.Class), assignedTask.Seq)

	el.stageChan <- StageFetchInputPath
}

func (el *executionLoop) fetchInputPath() {
	p := el.w.getWorkerProfile().profile
	t := el.w.getTask().task

	if !el.inputFetcher.isInit {
		// Initialize once per a task
		el.inputFetcher.inputFetchState = make([]Input, t.NumInputs)
		el.inputFetcher.fetchedInputs = make([][]byte, t.NumInputs)
		el.inputFetcher.whatToFetch = make([]int, 0)

		for i := range el.inputFetcher.inputFetchState {
			el.inputFetcher.whatToFetch = append(el.inputFetcher.whatToFetch, i)
		}

		el.inputFetcher.isInit = true
	} else {
		if el.inputFetcher.inputPathFetchRetryCount == MaxRetryWhileInputPathFetching {
			el.w.pushDelayedTask(pushDelayedTaskReq{task: t})
			log.Printf("<INFO> Execution loop: %v task %v is delayed", TaskClassToString(t.Class), t.Seq)
			el.w.clearTask()
			el.clearInputState()
			log.Printf("<ERROR> Execution loop: Failed to fetch inputs; Return to Schedule stage")
			el.stageChan <- StageSchedule
			return
		}
		el.inputFetcher.inputPathFetchRetryCount++
		log.Printf("<ERROR> Execution loop: Retried to fetch input paths %v times for %v task %v\n",
			el.inputFetcher.inputPathFetchRetryCount, TaskClassToString(t.Class), t.Seq)
	}

	args := FetchInputPathArgs{
		Profile:     p,
		Task:        t,
		WhatToFetch: el.inputFetcher.whatToFetch,
	}

	reply := FetchInputPathReply{}

	err := el.call(CoordinatorFetchInputPath, &args, &reply)

	if err != nil {
		log.Printf("<ERROR> Execution loop: Failed to : %v\n", err)
		// Retry
		el.stageChan <- StageFetchInputPath
		return
	}

	for i, v := range el.inputFetcher.whatToFetch {
		p := reply.InputPaths[i]
		el.inputFetcher.inputFetchState[v].Path = p

	}

	log.Println("<INFO> Execution loop: Input paths are fetched")

	el.stageChan <- StageShuffle
}

func (el *executionLoop) shuffle() {
	t := el.w.getTask().task
	addr := el.w.getAddr().addr

	// Local file reader for an input of a map task.
	read := func(filename string, inputIdx int) {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("<FATAL> Non-recoverable error from local file open: %v", err)
		}

		defer file.Close()

		content, err := io.ReadAll(file)

		if err != nil {
			log.Fatalf("<FATAL> Non-recoverable error from local file read: %v", err)
		}

		log.Printf("<INFO> Execution loop: Fetched %v bytes for a %v task %v from %v\n",
			len(content), TaskClassToString(t.Class), t.Seq, filename)
		// Loads a fetched input to the memory.
		el.inputFetcher.fetchedInputs[inputIdx] = content
	}

	// // Remote fetcher for inputs of a reduce task from other.
	fetch := func(path Path, inputIdx int, offset int64, size int) (int64, bool, error) {

		c, err := rpc.DialHTTPPath("unix", path.Addr, "/rpc")

		if err != nil {
			return offset, false, fmt.Errorf("failed to connect to %v: %v", path.Addr, err)
		}
		defer c.Close()

		args := FetchForShuffleArgs{
			Filename: path.Filename,
			Offset:   offset,
			Size:     size,
		}

		reply := &FetchForShuffleReply{}

		done := make(chan error, 1)

		go func() {
			done <- c.Call(ShuffleFetch, args, reply)
		}()

		select {
		case err := <-done:
			if err != nil {
				return offset, false, fmt.Errorf("failed to fetch from  %v, %v: %v", path.Addr, path.Filename, err)
			}
		case <-time.After(MaxTimerWhileShuffleFetching * time.Second):
			return offset, false, fmt.Errorf("failed to fetch from  %v, %v: timeout", path.Addr, path.Filename)
		}

		el.inputFetcher.fetchedInputs[inputIdx] = append(el.inputFetcher.fetchedInputs[inputIdx], reply.Data...)

		if reply.EOF {
			return reply.Offset, true, nil
		}

		return reply.Offset, false, nil
	}

	wg := sync.WaitGroup{}

	whatToFetch := make([]int, 0)

	for _, v := range el.inputFetcher.whatToFetch {

		path := el.inputFetcher.inputFetchState[v].Path
		if !path.IsInit {
			log.Printf("<ERROR> Execution loop: Failed to fetch input %v; Path is not initialized", v)
			whatToFetch = append(whatToFetch, v)
			continue
		}

		wg.Add(1)

		if path.Addr == "" || path.Addr == addr {
			go func() {
				defer wg.Done()

				read(path.Filename, v)
			}()
		} else {
			go func() {
				defer wg.Done()

				var offset int64 = 0
				size := 16 * 1024
				isDone := false
				retry := 0

				for !isDone && retry < MaxRetryWhileShuffleFetching {
					o, flag, err := fetch(path, v, offset, size)

					offset = o
					isDone = flag

					if err != nil {
						retry++
						log.Printf("<ERROR> Execution loop: Retried to fetch %v times for input %v: %v\n", retry, v, err)
					}

				}

				if retry == MaxRetryWhileShuffleFetching {
					log.Printf("<ERROR> Execution loop: Failed to fetch input %v\n", v)
					whatToFetch = append(whatToFetch, v)
				} else {
					log.Printf("<INFO> Execution loop: Fetched %v bytes for a %v task %v from %v, %v\n",
						offset, TaskClassToString(t.Class), t.Seq, path.Addr, path.Filename)
				}

			}()
		}

	}

	wg.Wait()

	// If some fetches failed, return to StateFetchInputPath for retrying on only failed inputs/paths.
	if len(whatToFetch) > 0 {
		log.Printf("<ERROR> Execution loop: Failed to fetch %v inputs; Return to FetchInputPath stage\n", len(whatToFetch))
		el.inputFetcher.whatToFetch = whatToFetch
		el.stageChan <- StageFetchInputPath
		return
	}

	log.Println("<INFO> Execution loop: Shuffle is succeeded; All inputs are fetched")

	el.stageChan <- StageProgressTask

}

func (el *executionLoop) processTask() {
	writeLocal := func(filename string, listKV []KeyValue) {
		// Writes to local FS.
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("<FATAL> Non-recoverable error from local file create: %v: %v", filename, err)
		}
		defer file.Close()

		for _, v := range listKV {

			_, err = fmt.Fprintf(file, "%v %v\n", v.Key, v.Value)

			if err != nil {
				log.Fatalf("<FATAL> Non-recoverable error from local file write: %v: %v", filename, err)
			}

		}
	}

	writeAtomic := func(oldFilename string, newFilename string, listKV []KeyValue) {
		// Writes to local FS.
		file, err := os.Create(oldFilename)
		if err != nil {
			log.Fatalf("<FATAL> Non-recoverable error from local file create: %v: %v", oldFilename, err)
		}

		for _, v := range listKV {

			_, err = fmt.Fprintf(file, "%v %v\n", v.Key, v.Value)

			if err != nil {
				file.Close()
				os.Remove(oldFilename)
				log.Fatalf("<FATAL> Non-recoverable error from local file write: %v: %v", oldFilename, err)
			}

		}

		if err = file.Sync(); err != nil {
			file.Close()
			os.Remove(oldFilename)
			log.Fatalf("<FATAL> Non-recoverable error from local file sync: %v: %v", oldFilename, err)
		}

		if err = file.Close(); err != nil {
			os.Remove(oldFilename)
			log.Fatalf("<FATAL> Non-recoverable error from local file close: %v: %v", oldFilename, err)
		}

		err = os.Rename(oldFilename, newFilename)
		if err != nil {
			// Recoverable error like EEXIST on window.
			// Retry after remove the final path.
			err = os.Remove(newFilename)
			if err != nil {
				log.Fatalf("<FATAL> Non-recoverable error from local file remove: %v: %v", newFilename, err)
			} else {
				err = os.Rename(oldFilename, newFilename)

				if err != nil {
					log.Fatalf("<FATAL> Non-recoverable error from local file rename from %v to %v: %v",
						oldFilename, newFilename, err)
				}

			}
		}

	}

	w := el.w.getWorkerProfile().profile
	t := el.w.getTask().task
	addr := el.w.getAddr().addr

	setContextForMapTaskExec := func(mapFunc mapFunc) {

		// Single input for a map task
		inputFilename := el.inputFetcher.inputFetchState[0].Path.Filename
		content := string(el.inputFetcher.fetchedInputs[0])

		output := mapFunc(inputFilename, content)

		getOutputFilename := func(partition int) string {
			return fmt.Sprintf("mr-out-worker-%v-map-%v-%v.txt", w.ID, t.Seq, partition)
		}

		numPartitions := t.NumOutputs
		partitions := make([][]KeyValue, t.NumOutputs)
		el.outputPath = make([]Path, numPartitions)

		for _, v := range output {
			hash := ihash(v.Key)
			p := hash % numPartitions

			if partitions[p] == nil {
				partitions[p] = make([]KeyValue, 0)
			}

			partitions[p] = append(partitions[p], v)
		}

		for i, v := range partitions {
			sort.Sort(ByKey(v)) // Ordering is guaranteed per a partition.

			filename := getOutputFilename(i)
			el.outputPath[i] = Path{
				IsInit:   true,
				Addr:     addr,
				Filename: filename,
			}

			writeLocal(filename, v)
		}
	}

	setContextForReduceTaskExec := func(reduceFunc reduceFunc) {
		// Inputs(intermidiate KVs) for a reduce task must be ordered and grouped with the same key.
		listKV := make([]KeyValue, 0)

		for _, bytes := range el.inputFetcher.fetchedInputs {
			s := string(bytes)
			parts := strings.Split(s, "\n")
			parts = parts[:len(parts)-1]
			listKVPerInput := make([]KeyValue, len(parts))
			for i, kvStr := range parts {

				kvParts := strings.Split(kvStr, " ")

				listKVPerInput[i] = KeyValue{
					Key:   kvParts[0],
					Value: kvParts[1],
				}
			}
			listKV = append(listKV, listKVPerInput...)
		}

		sort.Sort(ByKey(listKV))

		output := make([]KeyValue, 0)

		for i := 0; i < len(listKV); {
			key := listKV[i].Key
			values := make([]string, 1)
			values[0] = listKV[i].Value

			j := i + 1
			for ; j < len(listKV) && listKV[j].Key == key; j++ {
				values = append(values, listKV[j].Value)
			}

			reducedValue := reduceFunc(key, values)

			output = append(output, KeyValue{
				Key:   key,
				Value: reducedValue,
			})

			i = j
		}

		// Atomic rename
		tempOutputFilename := fmt.Sprintf("mr-out-worker-%v-reduce-%v-tmp.txt", w.ID, t.Seq)
		atomicFilename := fmt.Sprintf("mr-out-reduce-%v.txt", t.Seq)

		writeAtomic(tempOutputFilename, atomicFilename, output)
	}

	if t.Class == Map {
		setContextForMapTaskExec(el.w.getMapFunc().mapFunc)
	} else {
		setContextForReduceTaskExec(el.w.getReduceFunc().reduceFunc)
	}

	log.Printf("<INFO> Execution loop: %v Task %v is fully-processed; All outputs are written\n", TaskClassToString(t.Class), t.Seq)

	el.stageChan <- StageCommitOutput
}

func (el *executionLoop) commitOutput() {
	w := el.w.getWorkerProfile().profile
	t := el.w.getTask().task

	args := CommitOutputArgs{
		Profile:    w,
		Task:       t,
		OutputPath: el.outputPath,
	}

	reply := CommitOutputReply{}

	err := el.call(coordinatorCommitoutput, &args, &reply)

	if err != nil {
		log.Printf("<ERROR> Execution loop: Failed to : %v\n", err)
		// Retry
		el.stageChan <- StageCommitOutput
		return
	}

	// Required: Clear up prior task's context.
	el.w.clearTask() // Clear the current task.
	el.clearInputState()

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
	w := sh.w.getWorkerProfile().profile

	if !w.IsAssigned {
		// Backoff: Cannot send a ping if the coordinator cannot identify this worker yet.
		log.Println("<ERROR> Signal handler: Cannot send a ping if worker profile is not assigned from the coordinator yet.")
		return
	}

	args := PingArgs{}
	reply := PingReply{}

	args.WorkerID = w.ID

	log.Println("<INFO> Signal handler: Try to send a ping to the coordinator")

	err := sh.call(SignalPing, args, &reply)

	// Unexpected errors specific to RPC call internal.
	// Not related with ping response types.
	if err != nil {
		log.Fatalf("<FATAL> Signal handler: Failed to send a ping to the coordinator / %v", err)
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
	sh.w.terminate() // Notify to the state manger to start a graceful termination.
}

// ------------------------
// Type definitions for the shuffle server
// ------------------------

// ShuffleServer is an independent sender for fetching requests from other workers.
// It serves intermidiate KV outputs from map tasks which are stored localy.
type ShuffleServer struct {
	w *Worker

	rpcServer  *rpc.Server
	httpServer *http.Server
	listener   net.Listener

	barrierWithParent *ThreadLifecycleBarrier
}

func (ss *ShuffleServer) run() {
	defer ss.barrierWithParent.DoneSig()

	ss.server()
	ss.w.pushAddr(ss.listener.Addr().String())

	log.Println("<INFO> Shuffle server: Intialized")

	ss.barrierWithParent.ReadySig()

	ss.start()

	log.Println("<INFO> Shuffle server: Terminated")
}

func (ss *ShuffleServer) server() {
	// Register RPC methods to an instance of RPC server.
	rpcServer := rpc.NewServer()

	if err := rpcServer.RegisterName(ShuffleService, ss); err != nil {
		log.Fatalf("<FATAL> Shuffle server: Failed to register coordinator server RPC methods / %v\n", err)
	}

	// Create a custom HTTP mux and mount the RPC server
	mux := http.NewServeMux()
	mux.Handle("/rpc", rpcServer) // all RPC requests go to /rpc

	sockname := ShuffleServerSock()
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
		log.Fatalf("<FATAL> Shuffle server: Failed to register listener / %v\n", err)
	}

	ss.rpcServer = rpcServer
	ss.httpServer = httpServer
	ss.listener = l
}

func (ss *ShuffleServer) start() {

	log.Println("<INFO> Shuffle server: Running on", ss.listener.Addr())
	if err := ss.httpServer.Serve(ss.listener); err != nil && err != http.ErrServerClosed {
		log.Fatalf("<FATAL> Shuffle server: Failed to run HTTP server / %v\n", err)
	}

}

func (ss *ShuffleServer) stop() {
	log.Println("<INFO> Shuffle server: Shutting down shuffle server...")

	ctx := context.Background()

	if err := ss.httpServer.Shutdown(ctx); err != nil {
		log.Println("<INFO> Shuffle server: Shutdown error / ", err)
	} else {
		log.Println("<INFO> Shuffle server: Shutdown Successfully")
	}
}

func (ss *ShuffleServer) FetchForShuffle(args FetchForShuffleArgs, reply *FetchForShuffleReply) error {

	data, updatedOffset, isDone, err := ss.readBytesByOffset(args.Filename, args.Offset, args.Size)

	if err != nil {
		return fmt.Errorf("RPC call: ShuffleService.FetchForShuffle: %v", err)
	}

	reply.Data = data
	reply.EOF = isDone
	reply.Offset = updatedOffset

	return nil
}

func (ss *ShuffleServer) readBytesByOffset(filename string, offset int64, size int) ([]byte, int64, bool, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, offset, false, fmt.Errorf("failed open %v", filename)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, offset, false, fmt.Errorf("failed get stat from %v", filename)
	}

	isDone := false

	chunksize := int64(size)

	remaining := fileInfo.Size() - offset

	if remaining < int64(size) {
		chunksize = remaining
		isDone = true
	}

	buf := make([]byte, int(chunksize))

	_, err = file.Seek(offset, io.SeekStart)

	if err != nil {
		return nil, offset, false, fmt.Errorf("failed seek offset %v on %v", offset, filename)
	}

	n, err := io.ReadFull(file, buf)
	if err != nil {
		if err == io.EOF {
			println("<CHECKPOINT #1>")
			isDone = true
		} else {
			return nil, offset, false, fmt.Errorf("failed to read from offset %v on %v", offset, filename)
		}
	}

	updatedOffset := offset + int64(n)

	return buf, updatedOffset, isDone, nil
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

		mapf:    mapf,
		reducef: reducef,

		stateOpChan: make(chan func(), 100),
	}

	w.executionLoop = &executionLoop{
		w: &w,
	}
	w.signalHandler = &signalHandler{
		w: &w,
	}
	w.shuffleServer = &ShuffleServer{
		w: &w,
	}

	log.Println("<INFO> Main: Initialized")

	// Lifecycle synchronization barrier
	// Parent thread: Main thread
	// Child thread: State manager thread
	b := NewThreadLifeCycleBarrier(1)

	w.barrierWithParent = b

	go w.run()

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
