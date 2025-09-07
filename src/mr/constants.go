package mr

// TaskClass is an enum for two task types: Map, Reduce.
type TaskClass int

const (
	Map TaskClass = iota
	Reduce
)

// ------------------------
// Failure Detection Policies
// ------------------------

// WorkerLifetime specifies the maximum duration a worker may run before it is considered failed.
// It also define the interval for worker scanning.
const WorkerLifetime = 15

// PingSendInterval specifies how often workers send a ping request to the coordinator.
const PingSendInterval = 5
