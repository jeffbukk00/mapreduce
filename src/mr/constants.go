package mr

// TaskClass is an enum for two task types: Map, Reduce.
type TaskClass int

const (
	Map TaskClass = iota
	Reduce
)

func TaskClassToString(class TaskClass) string {
	if class == Map {
		return "Map"
	}

	return "Reduce"
}

// ------------------------
// Failure Detection Policies
// ------------------------

// WorkerLifetime specifies the maximum duration a worker may run before it is considered failed.
// It also define the interval for worker scanning.
const WorkerLifetime = 10

// PingSendInterval specifies how often workers send a ping request to the coordinator.
const PingSendInterval = 5

// MaxTimerWhileShuffleFetching specifies the maximum duration of fetching input data from the other worker while shuffling.
const MaxTimerWhileShuffleFetching = 3

// MaxRetryWhileShuffleFetching specifices the maximum number of retries to fetch input data from other workers while shuffling.
const MaxRetryWhileShuffleFetching = 3

// MaxRetryWhileInputPathFetching specifies the maximum number of refries to fetch input paths from the coordinator while input path fetching.
const MaxRetryWhileInputPathFetching = 3
