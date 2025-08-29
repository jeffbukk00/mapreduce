package mr

// ------------------------
// Public constants
// ------------------------
type TaskClass int

const (
	Map TaskClass = iota
	Reduce
)