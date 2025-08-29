package mr

type Queue[T any] struct {
	ch chan T
}

func NewQueue[T any](size int) *Queue[T] {
	return &Queue[T]{ch: make(chan T, size)}
}

func (q *Queue[T]) Enqueue(item T) {
	q.ch <- item // blocks if full
}

func (q *Queue[T]) TryEnqueue(item T) bool {
	select {
	case q.ch <- item:
		return true
	default:
		return false // queue full
	}
}

func (q *Queue[T]) Dequeue() T {
	return <-q.ch // blocks if empty
}

func (q *Queue[T]) TryDequeue() (T, bool) {
	select {
	case item := <-q.ch:
		return item, true
	default:
		var zero T
		return zero, false // queue empty
	}
}
