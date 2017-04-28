package librato

// Queue implements a simple FIFO queue using a ring buffer.
// It has a minimum buffer size of "ms", which must be a power of two.
type Queue struct {
	// Buffer to store queued items in.
	items []interface{}
	// Positions of the start and end items,
	// number of items in the Queue,
	// minimum size of the Queue.
	//
	// Both start and end will wrap around to the
	// beginning of the buffer as needed.
	start, end, count, ms int
}

func NewQueue(minBufferSize int) *Queue {
	if minBufferSize == 0 || minBufferSize&-minBufferSize != minBufferSize {
		panic("Queue size must be a power of two.")
	}

	return &Queue{
		items: make([]interface{}, minBufferSize),
		ms:    minBufferSize,
	}
}

func (q *Queue) Push(item interface{}) {
	if q.count == len(q.items) {
		// Queue is full, grow it.
		q.resize()
	}

	q.items[q.end] = item
	// Move the end position by 1. If we are already
	// at the end of the slice, this will move "end"
	// back to 0 since:
	// (x+1) & (y-1) == 0 for x=y+1
	q.end = (q.end + 1) & (len(q.items) - 1)
	q.count++
}

func (q *Queue) Pop() (interface{}, bool) {
	if q.count == 0 {
		return nil, false
	}

	item := q.items[q.start]
	q.items[q.start] = nil
	// Move start forward by 1.
	q.start = (q.start + 1) & (len(q.items) - 1)
	q.count--
	// Shrink the Queue if it's at 25% capacity,
	// but only if we are not already at minimum size.
	if q.count<<2 == len(q.items) && len(q.items) > q.ms {
		q.resize()
	}
	return item, true
}

func (q *Queue) Length() int {
	return q.count
}

func (q *Queue) resize() {
	// Create a new buffer with double the current item count.
	// This can shrink or grow the Queue, depending on the count.
	items := make([]interface{}, q.count<<1)

	if q.start < q.end {
		// If "end" position is ahead of "start",
		// we can simply copy from "start" to "end".
		copy(items, q.items[q.start:q.end])
	} else {
		// If not, we need to make two copies:
		// One from "start" to the end of the buffer
		// and one from the start of the buffer to "end".
		n := copy(items, q.items[q.start:])
		copy(items[n:], q.items[:q.end])
	}

	q.start = 0
	q.end = q.count
	q.items = items
}
