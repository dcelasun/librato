package librato

// Chan represents a channel.
type Chan interface {
	// Push pushes the given item to the channel
	Push(item interface{})
	// Pop gets and remove an item from the channel.
	// This method is blocking, only returns ok=false
	// if the channel is closed.
	Pop() (item interface{}, ok bool)
	// Close closes the channel. It's a prerequisite for Wait()
	Close()
	// Wait blocks until the channel is closed.
	Wait()
}

// FlexibleChan is a dynamically resizing channel.
// It has a minimum capacity of "ms".
type FlexibleChan struct {
	rx   chan interface{}
	tx   chan interface{}
	quit chan struct{}
	buf  *Queue
	ms   int
}

func NewFlexibleChan(ms int) *FlexibleChan {
	ch := &FlexibleChan{
		rx:   make(chan interface{}, ms),
		tx:   make(chan interface{}, ms),
		quit: make(chan struct{}),
		buf:  NewQueue(2 << 10),
		ms:   ms,
	}
	go ch.work()
	return ch
}

func (c *FlexibleChan) Close() {
	close(c.rx)
}

func (c *FlexibleChan) Wait() {
	<-c.quit
}

func (c *FlexibleChan) Push(item interface{}) {
	c.rx <- item
}

func (c *FlexibleChan) Pop() (interface{}, bool) {
	item, ok := <-c.tx
	return item, ok
}

func (c *FlexibleChan) work() {
	var inCh, outCh chan interface{} = c.rx, nil
	var inItem, outItem interface{}
	var ok bool

	for {
		select {
		case inItem, ok = <-inCh:
			if !ok {
				// Input channel is closed, so we need to finish up and stop the worker.
				// If outCh is nil, it means the buffer is empty and we can go ahead
				// and close the output channel (c.tx). If not, simply disable the input
				// worker (this select case) and let the output worker (the other select
				// case) continue until the buffer is cleared.
				if outCh == nil {
					close(c.tx)
					close(c.quit)
					return
				}
				inCh = nil
				break
			}

			// If output channel is disabled, re-enable it and send the input item.
			if outCh == nil {
				outItem = inItem
				outCh = c.tx
			} else {
				// If it's already enabled, it means the channel is busy and we should buffer any new items.
				c.buf.Push(inItem)
			}

		case outCh <- outItem:
			// The write above will only succeed if outCh is not nil (cases with nil channels are never selected)
			outItem, ok = c.buf.Pop()
			if !ok {
				if inCh == nil {
					// The buffer is empty *and* the input channel is closed, which means we are stopping
					// the worker. Simply close the output (c.tx) and return.
					close(c.tx)
					close(c.quit)
					return
				}
				// The buffer is empty, so disable outCh, which will be re-enabled on the input side.
				outCh = nil
			}
		}
	}
}
