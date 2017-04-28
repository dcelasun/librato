package librato

type BufferedChan interface {
	Push(item interface{})
	Pop() (item interface{}, ok bool)
	Close()
	Wait()
}

// bufferedChan
type bufferedChan struct {
	rx   chan interface{}
	tx   chan interface{}
	quit chan struct{}
	buf  *queue
	ms   int
}

func newBufferedChan(ms int) *bufferedChan {
	ch := &bufferedChan{
		rx:   make(chan interface{}, ms),
		tx:   make(chan interface{}, ms),
		quit: make(chan struct{}),
		buf:  newQueue(2 << 10),
		ms:   ms,
	}
	go ch.work()
	return ch
}

func (c *bufferedChan) Close() {
	close(c.rx)
}

func (c *bufferedChan) Wait() {
	<-c.quit
}

func (c *bufferedChan) Push(item interface{}) {
	c.rx <- item
}

func (c *bufferedChan) Pop() (interface{}, bool) {
	item, ok := <-c.tx
	return item, ok
}

func (c *bufferedChan) work() {
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
				c.buf.push(inItem)
			}

		case outCh <- outItem:
			// The write above will only succeed if outCh is not nil (cases with nil channels are never selected)
			outItem, ok = c.buf.pop()
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
