package librato

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type Client interface {
	GetGauge(name string) BufferedChan
	GetCounter(name string) BufferedChan
	Close()
	Wait()
}

type TimeCollatedClient struct {
	user, token, source string
	duration            time.Duration
	counters            map[string]BufferedChan
	gauges              map[string]BufferedChan
	collateCounters     BufferedChan
	collateGauges       BufferedChan
	stop                chan struct{}
	client              *http.Client
	wg                  *sync.WaitGroup
}

func NewTimeCollatedClient(user, token, source string, duration time.Duration) *TimeCollatedClient {
	c := &TimeCollatedClient{
		user:            user,
		token:           token,
		source:          source,
		duration:        duration,
		counters:        make(map[string]BufferedChan),
		gauges:          make(map[string]BufferedChan),
		collateCounters: newBufferedChan(2 << 10),
		collateGauges:   newBufferedChan(2 << 10),
		stop:            make(chan struct{}),
		client:          &http.Client{},
		wg:              &sync.WaitGroup{},
	}
	go c.work()
	return c
}

func (c *TimeCollatedClient) work() {
	t := time.NewTicker(c.duration)
	gauges := []interface{}{}
	counters := []interface{}{}
	for {
		select {
		case <-t.C:
			if len(gauges) > 0 || len(counters) > 0 {
				c.makeRequest(map[string]interface{}{
					"gauges":   gauges,
					"counters": counters,
				})
				gauges, counters = nil, nil
			}
		default:
			closed := 0
			if item, ok := c.collateGauges.Pop(); ok {
				gauges = append(gauges, item)
			} else {
				closed++
			}
			if item, ok := c.collateCounters.Pop(); ok {
				counters = append(counters, item)
			} else {
				closed++
			}

			if closed == 2 {
				t.Stop()
				c.makeRequest(map[string]interface{}{
					"gauges":   gauges,
					"counters": counters,
				})
				gauges, counters = nil, nil
				close(c.stop)
			}
		}
	}
}

func (c *TimeCollatedClient) SetHTTPClient(client *http.Client) {
	c.client = client
}

func (c *TimeCollatedClient) Close() {
	for _, g := range c.gauges {
		g.Close()
		g.Wait()
	}
	for _, c := range c.counters {
		c.Close()
		c.Wait()
	}
	c.collateGauges.Close()
	c.collateGauges.Wait()
	c.collateCounters.Close()
	c.collateCounters.Wait()
}

func (c *TimeCollatedClient) Wait() {
	<-c.stop
}

func (c *TimeCollatedClient) GetGauge(name string) BufferedChan {
	ch, ok := c.gauges[name]
	if !ok {
		ch = newBufferedChan(2 << 9)
		c.gauges[name] = ch
		go c.runMetric(name, ch, c.collateGauges)
	}
	return ch
}

func (c *TimeCollatedClient) GetCounter(name string) BufferedChan {
	ch, ok := c.counters[name]
	if !ok {
		ch = newBufferedChan(2 << 9)
		c.counters[name] = ch
		go c.runMetric(name, ch, c.collateCounters)
	}
	return ch
}

func (c *TimeCollatedClient) makeRequest(body map[string]interface{}) error {
	b, err := json.Marshal(body)
	if nil != err {
		return err
	}
	req, err := http.NewRequest(
		"POST",
		"https://metrics-api.librato.com/v1/metrics",
		bytes.NewBuffer(b),
	)
	if nil != err {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.SetBasicAuth(c.user, c.token)
	_, err = c.client.Do(req)
	return err
}

func (c *TimeCollatedClient) runMetric(name string, ch BufferedChan, collate BufferedChan) {
	c.wg.Add(1)
	for {
		item, ok := ch.Pop()
		if !ok {
			break
		}

		body := map[string]interface{}{
			"name":         name,
			"measure_time": time.Now().Unix(),
		}
		if c.source != "" {
			body["source"] = c.source
		}

		switch typedItem := item.(type) {
		case map[string]interface{}:
			for k, v := range typedItem {
				body[k] = v
			}
		default:
			body["value"] = item
		}

		if _, present := body["measure_time"]; !present {
			body["measure_time"] = time.Now().Unix()
		}

		collate.Push(body)
	}
	c.wg.Done()
}
