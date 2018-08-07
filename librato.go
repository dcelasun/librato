// package librato implements a Librato client with time.Duration based collation.
// See examples for details.
package librato

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
	"log"
	"os"
)

var (
	// Use this variable to set a custom logger or set it to nil to disable logging.
	Logger = log.New(os.Stderr, "librato", log.LstdFlags)

	// Librato suggests max. 300 measurements per POST. There is also an undocumented
	// payload size limit which triggers an HTTP 413 - Request Entity Too Large response.
	// So the client will make a request either at MaxMetrics measurements or when the timer
	// arrives, whichever happens first.
	MaxMetrics = 300
)

type Client interface {
	GetGauge(name string) Chan
	GetCounter(name string) Chan
	Close()
	Wait()
}

// TimeCollatedClient is Librato client with that collates metrics for `duration` and
// sends them to Librato in a single request.
type TimeCollatedClient struct {
	user, token, source string
	duration            time.Duration
	counters            map[string]Chan
	gauges              map[string]Chan
	collateCounters     Chan
	collateGauges       Chan
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
		counters:        make(map[string]Chan),
		gauges:          make(map[string]Chan),
		collateCounters: NewFlexibleChan(2 << 10),
		collateGauges:   NewFlexibleChan(2 << 10),
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
	closed := 0
	gaugeChan := c.collateGauges.Output()
	counterChan := c.collateCounters.Output()
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
		case item, ok := <-gaugeChan:
			if !ok {
				closed++
				gaugeChan = nil
				continue
			}
			gauges = append(gauges, item)
		case item, ok := <-counterChan:
			if !ok {
				closed++
				counterChan = nil
				continue
			}
			counters = append(counters, item)
		default:
			if closed == 2 {
				t.Stop()
				c.makeRequest(map[string]interface{}{
					"gauges":   gauges,
					"counters": counters,
				})
				gauges, counters = nil, nil
				close(c.stop)
				return
			} else if len(gauges) + len(counters) >= MaxMetrics {
				// Librato doesn't like requests with more than ~300 metrics
				// so we need to flush early, without waiting for the timer.
				c.makeRequest(map[string]interface{}{
					"gauges":   gauges,
					"counters": counters,
				})
				gauges, counters = nil, nil
			}

			time.Sleep(1 * time.Second)
		}
	}
}

// Set a custom HTTP client. Must be called before sending any metrics.
func (c *TimeCollatedClient) SetHTTPClient(client *http.Client) {
	c.client = client
}

func (c *TimeCollatedClient) Close() {
	for _, i := range c.gauges {
		func(c Chan) {
			c.Close()
			c.Wait()
		}(i)
	}
	for _, i := range c.counters {
		func(c Chan) {
			c.Close()
			c.Wait()
		}(i)
	}
	c.wg.Wait()
	c.collateGauges.Close()
	c.collateGauges.Wait()
	c.collateCounters.Close()
	c.collateCounters.Wait()
}

func (c *TimeCollatedClient) Wait() {
	<-c.stop
}

func (c *TimeCollatedClient) GetGauge(name string) Chan {
	ch, ok := c.gauges[name]
	if !ok {
		ch = NewFlexibleChan(2 << 9)
		c.gauges[name] = ch
		go c.runMetric(name, ch, c.collateGauges)
	}
	return ch
}

func (c *TimeCollatedClient) GetCounter(name string) Chan {
	ch, ok := c.counters[name]
	if !ok {
		ch = NewFlexibleChan(2 << 9)
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
	res, err := c.client.Do(req)
	// Do not discard response body in case of Librato errors
	// http://api-docs-archive.librato.com/#http-status-codes
	if err == nil && res.StatusCode <= 204 {
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}

	// http://api-docs-archive.librato.com/#http-status-codes
	if res.StatusCode > 204 && Logger != nil {
		b, _ :=ioutil.ReadAll(res.Body)
		res.Body.Close()
		Logger.Printf("status:%d, error: %s\n", res.StatusCode, string(b))
	}

	return err
}

func (c *TimeCollatedClient) runMetric(name string, ch Chan, collate Chan) {
	c.wg.Add(1)
	for {
		select {
		case item, ok := <-ch.Output():
			if !ok {
				c.wg.Done()
				return
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

			collate.Input() <- body
		}
	}
}
