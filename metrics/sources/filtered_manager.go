// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sources

import (
	"math/rand"
	"strings"
	"time"

	. "k8s.io/heapster/metrics/core"

	"github.com/golang/glog"
)

func NewFilteredSourceManager(metricsSourceProvider MetricsSourceProvider, metricsScrapeTimeout time.Duration) (FilteredMetricsSource, error) {
	fsm := &filteredSourceManager{
		metricsSourceProvider: metricsSourceProvider,
		metricsScrapeTimeout:  metricsScrapeTimeout,
		nodesToScrape:         make(map[string]interface{}),
		nodesChan:             make(chan NodeEvent, 100),
	}

	go func(fsm *filteredSourceManager) {
		for {
			select {
			case e := <-fsm.nodesChan:
				if e.Event == EventAdd {
					fsm.nodesToScrape[e.NodeIP] = struct{}{}
				} else if e.Event == EventDelete {
					if _, exists := fsm.nodesToScrape[e.NodeIP]; exists {
						delete(fsm.nodesToScrape, e.NodeIP)
					}
				}
			case <-time.After(time.Second * 3):
			}
		}
	}(fsm)

	return fsm, nil
}

type filteredSourceManager struct {
	metricsSourceProvider MetricsSourceProvider
	metricsScrapeTimeout  time.Duration

	nodesToScrape map[string]interface{}
	nodesChan     chan NodeEvent
}

func (this *filteredSourceManager) Name() string {
	return "filtered_source_manager"
}

func (this *filteredSourceManager) ScrapeMetrics(start, end time.Time) *DataBatch {
	glog.V(1).Infof("Scraping metrics start: %s, end: %s", start, end)
	sources := this.filterSources(this.metricsSourceProvider.GetMetricsSources())

	responseChannel := make(chan *DataBatch)
	startTime := time.Now()
	timeoutTime := startTime.Add(this.metricsScrapeTimeout)

	delayMs := DelayPerSourceMs * len(sources)
	if delayMs > MaxDelayMs {
		delayMs = MaxDelayMs
	}

	for _, source := range sources {

		go func(source MetricsSource, channel chan *DataBatch, start, end, timeoutTime time.Time, delayInMs int) {

			// Prevents network congestion.
			time.Sleep(time.Duration(rand.Intn(delayMs)) * time.Millisecond)

			glog.V(2).Infof("Querying source: %s", source)
			metrics := scrape(source, start, end)
			now := time.Now()
			if !now.Before(timeoutTime) {
				glog.Warningf("Failed to get %s response in time", source)
				return
			}
			timeForResponse := timeoutTime.Sub(now)

			select {
			case channel <- metrics:
				// passed the response correctly.
				return
			case <-time.After(timeForResponse):
				glog.Warningf("Failed to send the response back %s", source)
				return
			}
		}(source, responseChannel, start, end, timeoutTime, delayMs)
	}
	response := DataBatch{
		Timestamp:  end,
		MetricSets: map[string]*MetricSet{},
	}

	latencies := make([]int, 11)

responseloop:
	for i := range sources {
		now := time.Now()
		if !now.Before(timeoutTime) {
			glog.Warningf("Failed to get all responses in time (got %d/%d)", i, len(sources))
			break
		}

		select {
		case dataBatch := <-responseChannel:
			if dataBatch != nil {
				for key, value := range dataBatch.MetricSets {
					response.MetricSets[key] = value
				}
			}
			latency := now.Sub(startTime)
			bucket := int(latency.Seconds())
			if bucket >= len(latencies) {
				bucket = len(latencies) - 1
			}
			latencies[bucket]++

		case <-time.After(timeoutTime.Sub(now)):
			glog.Warningf("Failed to get all responses in time (got %d/%d)", i, len(sources))
			break responseloop
		}
	}

	glog.V(1).Infof("ScrapeMetrics: time: %s size: %d", time.Since(startTime), len(response.MetricSets))
	for i, value := range latencies {
		glog.V(1).Infof("   scrape  bucket %d: %d", i, value)
	}
	return &response
}

func (this *filteredSourceManager) GetNotifyChan() chan<- NodeEvent {
	return this.nodesChan
}

func (this *filteredSourceManager) filterSources(sources []MetricsSource) []MetricsSource {
	var filtered []MetricsSource
	for _, s := range sources {
		if _, exists := this.nodesToScrape[strings.Split(s.Name(), ":")[1]]; exists {
			filtered = append(filtered, s)
		}
	}
	return filtered
}
