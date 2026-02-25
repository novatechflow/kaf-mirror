// Copyright 2025 Scalytics, Inc. and Scalytics Europe, LTD
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"sync"
	"time"
)

type CacheEntry struct {
	Data      interface{}
	Timestamp time.Time
	TTL       time.Duration
}

func (ce *CacheEntry) IsExpired() bool {
	return time.Since(ce.Timestamp) > ce.TTL
}

type DataManager struct {
	cache      map[string]*CacheEntry
	cacheMutex sync.RWMutex
	token      string

	fetchJobs        func() ([]map[string]interface{}, error)
	fetchClusters    func() ([]map[string]interface{}, error)
	fetchMetrics     func(jobID string) (map[string]interface{}, error)
	fetchLogs        func() ([]map[string]interface{}, error)
	fetchAIInsights  func() ([]map[string]interface{}, error)
	fetchMe          func() (map[string]interface{}, error)
	fetchJob         func(jobID string) (map[string]interface{}, error)
	fetchCluster     func(clusterName string) (map[string]interface{}, error)
	fetchJobMappings func(jobID string) ([]map[string]interface{}, error)
}

type DataFetchers struct {
	FetchJobs        func() ([]map[string]interface{}, error)
	FetchClusters    func() ([]map[string]interface{}, error)
	FetchMetrics     func(jobID string) (map[string]interface{}, error)
	FetchLogs        func() ([]map[string]interface{}, error)
	FetchAIInsights  func() ([]map[string]interface{}, error)
	FetchMe          func() (map[string]interface{}, error)
	FetchJob         func(jobID string) (map[string]interface{}, error)
	FetchCluster     func(clusterName string) (map[string]interface{}, error)
	FetchJobMappings func(jobID string) ([]map[string]interface{}, error)
}

func NewDataManager(token string, fetchers DataFetchers) *DataManager {
	return &DataManager{
		cache:            make(map[string]*CacheEntry),
		token:            token,
		fetchJobs:        fetchers.FetchJobs,
		fetchClusters:    fetchers.FetchClusters,
		fetchMetrics:     fetchers.FetchMetrics,
		fetchLogs:        fetchers.FetchLogs,
		fetchAIInsights:  fetchers.FetchAIInsights,
		fetchMe:          fetchers.FetchMe,
		fetchJob:         fetchers.FetchJob,
		fetchCluster:     fetchers.FetchCluster,
		fetchJobMappings: fetchers.FetchJobMappings,
	}
}

const (
	LiveMetricsTTL    = 2 * time.Second  // High-frequency updates for live metrics
	ListDataTTL       = 5 * time.Second  // Medium-frequency for job/cluster lists
	HistoricalDataTTL = 30 * time.Second // Low-frequency for historical/detail data
)

func (dm *DataManager) getCachedData(key string, ttl time.Duration, fetchFunc func() (interface{}, error)) (interface{}, error) {
	dm.cacheMutex.RLock()
	entry, exists := dm.cache[key]
	dm.cacheMutex.RUnlock()

	if exists && !entry.IsExpired() {
		return entry.Data, nil
	}

	data, err := fetchFunc()
	if err != nil {
		// If fetch fails but we have cached data, return it even if expired
		if exists {
			return entry.Data, nil
		}
		return nil, err
	}

	dm.cacheMutex.Lock()
	dm.cache[key] = &CacheEntry{
		Data:      data,
		Timestamp: time.Now(),
		TTL:       ttl,
	}
	dm.cacheMutex.Unlock()

	return data, nil
}

func (dm *DataManager) GetJobs() ([]map[string]interface{}, error) {
	data, err := dm.getCachedData("jobs", ListDataTTL, func() (interface{}, error) {
		return dm.fetchJobs()
	})
	if err != nil {
		return nil, err
	}
	return data.([]map[string]interface{}), nil
}

func (dm *DataManager) GetClusters() ([]map[string]interface{}, error) {
	data, err := dm.getCachedData("clusters", ListDataTTL, func() (interface{}, error) {
		return dm.fetchClusters()
	})
	if err != nil {
		return nil, err
	}
	return data.([]map[string]interface{}), nil
}

func (dm *DataManager) GetJobMetrics(jobID string) (map[string]interface{}, error) {
	key := "metrics_" + jobID
	data, err := dm.getCachedData(key, LiveMetricsTTL, func() (interface{}, error) {
		return dm.fetchMetrics(jobID)
	})
	if err != nil {
		return nil, err
	}
	return data.(map[string]interface{}), nil
}

func (dm *DataManager) GetLogs() ([]map[string]interface{}, error) {
	data, err := dm.getCachedData("logs", ListDataTTL, func() (interface{}, error) {
		return dm.fetchLogs()
	})
	if err != nil {
		return nil, err
	}
	return data.([]map[string]interface{}), nil
}

func (dm *DataManager) GetAIInsights() ([]map[string]interface{}, error) {
	data, err := dm.getCachedData("ai_insights", ListDataTTL, func() (interface{}, error) {
		return dm.fetchAIInsights()
	})
	if err != nil {
		return nil, err
	}
	return data.([]map[string]interface{}), nil
}

func (dm *DataManager) GetUserInfo() (map[string]interface{}, error) {
	data, err := dm.getCachedData("user_info", HistoricalDataTTL, func() (interface{}, error) {
		return dm.fetchMe()
	})
	if err != nil {
		return nil, err
	}
	return data.(map[string]interface{}), nil
}

func (dm *DataManager) GetJobDetails(jobID string) (map[string]interface{}, error) {
	key := "job_details_" + jobID
	data, err := dm.getCachedData(key, HistoricalDataTTL, func() (interface{}, error) {
		return dm.fetchJob(jobID)
	})
	if err != nil {
		return nil, err
	}
	return data.(map[string]interface{}), nil
}

func (dm *DataManager) GetClusterDetails(clusterName string) (map[string]interface{}, error) {
	key := "cluster_details_" + clusterName
	data, err := dm.getCachedData(key, HistoricalDataTTL, func() (interface{}, error) {
		return dm.fetchCluster(clusterName)
	})
	if err != nil {
		return nil, err
	}
	return data.(map[string]interface{}), nil
}

func (dm *DataManager) GetJobMappings(jobID string) ([]map[string]interface{}, error) {
	key := "job_mappings_" + jobID
	data, err := dm.getCachedData(key, HistoricalDataTTL, func() (interface{}, error) {
		return dm.fetchJobMappings(jobID)
	})
	if err != nil {
		return nil, err
	}
	return data.([]map[string]interface{}), nil
}

func (dm *DataManager) InvalidateCache(key string) {
	dm.cacheMutex.Lock()
	delete(dm.cache, key)
	dm.cacheMutex.Unlock()
}

func (dm *DataManager) InvalidateAllCache() {
	dm.cacheMutex.Lock()
	dm.cache = make(map[string]*CacheEntry)
	dm.cacheMutex.Unlock()
}

func (dm *DataManager) GetCacheStats() map[string]int {
	dm.cacheMutex.RLock()
	defer dm.cacheMutex.RUnlock()

	stats := map[string]int{
		"total_entries": len(dm.cache),
		"expired":       0,
		"valid":         0,
	}

	for _, entry := range dm.cache {
		if entry.IsExpired() {
			stats["expired"]++
		} else {
			stats["valid"]++
		}
	}

	return stats
}

func (dm *DataManager) CleanExpiredEntries() {
	dm.cacheMutex.Lock()
	defer dm.cacheMutex.Unlock()

	for key, entry := range dm.cache {
		if entry.IsExpired() {
			delete(dm.cache, key)
		}
	}
}
