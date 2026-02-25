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

package cmd_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockServer struct {
	server *httptest.Server
	calls  []APICall
}

type APICall struct {
	Method   string
	Path     string
	Headers  map[string]string
	Body     string
	Response int
}

func NewMockServer() *MockServer {
	ms := &MockServer{
		calls: make([]APICall, 0),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/clusters/test", ms.handleClusterTest)
	mux.HandleFunc("/api/v1/clusters", ms.handleClusters)
	mux.HandleFunc("/api/v1/clusters/", ms.handleClusterOperations)

	ms.server = httptest.NewServer(mux)
	return ms
}

func (ms *MockServer) Close() {
	ms.server.Close()
}

func (ms *MockServer) URL() string {
	return ms.server.URL
}

func (ms *MockServer) GetCalls() []APICall {
	return ms.calls
}

func (ms *MockServer) ClearCalls() {
	ms.calls = make([]APICall, 0)
}

func (ms *MockServer) recordCall(method, path string, headers map[string]string, body string, response int) {
	ms.calls = append(ms.calls, APICall{
		Method:   method,
		Path:     path,
		Headers:  headers,
		Body:     body,
		Response: response,
	})
}

func (ms *MockServer) handleClusterTest(w http.ResponseWriter, r *http.Request) {
	headers := make(map[string]string)
	for k, v := range r.Header {
		headers[k] = strings.Join(v, ",")
	}

	body := ""
	if r.Body != nil {
		buf := make([]byte, r.ContentLength)
		r.Body.Read(buf)
		body = string(buf)
	}

	var testConfig map[string]interface{}
	if err := json.Unmarshal([]byte(body), &testConfig); err != nil {
		ms.recordCall(r.Method, r.URL.Path, headers, body, 400)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"Invalid JSON"}`))
		return
	}

	provider, ok := testConfig["provider"].(string)
	if !ok {
		ms.recordCall(r.Method, r.URL.Path, headers, body, 400)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"Missing provider"}`))
		return
	}

	switch provider {
	case "confluent":
		security, ok := testConfig["security"].(map[string]interface{})
		if !ok {
			ms.recordCall(r.Method, r.URL.Path, headers, body, 400)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"Missing security configuration"}`))
			return
		}
		if security["api_key"] == "" || security["api_secret"] == "" {
			ms.recordCall(r.Method, r.URL.Path, headers, body, 503)
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"error":"confluent provider requires API key and secret"}`))
			return
		}
	case "azure":
		if testConfig["connection_string"] == "" {
			ms.recordCall(r.Method, r.URL.Path, headers, body, 400)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"azure provider requires connection_string"}`))
			return
		}
	}

	ms.recordCall(r.Method, r.URL.Path, headers, body, 200)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"connection successful"}`))
}

func (ms *MockServer) handleClusters(w http.ResponseWriter, r *http.Request) {
	headers := make(map[string]string)
	for k, v := range r.Header {
		headers[k] = strings.Join(v, ",")
	}

	body := ""
	if r.Body != nil {
		buf := make([]byte, r.ContentLength)
		r.Body.Read(buf)
		body = string(buf)
	}

	if r.Method == "POST" {
		var cluster map[string]interface{}
		if err := json.Unmarshal([]byte(body), &cluster); err != nil {
			ms.recordCall(r.Method, r.URL.Path, headers, body, 400)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		required := []string{"name", "provider", "brokers"}
		for _, field := range required {
			if cluster[field] == nil || cluster[field] == "" {
				ms.recordCall(r.Method, r.URL.Path, headers, body, 400)
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(fmt.Sprintf(`{"error":"Missing required field: %s"}`, field)))
				return
			}
		}

		ms.recordCall(r.Method, r.URL.Path, headers, body, 201)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"message":"cluster created successfully"}`))
	} else {
		ms.recordCall(r.Method, r.URL.Path, headers, body, 405)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (ms *MockServer) handleClusterOperations(w http.ResponseWriter, r *http.Request) {
	headers := make(map[string]string)
	for k, v := range r.Header {
		headers[k] = strings.Join(v, ",")
	}

	body := ""
	if r.Body != nil {
		buf := make([]byte, r.ContentLength)
		r.Body.Read(buf)
		body = string(buf)
	}

	pathParts := strings.Split(r.URL.Path, "/")
	if len(pathParts) < 4 {
		ms.recordCall(r.Method, r.URL.Path, headers, body, 400)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	clusterName := pathParts[4]

	switch r.Method {
	case "GET":
		mockCluster := map[string]interface{}{
			"name":       clusterName,
			"provider":   "confluent",
			"brokers":    "pkc-example.us-west1.example.cloud:9092",
			"cluster_id": "lkc-example",
			"api_key":    "EXAMPLE_KEY",
			"status":     "active",
		}

		ms.recordCall(r.Method, r.URL.Path, headers, body, 200)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(mockCluster)

	case "PUT":
		var cluster map[string]interface{}
		if err := json.Unmarshal([]byte(body), &cluster); err != nil {
			ms.recordCall(r.Method, r.URL.Path, headers, body, 400)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		brokers, exists := cluster["brokers"]
		if !exists || brokers == "" {
			ms.recordCall(r.Method, r.URL.Path, headers, body, 400)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"brokers field is required and cannot be empty"}`))
			return
		}

		ms.recordCall(r.Method, r.URL.Path, headers, body, 200)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"cluster updated successfully"}`))

	default:
		ms.recordCall(r.Method, r.URL.Path, headers, body, 405)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestClusterAddIntegration(t *testing.T) {
	mockServer := NewMockServer()
	defer mockServer.Close()

	tests := []struct {
		name           string
		clusterConfig  ClusterConfig
		expectTestCall bool
		expectAddCall  bool
		testResponse   int
		addResponse    int
	}{
		{
			name: "Successful Plain Kafka Add",
			clusterConfig: ClusterConfig{
				Name:     "test-plain",
				Provider: "plain",
				Brokers:  "localhost:9092",
			},
			expectTestCall: true,
			expectAddCall:  true,
			testResponse:   200,
			addResponse:    201,
		},
		{
			name: "Successful Confluent Add",
			clusterConfig: ClusterConfig{
				Name:      "test-confluent",
				Provider:  "confluent",
				ClusterID: "lkc-abc123",
				Brokers:   "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				APIKey:    "CONFLUENT_KEY",
				APISecret: "confluent-secret",
			},
			expectTestCall: true,
			expectAddCall:  true,
			testResponse:   200,
			addResponse:    201,
		},
		{
			name: "Failed Connection Test",
			clusterConfig: ClusterConfig{
				Name:     "test-confluent-fail",
				Provider: "confluent",
				Brokers:  "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				// Missing API credentials
			},
			expectTestCall: true,
			expectAddCall:  false,
			testResponse:   503,
			addResponse:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer.ClearCalls()

			err := simulateClusterAdd(mockServer.URL(), tt.clusterConfig)

			calls := mockServer.GetCalls()

			if tt.expectTestCall {
				assert.GreaterOrEqual(t, len(calls), 1, "Should have at least one API call")

				testCall := calls[0]
				assert.Equal(t, "POST", testCall.Method, "First call should be POST")
				assert.Equal(t, "/api/v1/clusters/test", testCall.Path, "First call should be to test endpoint")
				assert.Equal(t, tt.testResponse, testCall.Response, "Test response should match expected")

				var testConfig map[string]interface{}
				json.Unmarshal([]byte(testCall.Body), &testConfig)
				assert.Equal(t, tt.clusterConfig.Provider, testConfig["provider"], "Provider should match")
				assert.Equal(t, tt.clusterConfig.Brokers, testConfig["brokers"], "Brokers should match")
			}

			if tt.expectAddCall {
				assert.GreaterOrEqual(t, len(calls), 2, "Should have cluster creation call")

				addCall := calls[1]
				assert.Equal(t, "POST", addCall.Method, "Second call should be POST")
				assert.Equal(t, "/api/v1/clusters", addCall.Path, "Second call should be to clusters endpoint")
				assert.Equal(t, tt.addResponse, addCall.Response, "Add response should match expected")

				var addConfig map[string]interface{}
				json.Unmarshal([]byte(addCall.Body), &addConfig)
				assert.Equal(t, tt.clusterConfig.Name, addConfig["name"], "Name should match")
				assert.Equal(t, tt.clusterConfig.Provider, addConfig["provider"], "Provider should match")
				assert.Equal(t, tt.clusterConfig.Brokers, addConfig["brokers"], "Brokers should match")

				assert.NoError(t, err, "Add operation should succeed")
			} else {
				assert.Equal(t, 1, len(calls), "Should only have test call")
				assert.Error(t, err, "Add operation should fail")
			}
		})
	}
}

func TestClusterEditIntegration(t *testing.T) {
	mockServer := NewMockServer()
	defer mockServer.Close()

	tests := []struct {
		name          string
		clusterName   string
		newConfig     ClusterConfig
		expectGetCall bool
		expectPutCall bool
	}{
		{
			name:        "Successful Edit",
			clusterName: "existing-cluster",
			newConfig: ClusterConfig{
				Name:      "updated-cluster",
				Provider:  "confluent",
				ClusterID: "lkc-updated",
				Brokers:   "pkc-updated.us-east-1.aws.confluent.cloud:9092",
				APIKey:    "UPDATED_KEY",
				APISecret: "updated-secret",
			},
			expectGetCall: true,
			expectPutCall: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockServer.ClearCalls()

			err := simulateClusterEdit(mockServer.URL(), tt.clusterName, tt.newConfig)

			calls := mockServer.GetCalls()

			if tt.expectGetCall {
				assert.GreaterOrEqual(t, len(calls), 1, "Should have at least one API call")

				getCall := calls[0]
				assert.Equal(t, "GET", getCall.Method, "First call should be GET")
				assert.Contains(t, getCall.Path, tt.clusterName, "GET call should include cluster name")
			}

			if tt.expectPutCall {
				assert.GreaterOrEqual(t, len(calls), 3, "Should have multiple API calls for edit flow")

				var putCall *APICall
				for _, call := range calls {
					if call.Method == "PUT" {
						putCall = &call
						break
					}
				}

				assert.NotNil(t, putCall, "Should have PUT call")
				assert.Contains(t, putCall.Path, tt.clusterName, "PUT call should include cluster name")

				var updateConfig map[string]interface{}
				json.Unmarshal([]byte(putCall.Body), &updateConfig)

				assert.NotEmpty(t, updateConfig["brokers"], "Brokers field should not be empty in update request")
				assert.Equal(t, tt.newConfig.Brokers, updateConfig["brokers"], "Brokers should match new config")

				assert.NoError(t, err, "Edit operation should succeed")
			}
		})
	}
}

func TestSafeStringFunction(t *testing.T) {
	tests := []struct {
		name         string
		value        interface{}
		defaultValue string
		expected     string
	}{
		{
			name:         "String value",
			value:        "test-value",
			defaultValue: "default",
			expected:     "test-value",
		},
		{
			name:         "Nil value",
			value:        nil,
			defaultValue: "default",
			expected:     "default",
		},
		{
			name:         "Empty string",
			value:        "",
			defaultValue: "default",
			expected:     "",
		},
		{
			name:         "Integer value",
			value:        123,
			defaultValue: "default",
			expected:     "default", // Non-string should return default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := safeString(tt.value, tt.defaultValue)
			assert.Equal(t, tt.expected, result, "safeString should return expected value")
		})
	}
}

func simulateClusterAdd(serverURL string, config ClusterConfig) error {
	testConfig := buildConnectionTestConfig(config)
	testBody, _ := json.Marshal(testConfig)

	resp, err := http.Post(serverURL+"/api/v1/clusters/test", "application/json", strings.NewReader(string(testBody)))
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("connection test failed with status: %d", resp.StatusCode)
	}

	addRequest := buildClusterRequest(config, "add")
	addBody, _ := json.Marshal(addRequest)

	resp, err = http.Post(serverURL+"/api/v1/clusters", "application/json", strings.NewReader(string(addBody)))
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("cluster creation failed with status: %d", resp.StatusCode)
	}

	return nil
}

func simulateClusterEdit(serverURL, clusterName string, newConfig ClusterConfig) error {
	resp, err := http.Get(serverURL + "/api/v1/clusters/" + clusterName)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to get current cluster config: %d", resp.StatusCode)
	}

	testConfig := buildConnectionTestConfig(newConfig)
	testBody, _ := json.Marshal(testConfig)

	resp, err = http.Post(serverURL+"/api/v1/clusters/test", "application/json", strings.NewReader(string(testBody)))
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("connection test failed with status: %d", resp.StatusCode)
	}

	updateRequest := buildClusterRequest(newConfig, "edit")
	updateBody, _ := json.Marshal(updateRequest)

	req, _ := http.NewRequest("PUT", serverURL+"/api/v1/clusters/"+clusterName, strings.NewReader(string(updateBody)))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cluster update failed with status: %d", resp.StatusCode)
	}

	return nil
}

func safeString(value interface{}, defaultValue string) string {
	if value == nil {
		return defaultValue
	}
	if str, ok := value.(string); ok {
		return str
	}
	return defaultValue
}
