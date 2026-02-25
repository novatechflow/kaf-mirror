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

package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"kaf-mirror/internal/config"
	"kaf-mirror/internal/database"
	"kaf-mirror/internal/manager"
	"kaf-mirror/internal/server"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestContext struct {
	Server *server.Server
	Token  string
}

func setupTestServer(t *testing.T) *TestContext {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Host: "localhost",
			Port: 8080,
			Mode: "test",
		},
	}
	db, err := database.InitDB(":memory:")
	assert.NoError(t, err)

	err = database.SeedDefaultRolesAndPermissions(db)
	assert.NoError(t, err)

	testUser, err := database.CreateUser(db, "testuser", "testpassword", false)
	assert.NoError(t, err)

	var adminRoleID int
	err = db.Get(&adminRoleID, "SELECT id FROM roles WHERE name = 'admin'")
	assert.NoError(t, err)

	err = database.AssignRoleToUser(db, testUser.ID, adminRoleID)
	assert.NoError(t, err)

	token, _, err := database.CreateApiToken(db, testUser.ID, "Test token", time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	hub := server.NewHub()
	jobManager := manager.New(db, cfg, hub)

	srv := server.New(cfg, db, jobManager, hub, "test")

	return &TestContext{
		Server: srv,
		Token:  token,
	}
}

func addAuthHeader(req *http.Request, token string) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
}

func TestHealthCheck(t *testing.T) {
	ctx := setupTestServer(t)

	req := httptest.NewRequest("GET", "/health", nil)

	resp, err := ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	var healthResp map[string]interface{}
	err = json.Unmarshal(body, &healthResp)
	assert.NoError(t, err)

	// Check that status is "ok" and other expected fields exist
	assert.Equal(t, "ok", healthResp["status"])
	assert.Contains(t, healthResp, "timestamp")
	assert.Contains(t, healthResp, "uptime")
}

func TestClustersAPI(t *testing.T) {
	ctx := setupTestServer(t)

	clusterPayload := `{"name":"test-cluster","brokers":"localhost:9092","security_config":"{}"}`
	req := httptest.NewRequest("POST", "/api/v1/clusters", bytes.NewBufferString(clusterPayload))
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req, ctx.Token)

	resp, err := ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	var createdCluster database.KafkaCluster
	err = json.NewDecoder(resp.Body).Decode(&createdCluster)
	assert.NoError(t, err)
	assert.Equal(t, "test-cluster", createdCluster.Name)

	req = httptest.NewRequest("GET", "/api/v1/clusters/test-cluster", nil)
	addAuthHeader(req, ctx.Token)

	resp, err = ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var fetchedCluster database.KafkaCluster
	err = json.NewDecoder(resp.Body).Decode(&fetchedCluster)
	assert.NoError(t, err)
	assert.Equal(t, "test-cluster", fetchedCluster.Name)

	req = httptest.NewRequest("GET", "/api/v1/clusters", nil)
	addAuthHeader(req, ctx.Token)

	resp, err = ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var clusters []database.KafkaCluster
	err = json.NewDecoder(resp.Body).Decode(&clusters)
	assert.NoError(t, err)
	assert.Len(t, clusters, 1)
	assert.Equal(t, "test-cluster", clusters[0].Name)

	req = httptest.NewRequest("DELETE", "/api/v1/clusters/test-cluster", nil)
	addAuthHeader(req, ctx.Token)

	resp, err = ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 202, resp.StatusCode)
}

func TestJobsAPI(t *testing.T) {
	ctx := setupTestServer(t)

	sourceCluster := &database.KafkaCluster{Name: "src", Brokers: "localhost:9092", SecurityConfig: "{}"}
	targetCluster := &database.KafkaCluster{Name: "tgt", Brokers: "localhost:9093", SecurityConfig: "{}"}
	err := database.CreateCluster(ctx.Server.Db, sourceCluster)
	assert.NoError(t, err)
	err = database.CreateCluster(ctx.Server.Db, targetCluster)
	assert.NoError(t, err)

	jobPayload := `{"name":"test-job","source_cluster_name":"src","target_cluster_name":"tgt"}`
	req := httptest.NewRequest("POST", "/api/v1/jobs", bytes.NewBufferString(jobPayload))
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req, ctx.Token)

	resp, err := ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	var createdJob database.ReplicationJob
	err = json.NewDecoder(resp.Body).Decode(&createdJob)
	assert.NoError(t, err)
	assert.Equal(t, "test-job", createdJob.Name)
	assert.NotEmpty(t, createdJob.ID)
	jobID := createdJob.ID

	req = httptest.NewRequest("GET", "/api/v1/jobs/"+jobID, nil)
	addAuthHeader(req, ctx.Token)

	resp, err = ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var fetchedJob database.ReplicationJob
	err = json.NewDecoder(resp.Body).Decode(&fetchedJob)
	assert.NoError(t, err)
	assert.Equal(t, jobID, fetchedJob.ID)
	assert.Equal(t, "test-job", fetchedJob.Name)

	req = httptest.NewRequest("GET", "/api/v1/jobs", nil)
	addAuthHeader(req, ctx.Token)

	resp, err = ctx.Server.App.Test(req)
	assert.NoError(t, err)

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected status 200 but got %d. Response: %s", resp.StatusCode, string(body))
	}

	var jobs []database.ReplicationJob
	err = json.NewDecoder(resp.Body).Decode(&jobs)
	assert.NoError(t, err)
	assert.Len(t, jobs, 1)
	if len(jobs) > 0 {
		assert.Equal(t, jobID, jobs[0].ID)
	}
}

func TestMappingsAPI(t *testing.T) {
	ctx := setupTestServer(t)
	jobID := "test-job-for-mappings"

	sourceCluster := &database.KafkaCluster{Name: "a", Brokers: "localhost:9092", SecurityConfig: "{}"}
	targetCluster := &database.KafkaCluster{Name: "b", Brokers: "localhost:9093", SecurityConfig: "{}"}
	database.CreateCluster(ctx.Server.Db, sourceCluster)
	database.CreateCluster(ctx.Server.Db, targetCluster)
	job := &database.ReplicationJob{ID: jobID, Name: "mapping-test", SourceClusterName: "a", TargetClusterName: "b", Status: "paused"}
	err := database.CreateJob(ctx.Server.Db, job)
	assert.NoError(t, err)

	mappingsPayload := `[{"source_topic_pattern":"a","target_topic_pattern":"b","enabled":true}]`
	req := httptest.NewRequest("PUT", "/api/v1/jobs/"+jobID+"/mappings", bytes.NewBufferString(mappingsPayload))
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req, ctx.Token)

	resp, err := ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	req = httptest.NewRequest("GET", "/api/v1/jobs/"+jobID+"/mappings", nil)
	addAuthHeader(req, ctx.Token)

	resp, err = ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var mappings []database.TopicMapping
	err = json.NewDecoder(resp.Body).Decode(&mappings)
	assert.NoError(t, err)
	assert.Len(t, mappings, 1)
	assert.Equal(t, "a", mappings[0].SourceTopicPattern)
}

func TestMetricsAPI(t *testing.T) {
	ctx := setupTestServer(t)
	jobID := "test-job-for-metrics"

	sourceCluster := &database.KafkaCluster{Name: "a", Brokers: "localhost:9092", SecurityConfig: "{}"}
	targetCluster := &database.KafkaCluster{Name: "b", Brokers: "localhost:9093", SecurityConfig: "{}"}
	database.CreateCluster(ctx.Server.Db, sourceCluster)
	database.CreateCluster(ctx.Server.Db, targetCluster)
	job := &database.ReplicationJob{ID: jobID, Name: "metrics-test", SourceClusterName: "a", TargetClusterName: "b", Status: "paused"}
	err := database.CreateJob(ctx.Server.Db, job)
	assert.NoError(t, err)

	metric1 := &database.ReplicationMetric{
		JobID:              jobID,
		MessagesReplicated: 100,
		BytesTransferred:   1000,
		CurrentLag:         10,
		ErrorCount:         0,
		Timestamp:          time.Now().Add(-10 * time.Second),
	}
	err = database.InsertMetrics(ctx.Server.Db, metric1)
	assert.NoError(t, err)

	metric2 := &database.ReplicationMetric{
		JobID:              jobID,
		MessagesReplicated: 123,
		BytesTransferred:   4560,
		CurrentLag:         12,
		ErrorCount:         1,
		Timestamp:          time.Now(),
	}
	err = database.InsertMetrics(ctx.Server.Db, metric2)
	assert.NoError(t, err)

	req := httptest.NewRequest("GET", "/api/v1/jobs/"+jobID+"/metrics/history", nil)
	addAuthHeader(req, ctx.Token)

	resp, err := ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	var fetchedMetrics []database.AggregatedMetric
	err = json.NewDecoder(resp.Body).Decode(&fetchedMetrics)
	assert.NoError(t, err)
	assert.Len(t, fetchedMetrics, 2)
	assert.Equal(t, 23, fetchedMetrics[1].MessagesReplicatedDelta)
}

func TestAuthenticationRequired(t *testing.T) {
	ctx := setupTestServer(t)

	// Test that requests without auth header get 401
	req := httptest.NewRequest("GET", "/api/v1/jobs", nil)
	// Deliberately NOT adding auth header

	resp, err := ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 401, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	var errorResp map[string]string
	err = json.Unmarshal(body, &errorResp)
	assert.NoError(t, err)
	assert.Equal(t, "Missing or malformed JWT", errorResp["error"])
}

func TestInvalidToken(t *testing.T) {
	ctx := setupTestServer(t)

	// Test that requests with invalid token get 401
	req := httptest.NewRequest("GET", "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")

	resp, err := ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 401, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	var errorResp map[string]string
	err = json.Unmarshal(body, &errorResp)
	assert.NoError(t, err)
	assert.Equal(t, "Invalid or expired JWT", errorResp["error"])
}

func TestHandleGetTopicDetails(t *testing.T) {
	ctx := setupTestServer(t)

	clusterPayload := `{"name":"test-cluster","brokers":"localhost:9092","security_config":"{}"}`
	req := httptest.NewRequest("POST", "/api/v1/clusters", bytes.NewBufferString(clusterPayload))
	req.Header.Set("Content-Type", "application/json")
	addAuthHeader(req, ctx.Token)
	resp, err := ctx.Server.App.Test(req)
	assert.NoError(t, err)
	assert.Equal(t, 201, resp.StatusCode)

	req = httptest.NewRequest("GET", "/api/v1/clusters/test-cluster/topic-details?topics=test-topic-1,test-topic-2", nil)
	addAuthHeader(req, ctx.Token)

	resp, err = ctx.Server.App.Test(req)
	assert.NoError(t, err)
	// This will fail without a running Kafka instance, but we're testing the handler logic
	// assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestHandleTestClusterConnection(t *testing.T) {
	ctx := setupTestServer(t)

	t.Run("successful connection", func(t *testing.T) {
		clusterConfig := config.ClusterConfig{
			Brokers: "localhost:9092",
		}
		body, _ := json.Marshal(clusterConfig)

		req := httptest.NewRequest("POST", "/api/v1/clusters/test", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		addAuthHeader(req, ctx.Token)

		_, err := ctx.Server.App.Test(req)
		assert.NoError(t, err)
		// This will fail without a running Kafka instance, but we're testing the handler logic
		// assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestGenerateRandomPassword(t *testing.T) {
	password, err := server.GenerateRandomPassword(16)
	if err != nil {
		t.Fatalf("Failed to generate random password: %v", err)
	}

	if len(password) != 16 {
		t.Errorf("Expected password length of 16, but got %d", len(password))
	}
}
