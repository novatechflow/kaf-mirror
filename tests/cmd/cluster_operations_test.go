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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type ClusterConfig struct {
	Name             string            `json:"name"`
	Provider         string            `json:"provider"`
	ClusterID        string            `json:"cluster_id,omitempty"`
	Brokers          string            `json:"brokers"`
	APIKey           string            `json:"api_key,omitempty"`
	APISecret        string            `json:"api_secret,omitempty"`
	ConnectionString string            `json:"connection_string,omitempty"`
	Security         map[string]string `json:"security,omitempty"`
}

func TestClusterConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      ClusterConfig
		expectValid bool
		errorMsg    string
	}{
		{
			name: "Valid Plain Kafka Cluster",
			config: ClusterConfig{
				Name:     "test-plain",
				Provider: "plain",
				Brokers:  "localhost:9092,localhost:9093",
			},
			expectValid: true,
		},
		{
			name: "Valid Confluent Cluster",
			config: ClusterConfig{
				Name:      "test-confluent",
				Provider:  "confluent",
				ClusterID: "lkc-abc123",
				Brokers:   "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				APIKey:    "CONFLUENT_API_KEY",
				APISecret: "confluent-api-secret-123",
			},
			expectValid: true,
		},
		{
			name: "Valid Azure Event Hubs Cluster",
			config: ClusterConfig{
				Name:             "test-azure",
				Provider:         "azure",
				Brokers:          "mynamespace.servicebus.windows.net:9093",
				ConnectionString: "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123",
			},
			expectValid: true,
		},
		{
			name: "Valid RedPanda Cluster",
			config: ClusterConfig{
				Name:     "test-redpanda",
				Provider: "redpanda",
				Brokers:  "redpanda-0.redpanda.redpanda.svc.cluster.local:9092",
			},
			expectValid: true,
		},
		{
			name: "Invalid Empty Name",
			config: ClusterConfig{
				Name:     "",
				Provider: "plain",
				Brokers:  "localhost:9092",
			},
			expectValid: false,
			errorMsg:    "cluster name cannot be empty",
		},
		{
			name: "Invalid Empty Brokers",
			config: ClusterConfig{
				Name:     "test",
				Provider: "plain",
				Brokers:  "",
			},
			expectValid: false,
			errorMsg:    "brokers cannot be empty",
		},
		{
			name: "Invalid Confluent Missing API Key",
			config: ClusterConfig{
				Name:      "test-confluent",
				Provider:  "confluent",
				ClusterID: "lkc-abc123",
				Brokers:   "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				APISecret: "confluent-api-secret-123",
			},
			expectValid: false,
			errorMsg:    "confluent provider requires api_key",
		},
		{
			name: "Invalid Confluent Missing API Secret",
			config: ClusterConfig{
				Name:      "test-confluent",
				Provider:  "confluent",
				ClusterID: "lkc-abc123",
				Brokers:   "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				APIKey:    "CONFLUENT_API_KEY",
			},
			expectValid: false,
			errorMsg:    "confluent provider requires api_secret",
		},
		{
			name: "Invalid Confluent Missing Cluster ID",
			config: ClusterConfig{
				Name:      "test-confluent",
				Provider:  "confluent",
				Brokers:   "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				APIKey:    "CONFLUENT_API_KEY",
				APISecret: "confluent-api-secret-123",
			},
			expectValid: false,
			errorMsg:    "confluent provider requires cluster_id",
		},
		{
			name: "Invalid Azure Missing Connection String",
			config: ClusterConfig{
				Name:     "test-azure",
				Provider: "azure",
				Brokers:  "mynamespace.servicebus.windows.net:9093",
			},
			expectValid: false,
			errorMsg:    "azure provider requires connection_string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClusterConfig(tt.config)
			if tt.expectValid {
				assert.NoError(t, err, "Expected valid config but got error: %v", err)
			} else {
				assert.Error(t, err, "Expected error for invalid config")
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg, "Error message should contain expected text")
				}
			}
		})
	}
}

func TestClusterRequestStructure(t *testing.T) {
	tests := []struct {
		name           string
		operation      string
		config         ClusterConfig
		expectedFields []string
	}{
		{
			name:      "Add Plain Kafka Cluster",
			operation: "add",
			config: ClusterConfig{
				Name:     "plain-cluster",
				Provider: "plain",
				Brokers:  "localhost:9092,localhost:9093",
			},
			expectedFields: []string{"name", "provider", "brokers"},
		},
		{
			name:      "Add Confluent Cluster",
			operation: "add",
			config: ClusterConfig{
				Name:      "confluent-cluster",
				Provider:  "confluent",
				ClusterID: "lkc-abc123",
				Brokers:   "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				APIKey:    "CONFLUENT_API_KEY",
				APISecret: "confluent-api-secret",
			},
			expectedFields: []string{"name", "provider", "cluster_id", "brokers", "api_key", "api_secret"},
		},
		{
			name:      "Add Azure Event Hubs Cluster",
			operation: "add",
			config: ClusterConfig{
				Name:             "azure-cluster",
				Provider:         "azure",
				Brokers:          "mynamespace.servicebus.windows.net:9093",
				ConnectionString: "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123",
			},
			expectedFields: []string{"name", "provider", "brokers", "connection_string"},
		},
		{
			name:      "Edit Confluent Cluster",
			operation: "edit",
			config: ClusterConfig{
				Name:      "confluent-updated",
				Provider:  "confluent",
				ClusterID: "lkc-updated",
				Brokers:   "pkc-updated.us-east-1.aws.confluent.cloud:9092",
				APIKey:    "UPDATED_API_KEY",
				APISecret: "updated-api-secret",
			},
			expectedFields: []string{"name", "provider", "cluster_id", "brokers", "api_key", "api_secret"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestPayload := buildClusterRequest(tt.config, tt.operation)

			// Verify all expected fields are present
			for _, field := range tt.expectedFields {
				assert.Contains(t, requestPayload, field, "Request should contain field: %s", field)
			}

			// Verify no empty required fields
			if tt.config.Name != "" {
				assert.Equal(t, tt.config.Name, requestPayload["name"], "Name should match")
			}
			if tt.config.Provider != "" {
				assert.Equal(t, tt.config.Provider, requestPayload["provider"], "Provider should match")
			}
			if tt.config.Brokers != "" {
				assert.Equal(t, tt.config.Brokers, requestPayload["brokers"], "Brokers should match")
			}
		})
	}
}

func TestBrokerStringFormatting(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedValid  bool
		expectedFormat string
		provider       string
	}{
		{
			name:           "Single Plain Kafka Broker",
			input:          "localhost:9092",
			expectedValid:  true,
			expectedFormat: "localhost:9092",
			provider:       "plain",
		},
		{
			name:           "Multiple Plain Kafka Brokers",
			input:          "broker1:9092,broker2:9092,broker3:9092",
			expectedValid:  true,
			expectedFormat: "broker1:9092,broker2:9092,broker3:9092",
			provider:       "plain",
		},
		{
			name:           "Confluent Cloud Bootstrap URL",
			input:          "pkc-abc123.us-west-2.aws.confluent.cloud:9092",
			expectedValid:  true,
			expectedFormat: "pkc-abc123.us-west-2.aws.confluent.cloud:9092",
			provider:       "confluent",
		},
		{
			name:           "Azure Event Hubs Endpoint",
			input:          "mynamespace.servicebus.windows.net:9093",
			expectedValid:  true,
			expectedFormat: "mynamespace.servicebus.windows.net:9093",
			provider:       "azure",
		},
		{
			name:           "RedPanda Kubernetes Service",
			input:          "redpanda-0.redpanda.redpanda.svc.cluster.local:9092",
			expectedValid:  true,
			expectedFormat: "redpanda-0.redpanda.redpanda.svc.cluster.local:9092",
			provider:       "redpanda",
		},
		{
			name:          "Invalid Empty Brokers",
			input:         "",
			expectedValid: false,
			provider:      "plain",
		},
		{
			name:          "Invalid Broker Format Missing Port",
			input:         "localhost",
			expectedValid: false,
			provider:      "plain",
		},
		{
			name:           "Brokers with Extra Spaces",
			input:          " broker1:9092 , broker2:9092 ",
			expectedValid:  true,
			expectedFormat: "broker1:9092,broker2:9092",
			provider:       "plain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatted, err := formatBrokerString(tt.input)
			if tt.expectedValid {
				assert.NoError(t, err, "Expected valid broker string")
				if tt.expectedFormat != "" {
					assert.Equal(t, tt.expectedFormat, formatted, "Formatted broker string should match expected")
				}
			} else {
				assert.Error(t, err, "Expected error for invalid broker string")
			}
		})
	}
}

func TestConnectionTestConfiguration(t *testing.T) {
	tests := []struct {
		name               string
		config             ClusterConfig
		expectedTest       map[string]interface{}
		shouldHaveSecurity bool
	}{
		{
			name: "Plain Kafka Connection Test",
			config: ClusterConfig{
				Name:     "test-plain",
				Provider: "plain",
				Brokers:  "localhost:9092",
			},
			expectedTest: map[string]interface{}{
				"provider": "plain",
				"brokers":  "localhost:9092",
				"security": map[string]string{
					"api_key":    "",
					"api_secret": "",
				},
			},
			shouldHaveSecurity: true,
		},
		{
			name: "Confluent Connection Test",
			config: ClusterConfig{
				Name:      "test-confluent",
				Provider:  "confluent",
				ClusterID: "lkc-abc123",
				Brokers:   "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				APIKey:    "CONFLUENT_KEY",
				APISecret: "confluent-secret",
			},
			expectedTest: map[string]interface{}{
				"provider":   "confluent",
				"brokers":    "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				"cluster_id": "lkc-abc123",
				"security": map[string]string{
					"api_key":    "CONFLUENT_KEY",
					"api_secret": "confluent-secret",
				},
			},
			shouldHaveSecurity: true,
		},
		{
			name: "Azure Connection Test",
			config: ClusterConfig{
				Name:             "test-azure",
				Provider:         "azure",
				Brokers:          "mynamespace.servicebus.windows.net:9093",
				ConnectionString: "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=secret",
			},
			expectedTest: map[string]interface{}{
				"provider":          "azure",
				"brokers":           "mynamespace.servicebus.windows.net:9093",
				"connection_string": "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=secret",
				"security": map[string]string{
					"api_key":    "",
					"api_secret": "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=secret",
				},
			},
			shouldHaveSecurity: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testConfig := buildConnectionTestConfig(tt.config)

			// Verify basic structure
			assert.Equal(t, tt.expectedTest["provider"], testConfig["provider"], "Provider should match")
			assert.Equal(t, tt.expectedTest["brokers"], testConfig["brokers"], "Brokers should match")

			// Verify security configuration
			if tt.shouldHaveSecurity {
				security, exists := testConfig["security"]
				assert.True(t, exists, "Security section should exist")
				securityMap, ok := security.(map[string]string)
				assert.True(t, ok, "Security should be a map[string]string")

				expectedSecurity := tt.expectedTest["security"].(map[string]string)
				assert.Equal(t, expectedSecurity["api_key"], securityMap["api_key"], "API key should match")
				assert.Equal(t, expectedSecurity["api_secret"], securityMap["api_secret"], "API secret should match")
			}

			// Verify provider-specific fields
			if tt.config.Provider == "confluent" {
				assert.Equal(t, tt.config.ClusterID, testConfig["cluster_id"], "Cluster ID should match")
			}
			if tt.config.Provider == "azure" {
				assert.Equal(t, tt.config.ConnectionString, testConfig["connection_string"], "Connection string should match")
			}
		})
	}
}

func TestClusterNameValidation(t *testing.T) {
	tests := []struct {
		name        string
		clusterName string
		expectValid bool
		errorMsg    string
	}{
		{
			name:        "Valid alphanumeric name",
			clusterName: "cluster1",
			expectValid: true,
		},
		{
			name:        "Valid name with hyphens",
			clusterName: "my-cluster-01",
			expectValid: true,
		},
		{
			name:        "Valid name with underscores",
			clusterName: "my_cluster_01",
			expectValid: true,
		},
		{
			name:        "Valid uppercase name",
			clusterName: "PROD-CLUSTER",
			expectValid: true,
		},
		{
			name:        "Valid mixed case",
			clusterName: "Dev-Cluster_01",
			expectValid: true,
		},
		{
			name:        "Empty name",
			clusterName: "",
			expectValid: false,
			errorMsg:    "cluster name cannot be empty",
		},
		{
			name:        "Name with spaces",
			clusterName: "my cluster",
			expectValid: false,
			errorMsg:    "cluster name cannot contain spaces",
		},
		{
			name:        "Name with special characters",
			clusterName: "cluster@prod",
			expectValid: false,
			errorMsg:    "cluster name contains invalid characters",
		},
		{
			name:        "Name starting with number",
			clusterName: "1cluster",
			expectValid: true, // Allow names starting with numbers
		},
		{
			name:        "Very long name",
			clusterName: strings.Repeat("a", 100),
			expectValid: false,
			errorMsg:    "cluster name too long",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateClusterName(tt.clusterName)
			if tt.expectValid {
				assert.NoError(t, err, "Expected valid cluster name")
			} else {
				assert.Error(t, err, "Expected error for invalid cluster name")
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg, "Error message should contain expected text")
				}
			}
		})
	}
}

func validateClusterConfig(config ClusterConfig) error {
	if err := validateClusterName(config.Name); err != nil {
		return err
	}

	if config.Brokers == "" {
		return fmt.Errorf("brokers cannot be empty")
	}

	switch config.Provider {
	case "confluent":
		if config.APIKey == "" {
			return fmt.Errorf("confluent provider requires api_key")
		}
		if config.APISecret == "" {
			return fmt.Errorf("confluent provider requires api_secret")
		}
		if config.ClusterID == "" {
			return fmt.Errorf("confluent provider requires cluster_id")
		}
	case "azure":
		if config.ConnectionString == "" {
			return fmt.Errorf("azure provider requires connection_string")
		}
	}

	return nil
}

func validateClusterName(name string) error {
	if name == "" {
		return fmt.Errorf("cluster name cannot be empty")
	}

	if len(name) > 50 {
		return fmt.Errorf("cluster name too long (max 50 characters)")
	}

	if strings.Contains(name, " ") {
		return fmt.Errorf("cluster name cannot contain spaces")
	}

	for _, char := range name {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '-' || char == '_') {
			return fmt.Errorf("cluster name contains invalid characters (only alphanumeric, hyphens, and underscores allowed)")
		}
	}

	return nil
}

func formatBrokerString(input string) (string, error) {
	if strings.TrimSpace(input) == "" {
		return "", fmt.Errorf("brokers cannot be empty")
	}

	brokers := strings.Split(input, ",")
	var formatted []string

	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			continue
		}

		if !strings.Contains(broker, ":") {
			return "", fmt.Errorf("invalid broker format: %s (should be host:port)", broker)
		}

		formatted = append(formatted, broker)
	}

	if len(formatted) == 0 {
		return "", fmt.Errorf("no valid brokers found")
	}

	return strings.Join(formatted, ","), nil
}

func buildClusterRequest(config ClusterConfig, operation string) map[string]interface{} {
	request := map[string]interface{}{
		"name":     config.Name,
		"provider": config.Provider,
		"brokers":  config.Brokers,
	}

	if config.ClusterID != "" {
		request["cluster_id"] = config.ClusterID
	}

	if config.APIKey != "" {
		request["api_key"] = config.APIKey
	}

	if config.APISecret != "" {
		request["api_secret"] = config.APISecret
	}

	if config.ConnectionString != "" {
		request["connection_string"] = config.ConnectionString
	}

	return request
}

func buildConnectionTestConfig(config ClusterConfig) map[string]interface{} {
	testConfig := map[string]interface{}{
		"provider": config.Provider,
		"brokers":  config.Brokers,
	}

	if config.ClusterID != "" {
		testConfig["cluster_id"] = config.ClusterID
	}

	if config.ConnectionString != "" {
		testConfig["connection_string"] = config.ConnectionString
	}

	security := map[string]string{
		"api_key":    config.APIKey,
		"api_secret": config.APISecret,
	}

	if config.Provider == "azure" {
		security["api_secret"] = config.ConnectionString
	}

	testConfig["security"] = security

	return testConfig
}

func TestJSONSerialization(t *testing.T) {
	tests := []struct {
		name   string
		config ClusterConfig
	}{
		{
			name: "Plain Kafka Cluster",
			config: ClusterConfig{
				Name:     "plain-cluster",
				Provider: "plain",
				Brokers:  "localhost:9092",
			},
		},
		{
			name: "Confluent Cluster",
			config: ClusterConfig{
				Name:      "confluent-cluster",
				Provider:  "confluent",
				ClusterID: "lkc-abc123",
				Brokers:   "pkc-xyz123.us-west-2.aws.confluent.cloud:9092",
				APIKey:    "CONFLUENT_KEY",
				APISecret: "confluent-secret",
			},
		},
		{
			name: "Azure Cluster",
			config: ClusterConfig{
				Name:             "azure-cluster",
				Provider:         "azure",
				Brokers:          "mynamespace.servicebus.windows.net:9093",
				ConnectionString: "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKey=secret",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonData, err := json.Marshal(tt.config)
			assert.NoError(t, err, "Should serialize to JSON without error")

			var deserialized ClusterConfig
			err = json.Unmarshal(jsonData, &deserialized)
			assert.NoError(t, err, "Should deserialize from JSON without error")

			assert.Equal(t, tt.config.Name, deserialized.Name, "Name should be preserved")
			assert.Equal(t, tt.config.Provider, deserialized.Provider, "Provider should be preserved")
			assert.Equal(t, tt.config.Brokers, deserialized.Brokers, "Brokers should be preserved")

			if tt.config.ClusterID != "" {
				assert.Equal(t, tt.config.ClusterID, deserialized.ClusterID, "Cluster ID should be preserved")
			}
			if tt.config.APIKey != "" {
				assert.Equal(t, tt.config.APIKey, deserialized.APIKey, "API Key should be preserved")
			}
			if tt.config.APISecret != "" {
				assert.Equal(t, tt.config.APISecret, deserialized.APISecret, "API Secret should be preserved")
			}
			if tt.config.ConnectionString != "" {
				assert.Equal(t, tt.config.ConnectionString, deserialized.ConnectionString, "Connection String should be preserved")
			}
		})
	}
}
