package cmd_test

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Test cases for validateConfluentBrokers
func TestValidateConfluentBrokers(t *testing.T) {
	tests := []struct {
		name          string
		brokerString  string
		shouldSucceed bool
	}{
		// Valid Confluent Cloud formats
		{"Valid standard format", "pkc-12345.us-west-2.aws.confluent.cloud:9092", true},
		{"Valid Azure format", "pkc-xyz123.eastus.azure.confluent.cloud:9092", true},
		{"Valid with custom port", "pkc-test.region.aws.confluent.cloud:9093", true},
		{"Valid simple cluster name", "my-cluster.confluent.cloud:9092", true},
		{"Valid numeric cluster", "123456.confluent.cloud:9092", true},
		{"Valid mixed cluster", "test-123.confluent.cloud:443", true},

		// Invalid formats
		{"Wrong cloud format", "pkc-abcde.example.cloud:9092", false},
		{"Missing domain", "pkc-12345:9092", false},
		{"Wrong domain", "pkc-12345.kafka.com:9092", false},
		{"Missing port", "pkc-12345.confluent.cloud", false},
		{"Invalid port", "pkc-12345.confluent.cloud:abc", false},
		{"Empty string", "", false},
		{"Only domain", "confluent.cloud:9092", false},
		{"Multiple dots in cluster name", "pkc.12345.confluent.cloud:9092", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfluentBrokers(tt.brokerString)
			if tt.shouldSucceed {
				if err != nil {
					t.Errorf("validateConfluentBrokers(%q) should succeed but got error: %v", tt.brokerString, err)
				}
			} else {
				if err == nil {
					t.Errorf("validateConfluentBrokers(%q) should fail but succeeded", tt.brokerString)
				}
			}
		})
	}
}

// Test cases for validateRedpandaBrokers
func TestValidateRedpandaBrokers(t *testing.T) {
	tests := []struct {
		name          string
		brokerString  string
		shouldSucceed bool
	}{
		// Valid RedPanda Cloud formats
		{"Valid standard format", "seed-12345.redpanda.cloud:9092", true},
		{"Valid custom cluster", "my-cluster.redpanda.cloud:9092", true},
		{"Valid with different port", "test-cluster.redpanda.cloud:9093", true},
		{"Valid simple name", "prod.redpanda.cloud:443", true},
		{"Valid numeric cluster", "123456.redpanda.cloud:9092", true},
		{"Valid with hyphens", "test-prod-1.redpanda.cloud:9092", true},

		// Invalid formats
		{"Missing domain", "seed-12345:9092", false},
		{"Wrong domain", "seed-12345.kafka.com:9092", false},
		{"Missing port", "seed-12345.redpanda.cloud", false},
		{"Invalid port", "seed-12345.redpanda.cloud:abc", false},
		{"Empty string", "", false},
		{"Only domain", "redpanda.cloud:9092", false},
		{"Multiple dots in cluster name", "seed.12345.redpanda.cloud:9092", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRedpandaBrokers(tt.brokerString)
			if tt.shouldSucceed {
				if err != nil {
					t.Errorf("validateRedpandaBrokers(%q) should succeed but got error: %v", tt.brokerString, err)
				}
			} else {
				if err == nil {
					t.Errorf("validateRedpandaBrokers(%q) should fail but succeeded", tt.brokerString)
				}
			}
		})
	}
}

// Test cases for validateAzureBrokers
func TestValidateAzureBrokers(t *testing.T) {
	tests := []struct {
		name          string
		brokerString  string
		shouldSucceed bool
	}{
		// Valid Azure Event Hub formats
		{"Valid standard format", "mynamespace.servicebus.windows.net:9093", true},
		{"Valid with hyphens", "my-event-hub.servicebus.windows.net:9093", true},
		{"Valid numeric namespace", "eventhub123.servicebus.windows.net:9093", true},
		{"Valid mixed namespace", "test-123.servicebus.windows.net:9093", true},
		{"Valid custom port", "production.servicebus.windows.net:443", true},

		// Invalid formats
		{"Missing domain", "mynamespace:9093", false},
		{"Wrong domain", "mynamespace.eventhub.azure.com:9093", false},
		{"Missing port", "mynamespace.servicebus.windows.net", false},
		{"Invalid port", "mynamespace.servicebus.windows.net:abc", false},
		{"Empty string", "", false},
		{"Only domain", "servicebus.windows.net:9093", false},
		{"Multiple dots in namespace", "my.namespace.servicebus.windows.net:9093", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAzureBrokers(tt.brokerString)
			if tt.shouldSucceed {
				if err != nil {
					t.Errorf("validateAzureBrokers(%q) should succeed but got error: %v", tt.brokerString, err)
				}
			} else {
				if err == nil {
					t.Errorf("validateAzureBrokers(%q) should fail but succeeded", tt.brokerString)
				}
			}
		})
	}
}

// Test cases for validatePlainBrokers
func TestValidatePlainBrokers(t *testing.T) {
	tests := []struct {
		name          string
		brokerString  string
		shouldSucceed bool
	}{
		// Valid plain Kafka formats
		{"Single broker", "localhost:9092", true},
		{"IP address", "192.168.1.100:9092", true},
		{"Hostname with domain", "kafka.example.com:9092", true},
		{"Multiple brokers", "broker1:9092,broker2:9093,broker3:9094", true},
		{"Mixed formats", "localhost:9092,192.168.1.1:9093,kafka.example.com:9094", true},
		{"Custom port", "kafka:8080", true},
		{"High port number", "broker:65535", true},

		// Invalid formats
		{"Missing port", "localhost", false},
		{"Invalid port", "localhost:abc", false},
		{"Port too high", "localhost:65536", false},
		{"Port zero", "localhost:0", false},
		{"Empty broker in list", "broker1:9092,,broker3:9094", false},
		{"Empty string", "", false},
		{"Only colon", ":", false},
		{"Only port", ":9092", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePlainBrokers(tt.brokerString)
			if tt.shouldSucceed {
				if err != nil {
					t.Errorf("validatePlainBrokers(%q) should succeed but got error: %v", tt.brokerString, err)
				}
			} else {
				if err == nil {
					t.Errorf("validatePlainBrokers(%q) should fail but succeeded", tt.brokerString)
				}
			}
		})
	}
}

// Test cases for validateBrokerString (main dispatcher function)
func TestValidateBrokerString(t *testing.T) {
	tests := []struct {
		name          string
		brokerString  string
		provider      string
		shouldSucceed bool
	}{
		// Confluent provider
		{"Confluent valid", "pkc-12345.us-west-2.aws.confluent.cloud:9092", "confluent", true},
		{"Confluent invalid", "invalid-broker:9092", "confluent", false},

		// RedPanda provider
		{"RedPanda valid", "my-cluster.redpanda.cloud:9092", "redpanda", true},
		{"RedPanda invalid", "invalid-broker:9092", "redpanda", false},

		// Azure provider
		{"Azure valid", "mynamespace.servicebus.windows.net:9093", "azure", true},
		{"Azure invalid", "invalid-broker:9092", "azure", false},

		// Plain provider
		{"Plain valid", "localhost:9092", "plain", true},
		{"Plain invalid", "localhost", "plain", false},

		// Unknown provider (defaults to plain)
		{"Unknown provider", "localhost:9092", "unknown", true},
		{"Unknown provider invalid", "localhost", "unknown", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBrokerString(tt.brokerString, tt.provider)
			if tt.shouldSucceed {
				if err != nil {
					t.Errorf("validateBrokerString(%q, %q) should succeed but got error: %v", tt.brokerString, tt.provider, err)
				}
			} else {
				if err == nil {
					t.Errorf("validateBrokerString(%q, %q) should fail but succeeded", tt.brokerString, tt.provider)
				}
			}
		})
	}
}

// Test real-world broker string examples
func TestRealWorldBrokerExamples(t *testing.T) {
	realWorldTests := []struct {
		name          string
		brokerString  string
		provider      string
		description   string
		shouldSucceed bool
	}{
		{
			"Real Confluent Cloud",
			"pkc-4nym6.us-east-1.aws.confluent.cloud:9092",
			"confluent",
			"Actual Confluent Cloud bootstrap server format",
			true,
		},
		{
			"Real RedPanda Cloud",
			"seed-deadbeef.redpanda.cloud:9092",
			"redpanda",
			"Actual RedPanda Cloud bootstrap server format",
			true,
		},
		{
			"Real Azure Event Hub",
			"my-eventhub-namespace.servicebus.windows.net:9093",
			"azure",
			"Actual Azure Event Hub format",
			true,
		},
		{
			"Kafka on Kubernetes",
			"kafka-cluster.kafka.svc.cluster.local:9092",
			"plain",
			"Kafka running in Kubernetes",
			true,
		},
		{
			"Multiple plain brokers",
			"kafka-0:9092,kafka-1:9092,kafka-2:9092",
			"plain",
			"Multiple Kafka brokers in cluster",
			true,
		},
	}

	for _, tt := range realWorldTests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBrokerString(tt.brokerString, tt.provider)
			if tt.shouldSucceed {
				if err != nil {
					t.Errorf("%s: validateBrokerString(%q, %q) should succeed but got error: %v",
						tt.description, tt.brokerString, tt.provider, err)
				}
			} else {
				if err == nil {
					t.Errorf("%s: validateBrokerString(%q, %q) should fail but succeeded",
						tt.description, tt.brokerString, tt.provider)
				}
			}
		})
	}
}

func validateConfluentBrokers(brokerString string) error {
	if strings.TrimSpace(brokerString) == "" {
		return fmt.Errorf("broker string cannot be empty")
	}

	// Check if it ends with .confluent.cloud:port
	if !strings.Contains(brokerString, ".confluent.cloud:") {
		return fmt.Errorf("invalid Confluent Cloud broker format: %s (expected format: cluster-name[.region.provider].confluent.cloud:port)", brokerString)
	}

	// Split on .confluent.cloud: to separate the cluster/region part from port
	beforeConfluent := strings.Split(brokerString, ".confluent.cloud:")[0]
	portPart := strings.Split(brokerString, ".confluent.cloud:")[1]

	// Validate port
	if _, err := strconv.Atoi(portPart); err != nil {
		return fmt.Errorf("invalid port in Confluent Cloud broker: %s", brokerString)
	}

	// Split the part before .confluent.cloud by dots
	parts := strings.Split(beforeConfluent, ".")

	// For simple format like "cluster.confluent.cloud", we should have 1 part
	// For complex format like "cluster.region.provider.confluent.cloud", we should have 3+ parts
	// But we need to reject formats like "pkc.12345.confluent.cloud" where the cluster name contains dots

	if len(parts) == 2 {
		// This is the problematic case like "pkc.12345" - cluster name with dots but not full region.provider format
		return fmt.Errorf("invalid Confluent Cloud broker format: cluster name cannot contain dots: %s", brokerString)
	}

	if len(parts) < 1 {
		return fmt.Errorf("invalid Confluent Cloud broker format: missing cluster name: %s", brokerString)
	}

	// First part should not be empty (cluster name)
	if parts[0] == "" {
		return fmt.Errorf("invalid Confluent Cloud broker format: empty cluster name: %s", brokerString)
	}

	return nil
}

func validateRedpandaBrokers(brokerString string) error {
	if strings.TrimSpace(brokerString) == "" {
		return fmt.Errorf("broker string cannot be empty")
	}

	pattern := `^[^.]+\.redpanda\.cloud:[0-9]+$`
	matched, err := regexp.MatchString(pattern, brokerString)
	if err != nil {
		return err
	}

	if !matched {
		return fmt.Errorf("invalid RedPanda Cloud broker format: %s (expected format: cluster-name.redpanda.cloud:port)", brokerString)
	}

	return nil
}

func validateAzureBrokers(brokerString string) error {
	if strings.TrimSpace(brokerString) == "" {
		return fmt.Errorf("broker string cannot be empty")
	}

	pattern := `^[^.]+\.servicebus\.windows\.net:[0-9]+$`
	matched, err := regexp.MatchString(pattern, brokerString)
	if err != nil {
		return err
	}

	if !matched {
		return fmt.Errorf("invalid Azure Event Hub broker format: %s (expected format: namespace.servicebus.windows.net:port)", brokerString)
	}

	return nil
}

func validatePlainBrokers(brokerString string) error {
	if strings.TrimSpace(brokerString) == "" {
		return fmt.Errorf("broker string cannot be empty")
	}

	brokers := strings.Split(brokerString, ",")
	for _, broker := range brokers {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return fmt.Errorf("empty broker in broker list")
		}

		if !strings.Contains(broker, ":") {
			return fmt.Errorf("broker must include port: %s", broker)
		}

		parts := strings.Split(broker, ":")
		if len(parts) != 2 {
			return fmt.Errorf("invalid broker format: %s", broker)
		}

		hostname := parts[0]
		if hostname == "" {
			return fmt.Errorf("hostname cannot be empty: %s", broker)
		}

		portStr := parts[1]
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return fmt.Errorf("invalid port in broker: %s", broker)
		}

		if port < 1 || port > 65535 {
			return fmt.Errorf("port out of range (1-65535): %d", port)
		}
	}

	return nil
}

func validateBrokerString(brokerString, provider string) error {
	switch strings.ToLower(provider) {
	case "confluent":
		return validateConfluentBrokers(brokerString)
	case "redpanda":
		return validateRedpandaBrokers(brokerString)
	case "azure":
		return validateAzureBrokers(brokerString)
	case "plain":
		return validatePlainBrokers(brokerString)
	default:
		return validatePlainBrokers(brokerString)
	}
}
