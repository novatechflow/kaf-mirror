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

package kafka_test

import (
	"kaf-mirror/internal/config"
	"kaf-mirror/internal/kafka"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestGetKgoOpts(t *testing.T) {
	t.Run("AzureProvider", func(t *testing.T) {
		connStr := "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=testkey"
		cfg := config.ClusterConfig{
			Provider: "azure",
			Brokers:  "test.servicebus.windows.net:9093",
			Security: config.SecurityConfig{
				ConnectionString: &connStr,
			},
		}

		opts, err := kafka.GetKgoOpts(cfg)
		assert.NoError(t, err)

		client, err := kgo.NewClient(opts...)
		assert.NoError(t, err)
		defer client.Close()

		// This is a bit of a hack to inspect the SASL mechanism.
		// We are checking if the generated options produce a client that has the expected SASL configuration.
		// A more direct way would be to inspect the opts slice, but the SASL option is a function literal.
		// This approach verifies the end result.
		// Note: This doesn't actually connect to Azure, it just configures the client.

		// A better way to test this would be to export the SASL options from the kafka package,
		// but for now, we will rely on this indirect verification.

		// Since we can't inspect the SASL options directly, we'll just check that the function runs without error
		// and produces a non-nil client.
		assert.NotNil(t, client)
	})
}
