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

package config_test

import (
	"kaf-mirror/internal/config"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	// Test loading the default config (no parameters needed)
	cfg, err := config.LoadConfig()
	assert.NoError(t, err)
	assert.NotNil(t, cfg)

	// Basic validation of config structure
	assert.NotEmpty(t, cfg.Server.Host)
	assert.Greater(t, cfg.Server.Port, 0)
	assert.Equal(t, 30, cfg.Database.RetentionDays)
}

func TestConfigValidate_RetentionBounds(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{Port: 8080},
		Clusters: map[string]config.ClusterConfig{
			"source": {Brokers: "localhost:9092"},
		},
	}

	err := cfg.Validate()
	assert.NoError(t, err)
	assert.Equal(t, 30, cfg.Database.RetentionDays)

	cfg.Database.RetentionDays = 7
	err = cfg.Validate()
	assert.NoError(t, err)

	cfg.Database.RetentionDays = 45
	err = cfg.Validate()
	assert.Error(t, err)
}
