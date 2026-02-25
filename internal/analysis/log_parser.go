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

package analysis

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// LogEntry represents a single parsed log entry.
type LogEntry struct {
	Timestamp     time.Time
	Level         string
	Component     string
	Message       string
	AICategory    string
	AISubcategory string
	JobID         string
}

// GetLogsForJob retrieves logs for a specific jobID from the log file.
func GetLogsForJob(logDir, jobID string, since time.Time) ([]LogEntry, error) {
	var entries []LogEntry

	// Assume log file is named with the current date
	timestamp := time.Now().Format("2006-01-02")
	logFile := filepath.Join(logDir, fmt.Sprintf("kaf-mirror-%s.log", timestamp))

	file, err := os.Open(logFile)
	if err != nil {
		if os.IsNotExist(err) {
			return entries, nil // No log file for today yet
		}
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Include both AI-tagged logs for the job and regular job logs
		if strings.Contains(line, fmt.Sprintf("[job:%s]", jobID)) ||
			(strings.Contains(line, "[AI:") && strings.Contains(line, jobID)) {
			entry, err := parseLogLine(line)
			if err == nil && entry.Timestamp.After(since) {
				entries = append(entries, entry)
			}
		}
	}

	return entries, scanner.Err()
}

// parseLogLine parses a single line from the log file.
func parseLogLine(line string) (LogEntry, error) {
	var entry LogEntry

	// Parse timestamp - first bracketed section
	if !strings.HasPrefix(line, "[") {
		return entry, fmt.Errorf("invalid log line format - no timestamp")
	}

	tsEnd := strings.Index(line, "]")
	if tsEnd == -1 {
		return entry, fmt.Errorf("invalid log line format - unclosed timestamp")
	}

	tsStr := line[1:tsEnd]
	ts, err := time.Parse("2006-01-02 15:04:05.000", tsStr)
	if err != nil {
		return entry, err
	}
	entry.Timestamp = ts

	// Parse level - after timestamp
	remaining := line[tsEnd+1:]
	remaining = strings.TrimSpace(remaining)

	levelEnd := strings.Index(remaining, " ")
	if levelEnd == -1 {
		return entry, fmt.Errorf("invalid log line format - no level")
	}
	entry.Level = remaining[:levelEnd]
	remaining = strings.TrimSpace(remaining[levelEnd:])

	// Parse AI tags if present: [AI:category:subcategory]
	if strings.HasPrefix(remaining, "[") && strings.Contains(remaining, "[AI:") {
		aiStart := strings.Index(remaining, "[AI:") + 4
		aiEnd := strings.Index(remaining[aiStart:], "]")
		if aiEnd != -1 {
			aiEnd += aiStart
			aiTag := remaining[aiStart:aiEnd]
			aiParts := strings.Split(aiTag, ":")
			if len(aiParts) >= 2 {
				entry.AICategory = aiParts[0]
				entry.AISubcategory = aiParts[1]
			}
			remaining = strings.TrimSpace(remaining[aiEnd+1:])
		}
	}

	// Parse job ID if present: [job:jobID]
	if strings.Contains(remaining, "[job:") {
		jobStart := strings.Index(remaining, "[job:") + 5
		jobEnd := strings.Index(remaining[jobStart:], "]")
		if jobEnd != -1 {
			jobEnd += jobStart
			entry.JobID = remaining[jobStart:jobEnd]
			remaining = strings.TrimSpace(remaining[jobEnd+1:])
		}
	}

	// Parse component if present (legacy format)
	if strings.Contains(remaining, "[component:") {
		compStart := strings.Index(remaining, "[component:") + 11
		compEnd := strings.Index(remaining[compStart:], "]")
		if compEnd != -1 {
			compEnd += compStart
			entry.Component = remaining[compStart:compEnd]
			remaining = strings.TrimSpace(remaining[compEnd+1:])
		}
	}

	// Skip caller info [caller.go:123] and get message
	if strings.HasPrefix(remaining, "[") {
		callerEnd := strings.Index(remaining, "]")
		if callerEnd != -1 {
			remaining = strings.TrimSpace(remaining[callerEnd+1:])
		}
	}

	entry.Message = remaining
	return entry, nil
}
