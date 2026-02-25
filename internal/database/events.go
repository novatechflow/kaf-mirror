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

package database

import (
	"github.com/jmoiron/sqlx"
)

// CreateOperationalEvent inserts a new operational event into the database.
func CreateOperationalEvent(db *sqlx.DB, event *OperationalEvent) error {
	query := `INSERT INTO operational_events (event_type, initiator, details)
              VALUES (?, ?, ?)`
	_, err := db.Exec(query, event.EventType, event.Initiator, event.Details)
	return err
}

// GetOperationalEvent retrieves a single operational event by its ID.
func GetOperationalEvent(db *sqlx.DB, eventID int) (*OperationalEvent, error) {
	var event OperationalEvent
	err := db.Get(&event, "SELECT * FROM operational_events WHERE id = ?", eventID)
	return &event, err
}

// ListOperationalEvents retrieves all operational events from the database.
func ListOperationalEvents(db *sqlx.DB) ([]OperationalEvent, error) {
	var events []OperationalEvent
	err := db.Select(&events, "SELECT * FROM operational_events ORDER BY timestamp DESC LIMIT 100")
	return events, err
}
