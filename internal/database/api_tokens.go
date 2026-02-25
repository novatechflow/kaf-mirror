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
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
)

// CreateApiToken creates a new API token for a user.
func CreateApiToken(db *sqlx.DB, userID int, description string, expiresAt time.Time) (string, *ApiToken, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", nil, err
	}
	token := hex.EncodeToString(tokenBytes)

	hash := sha256.Sum256([]byte(token))
	tokenHash := hex.EncodeToString(hash[:])

	query := `INSERT INTO api_tokens (user_id, token_hash, description, expires_at) VALUES (?, ?, ?, ?)`
	result, err := db.Exec(query, userID, tokenHash, description, expiresAt)
	if err != nil {
		return "", nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return "", nil, err
	}

	var apiToken ApiToken
	err = db.Get(&apiToken, "SELECT * FROM api_tokens WHERE id = ?", id)
	if err != nil {
		return "", nil, err
	}

	return token, &apiToken, nil
}

// ValidateApiToken checks if a token is valid and returns the user ID.
func ValidateApiToken(db *sqlx.DB, token string) (int, error) {
	hash := sha256.Sum256([]byte(token))
	tokenHash := hex.EncodeToString(hash[:])

	var tokenInfo ApiToken
	err := db.Get(&tokenInfo, "SELECT * FROM api_tokens WHERE token_hash = ?", tokenHash)
	if err != nil {
		log.Printf("ERROR: DB: Token not found in database: %v", err)
		return 0, err
	}

	if !tokenInfo.ExpiresAt.IsZero() && tokenInfo.ExpiresAt.Before(time.Now()) {
		log.Printf("ERROR: DB: Token expired for user %d", tokenInfo.UserID)
		return 0, sql.ErrNoRows
	}

	return tokenInfo.UserID, nil
}

// RevokeAllUserTokens revokes all API tokens for a user.
func RevokeAllUserTokens(db *sqlx.DB, userID int) error {
	query := `DELETE FROM api_tokens WHERE user_id = ?`
	_, err := db.Exec(query, userID)
	return err
}
