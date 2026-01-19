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
	"golang.org/x/crypto/bcrypt"
)

// CreateUser creates a new user with a hashed password.
func CreateUser(db *sqlx.DB, username, password string, isInitial bool) (*User, error) {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}

	query := `INSERT INTO users (username, password_hash, is_initial) VALUES (?, ?, ?)`
	result, err := db.Exec(query, username, hashedPassword, isInitial)
	if err != nil {
		return nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return GetUser(db, int(id))
}

// GetUser retrieves a user by their ID.
func GetUser(db *sqlx.DB, id int) (*User, error) {
	var user User
	err := db.Get(&user, "SELECT * FROM users WHERE id = ?", id)
	return &user, err
}

// GetUserByUsername retrieves a user by their username.
func GetUserByUsername(db *sqlx.DB, username string) (*User, error) {
	var user User
	err := db.Get(&user, "SELECT * FROM users WHERE username = ?", username)
	return &user, err
}

// GetInitialUserID returns the initial admin user ID.
func GetInitialUserID(db *sqlx.DB) (int, error) {
	var userID int
	err := db.Get(&userID, "SELECT id FROM users WHERE is_initial = 1 ORDER BY id LIMIT 1")
	return userID, err
}

// ListUsers retrieves all users from the database.
func ListUsers(db *sqlx.DB) ([]User, error) {
	var users []User
	err := db.Select(&users, "SELECT id, username, is_initial, created_at FROM users ORDER BY username")
	return users, err
}

// VerifyPassword checks if the provided password is correct for the user.
func (u *User) VerifyPassword(password string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password))
	return err == nil
}

// UpdateUser updates a user's details.
func UpdateUser(db *sqlx.DB, user *User) error {
	query := `UPDATE users SET username = ? WHERE id = ?`
	_, err := db.Exec(query, user.Username, user.ID)
	return err
}

// UpdateUserPassword updates a user's password.
func UpdateUserPassword(db *sqlx.DB, userID int, password string) error {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	query := `UPDATE users SET password_hash = ? WHERE id = ?`
	_, err = db.Exec(query, hashedPassword, userID)
	return err
}

// DeleteUser deletes a user from the database.
func DeleteUser(db *sqlx.DB, id int) error {
	query := `DELETE FROM users WHERE id = ?`
	_, err := db.Exec(query, id)
	return err
}
