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
	"encoding/json"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func UpdateMirrorProgress(db *sqlx.DB, progress MirrorProgress) error {
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// First, create a historical snapshot by inserting into a historical table
	// We'll create entries without the unique constraint to preserve history
	historyQuery := `
		INSERT INTO mirror_progress_history 
		(job_id, source_topic, target_topic, partition_id, source_offset, target_offset, 
		 source_high_water_mark, target_high_water_mark, last_replicated_offset, 
		 replication_lag, last_updated, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err = tx.Exec(historyQuery, progress.JobID, progress.SourceTopic, progress.TargetTopic,
		progress.PartitionID, progress.SourceOffset, progress.TargetOffset,
		progress.SourceHighWaterMark, progress.TargetHighWaterMark, progress.LastReplicatedOffset,
		progress.ReplicationLag, progress.LastUpdated, progress.Status)
	if err != nil {
		// If history table doesn't exist, we'll continue with the current table update
		// This provides backward compatibility
	}

	// Then update the current state table (with unique constraint)
	currentQuery := `
		INSERT INTO mirror_progress 
		(job_id, source_topic, target_topic, partition_id, source_offset, target_offset, 
		 source_high_water_mark, target_high_water_mark, last_replicated_offset, 
		 replication_lag, last_updated, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id, source_topic, partition_id) DO UPDATE SET
		    target_topic = excluded.target_topic,
		    source_offset = excluded.source_offset,
		    target_offset = excluded.target_offset,
		    source_high_water_mark = excluded.source_high_water_mark,
		    target_high_water_mark = excluded.target_high_water_mark,
		    last_replicated_offset = excluded.last_replicated_offset,
		    replication_lag = excluded.replication_lag,
		    last_updated = excluded.last_updated,
		    status = excluded.status`

	_, err = tx.Exec(currentQuery, progress.JobID, progress.SourceTopic, progress.TargetTopic,
		progress.PartitionID, progress.SourceOffset, progress.TargetOffset,
		progress.SourceHighWaterMark, progress.TargetHighWaterMark, progress.LastReplicatedOffset,
		progress.ReplicationLag, progress.LastUpdated, progress.Status)
	if err != nil {
		return fmt.Errorf("failed to update current mirror progress: %w", err)
	}

	return tx.Commit()
}

func GetMirrorProgress(db *sqlx.DB, jobID string) ([]MirrorProgress, error) {
	query := `SELECT * FROM mirror_progress WHERE job_id = ? ORDER BY source_topic, partition_id`

	progress := make([]MirrorProgress, 0)
	err := db.Select(&progress, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get mirror progress: %w", err)
	}
	return progress, nil
}

func GetMirrorProgressByTopic(db *sqlx.DB, jobID, sourceTopic string) ([]MirrorProgress, error) {
	query := `SELECT * FROM mirror_progress WHERE job_id = ? AND source_topic = ? ORDER BY partition_id`

	var progress []MirrorProgress
	err := db.Select(&progress, query, jobID, sourceTopic)
	if err != nil {
		return nil, fmt.Errorf("failed to get mirror progress for topic: %w", err)
	}
	return progress, nil
}

func CreateMigrationCheckpoint(db *sqlx.DB, checkpoint MigrationCheckpoint) (int, error) {
	query := `
		INSERT INTO migration_checkpoints 
		(job_id, checkpoint_type, source_consumer_group_offsets, target_high_water_marks, 
		 created_at, created_by, migration_reason, validation_results)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?) 
		RETURNING id`

	var id int
	err := db.QueryRow(query, checkpoint.JobID, checkpoint.CheckpointType,
		checkpoint.SourceConsumerGroupOffsets, checkpoint.TargetHighWaterMarks,
		checkpoint.CreatedAt, checkpoint.CreatedBy, checkpoint.MigrationReason,
		checkpoint.ValidationResults).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("failed to create migration checkpoint: %w", err)
	}
	return id, nil
}

func GetLatestMigrationCheckpoint(db *sqlx.DB, jobID string) (*MigrationCheckpoint, error) {
	query := `SELECT * FROM migration_checkpoints WHERE job_id = ? ORDER BY created_at DESC LIMIT 1`

	var checkpoint MigrationCheckpoint
	err := db.Get(&checkpoint, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest migration checkpoint: %w", err)
	}
	return &checkpoint, nil
}

func CalculateResumePoints(db *sqlx.DB, jobID string, resumePointsData map[string]map[int32]int64, checkpointID *int) error {
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Clear existing resume points for this calculation
	_, err = tx.Exec("DELETE FROM resume_points WHERE job_id = ? AND calculated_at > datetime('now', '-1 hour')", jobID)
	if err != nil {
		return fmt.Errorf("failed to clear old resume points: %w", err)
	}

	// Insert new resume points
	insertQuery := `
		INSERT INTO resume_points 
		(job_id, source_topic, target_topic, partition_id, safe_resume_offset, 
		 calculated_at, validation_status, migration_checkpoint_id, gap_detected)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	for sourceTopic, partitions := range resumePointsData {
		// Get corresponding target topic from topic mappings
		var targetTopic string
		err = tx.Get(&targetTopic,
			"SELECT target_topic_pattern FROM topic_mappings WHERE job_id = ? AND source_topic_pattern = ? LIMIT 1",
			jobID, sourceTopic)
		if err != nil {
			targetTopic = sourceTopic // Fallback to same name
		}

		for partitionID, resumeOffset := range partitions {
			gapDetected := resumeOffset > 0

			_, err = tx.Exec(insertQuery, jobID, sourceTopic, targetTopic, partitionID,
				resumeOffset, time.Now(), "pending", checkpointID, gapDetected)
			if err != nil {
				return fmt.Errorf("failed to insert resume point: %w", err)
			}
		}
	}

	return tx.Commit()
}

func GetResumePoints(db *sqlx.DB, jobID string) ([]ResumePoint, error) {
	query := `
		SELECT * FROM resume_points 
		WHERE job_id = ? 
		ORDER BY calculated_at DESC, source_topic, partition_id`

	resumePoints := make([]ResumePoint, 0)
	err := db.Select(&resumePoints, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get resume points: %w", err)
	}
	return resumePoints, nil
}

func GetLatestResumePoints(db *sqlx.DB, jobID string) (map[string]map[int32]int64, error) {
	query := `
		SELECT source_topic, partition_id, safe_resume_offset 
		FROM resume_points 
		WHERE job_id = ? 
		AND calculated_at = (
			SELECT MAX(calculated_at) 
			FROM resume_points 
			WHERE job_id = ?
		)
		ORDER BY source_topic, partition_id`

	rows, err := db.Query(query, jobID, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest resume points: %w", err)
	}
	defer rows.Close()

	resumePoints := make(map[string]map[int32]int64)
	for rows.Next() {
		var sourceTopic string
		var partitionID int32
		var resumeOffset int64

		err = rows.Scan(&sourceTopic, &partitionID, &resumeOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to scan resume point: %w", err)
		}

		if resumePoints[sourceTopic] == nil {
			resumePoints[sourceTopic] = make(map[int32]int64)
		}
		resumePoints[sourceTopic][partitionID] = resumeOffset
	}

	return resumePoints, nil
}

func StoreMirrorStateAnalysis(db *sqlx.DB, analysis MirrorStateAnalysis) (int, error) {
	query := `
		INSERT INTO mirror_state_analysis 
		(job_id, analysis_type, source_cluster_state, target_cluster_state, 
		 analysis_results, recommendations, critical_issues_count, warning_issues_count, 
		 analyzed_at, analyzer_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
		RETURNING id`

	recommendationsJSON, _ := json.Marshal(analysis.Recommendations)

	var id int
	err := db.QueryRow(query, analysis.JobID, analysis.AnalysisType,
		analysis.SourceClusterState, analysis.TargetClusterState,
		analysis.AnalysisResults, string(recommendationsJSON),
		analysis.CriticalIssuesCount, analysis.WarningIssuesCount,
		analysis.AnalyzedAt, analysis.AnalyzerVersion).Scan(&id)

	if err != nil {
		return 0, fmt.Errorf("failed to store mirror state analysis: %w", err)
	}
	return id, nil
}

func GetMirrorStateAnalysis(db *sqlx.DB, jobID string) ([]MirrorStateAnalysis, error) {
	query := `SELECT * FROM mirror_state_analysis WHERE job_id = ? ORDER BY analyzed_at DESC`

	analyses := make([]MirrorStateAnalysis, 0)
	err := db.Select(&analyses, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get mirror state analysis: %w", err)
	}
	return analyses, nil
}

func DetectMirrorGaps(db *sqlx.DB, jobID string, gaps []MirrorGap) error {
	if len(gaps) == 0 {
		return nil
	}

	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	insertQuery := `
		INSERT INTO mirror_gaps 
		(job_id, source_topic, target_topic, partition_id, gap_start_offset, 
		 gap_end_offset, gap_size, detected_at, gap_type, resolution_status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	for _, gap := range gaps {
		_, err = tx.Exec(insertQuery, gap.JobID, gap.SourceTopic, gap.TargetTopic,
			gap.PartitionID, gap.GapStartOffset, gap.GapEndOffset, gap.GapSize,
			gap.DetectedAt, gap.GapType, gap.ResolutionStatus)
		if err != nil {
			return fmt.Errorf("failed to insert mirror gap: %w", err)
		}
	}

	return tx.Commit()
}

func GetMirrorGaps(db *sqlx.DB, jobID string) ([]MirrorGap, error) {
	query := `SELECT * FROM mirror_gaps WHERE job_id = ? ORDER BY detected_at DESC`

	gaps := make([]MirrorGap, 0)
	err := db.Select(&gaps, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get mirror gaps: %w", err)
	}
	return gaps, nil
}

func GetUnresolvedMirrorGaps(db *sqlx.DB, jobID string) ([]MirrorGap, error) {
	query := `SELECT * FROM mirror_gaps WHERE job_id = ? AND resolution_status = 'unresolved' ORDER BY detected_at DESC`

	var gaps []MirrorGap
	err := db.Select(&gaps, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get unresolved mirror gaps: %w", err)
	}
	return gaps, nil
}

func ResolveMirrorGap(db *sqlx.DB, gapID int, resolutionMethod string) error {
	query := `
		UPDATE mirror_gaps 
		SET resolution_status = 'resolved', 
		    resolution_method = ?, 
		    resolved_at = CURRENT_TIMESTAMP 
		WHERE id = ?`

	_, err := db.Exec(query, resolutionMethod, gapID)
	if err != nil {
		return fmt.Errorf("failed to resolve mirror gap: %w", err)
	}
	return nil
}

func GetMirrorStateData(db *sqlx.DB, jobID string) (*MirrorStateData, error) {
	data := &MirrorStateData{
		JobID:          jobID,
		MirrorProgress: make([]MirrorProgress, 0),
		ResumePoints:   make([]ResumePoint, 0),
		MirrorGaps:     make([]MirrorGap, 0),
		StateAnalysis:  make([]MirrorStateAnalysis, 0),
	}

	// Get only the latest mirror progress entries (one per topic/partition combination)
	progressQuery := `
		SELECT * FROM mirror_progress 
		WHERE job_id = ? 
		AND (source_topic, partition_id, last_updated) IN (
			SELECT source_topic, partition_id, MAX(last_updated) 
			FROM mirror_progress 
			WHERE job_id = ? 
			GROUP BY source_topic, partition_id
		)
		ORDER BY source_topic, partition_id`
	err := db.Select(&data.MirrorProgress, progressQuery, jobID, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest mirror progress: %w", err)
	}

	// Get only the latest resume points (one per topic/partition combination)
	resumeQuery := `
		SELECT * FROM resume_points 
		WHERE job_id = ? 
		AND (source_topic, partition_id, calculated_at) IN (
			SELECT source_topic, partition_id, MAX(calculated_at) 
			FROM resume_points 
			WHERE job_id = ? 
			GROUP BY source_topic, partition_id
		)
		ORDER BY source_topic, partition_id`
	err = db.Select(&data.ResumePoints, resumeQuery, jobID, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest resume points: %w", err)
	}

	// Get only unresolved mirror gaps (latest detection per topic/partition)
	gapsQuery := `
		SELECT * FROM mirror_gaps 
		WHERE job_id = ? 
		AND resolution_status = 'unresolved'
		AND (source_topic, partition_id, detected_at) IN (
			SELECT source_topic, partition_id, MAX(detected_at) 
			FROM mirror_gaps 
			WHERE job_id = ? AND resolution_status = 'unresolved'
			GROUP BY source_topic, partition_id
		)
		ORDER BY source_topic, partition_id`
	err = db.Select(&data.MirrorGaps, gapsQuery, jobID, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest mirror gaps: %w", err)
	}

	// Get only the latest state analysis
	analyses, err := GetMirrorStateAnalysis(db, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get state analysis: %w", err)
	}
	if len(analyses) > 0 {
		data.StateAnalysis = []MirrorStateAnalysis{analyses[0]} // analyses are ordered by analyzed_at DESC
	} else {
		data.StateAnalysis = make([]MirrorStateAnalysis, 0)
	}

	checkpoint, err := GetLatestMigrationCheckpoint(db, jobID)
	if err == nil {
		data.LastCheckpoint = checkpoint
	}

	return data, nil
}

func GetMirrorStateDataForPeriod(db *sqlx.DB, jobID string, startTime, endTime time.Time) (*MirrorStateData, error) {
	data := &MirrorStateData{
		JobID:          jobID,
		MirrorProgress: make([]MirrorProgress, 0),
		ResumePoints:   make([]ResumePoint, 0),
		MirrorGaps:     make([]MirrorGap, 0),
		StateAnalysis:  make([]MirrorStateAnalysis, 0),
	}

	// Get mirror progress within time range
	progressQuery := `SELECT * FROM mirror_progress WHERE job_id = ? AND last_updated BETWEEN ? AND ? ORDER BY source_topic, partition_id`
	err := db.Select(&data.MirrorProgress, progressQuery, jobID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get mirror progress for period: %w", err)
	}

	// Get resume points within time range
	resumeQuery := `SELECT * FROM resume_points WHERE job_id = ? AND calculated_at BETWEEN ? AND ? ORDER BY calculated_at DESC, source_topic, partition_id`
	err = db.Select(&data.ResumePoints, resumeQuery, jobID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get resume points for period: %w", err)
	}

	// Get mirror gaps within time range
	gapsQuery := `SELECT * FROM mirror_gaps WHERE job_id = ? AND detected_at BETWEEN ? AND ? ORDER BY detected_at DESC`
	err = db.Select(&data.MirrorGaps, gapsQuery, jobID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get mirror gaps for period: %w", err)
	}

	// Get state analysis within time range (latest one within the period)
	analysisQuery := `SELECT * FROM mirror_state_analysis WHERE job_id = ? AND analyzed_at BETWEEN ? AND ? ORDER BY analyzed_at DESC LIMIT 1`
	var analyses []MirrorStateAnalysis
	err = db.Select(&analyses, analysisQuery, jobID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get state analysis for period: %w", err)
	}
	if len(analyses) > 0 {
		data.StateAnalysis = []MirrorStateAnalysis{analyses[0]}
	} else {
		data.StateAnalysis = make([]MirrorStateAnalysis, 0)
	}

	// Get latest migration checkpoint within time range
	checkpointQuery := `SELECT * FROM migration_checkpoints WHERE job_id = ? AND created_at BETWEEN ? AND ? ORDER BY created_at DESC LIMIT 1`
	var checkpoint MigrationCheckpoint
	err = db.Get(&checkpoint, checkpointQuery, jobID, startTime, endTime)
	if err == nil {
		data.LastCheckpoint = &checkpoint
	}

	return data, nil
}

func ValidateResumePointsIntegrity(db *sqlx.DB, jobID string) error {
	query := `
		UPDATE resume_points 
		SET validation_status = 'validated' 
		WHERE job_id = ? AND validation_status = 'pending'`

	_, err := db.Exec(query, jobID)
	if err != nil {
		return fmt.Errorf("failed to validate resume points: %w", err)
	}
	return nil
}

func CleanupOldMirrorData(db *sqlx.DB, retentionDays int) error {
	cutoffDate := time.Now().AddDate(0, 0, -retentionDays)

	queries := []string{
		"DELETE FROM mirror_gaps WHERE detected_at < ? AND resolution_status = 'resolved'",
		"DELETE FROM resume_points WHERE calculated_at < ?",
		"DELETE FROM mirror_state_analysis WHERE analyzed_at < ?",
		"DELETE FROM migration_checkpoints WHERE created_at < ?",
	}

	for _, query := range queries {
		_, err := db.Exec(query, cutoffDate)
		if err != nil {
			return fmt.Errorf("failed to cleanup old mirror data: %w", err)
		}
	}

	return nil
}
