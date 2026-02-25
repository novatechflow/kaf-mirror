package database

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

func CreateInventorySnapshot(db *sqlx.DB, jobID, snapshotType string) (int, error) {
	query := `
		INSERT INTO job_inventory_snapshots (job_id, snapshot_type)
		VALUES (?, ?)`

	result, err := db.Exec(query, jobID, snapshotType)
	if err != nil {
		return 0, fmt.Errorf("failed to create inventory snapshot: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get snapshot ID: %w", err)
	}

	return int(id), nil
}

func InsertClusterInventory(db *sqlx.DB, inventory ClusterInventory) (int, error) {
	query := `
		INSERT INTO cluster_inventory 
		(snapshot_id, cluster_type, cluster_name, provider, brokers, 
		 broker_count, total_topics, controller_id, cluster_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := db.Exec(query,
		inventory.SnapshotID, inventory.ClusterType, inventory.ClusterName,
		inventory.Provider, inventory.Brokers, inventory.BrokerCount,
		inventory.TotalTopics, inventory.ControllerID, inventory.ClusterID)
	if err != nil {
		return 0, fmt.Errorf("failed to insert cluster inventory: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get cluster inventory ID: %w", err)
	}

	return int(id), nil
}

func InsertTopicInventory(db *sqlx.DB, inventory TopicInventory) (int, error) {
	query := `
		INSERT INTO topic_inventory 
		(cluster_inventory_id, topic_name, partition_count, replication_factor, 
		 is_internal, config_data)
		VALUES (?, ?, ?, ?, ?, ?)`

	result, err := db.Exec(query,
		inventory.ClusterInventoryID, inventory.TopicName,
		inventory.PartitionCount, inventory.ReplicationFactor,
		inventory.IsInternal, inventory.ConfigData)
	if err != nil {
		return 0, fmt.Errorf("failed to insert topic inventory: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get topic inventory ID: %w", err)
	}

	return int(id), nil
}

func InsertPartitionInventory(db *sqlx.DB, inventory PartitionInventory) error {
	query := `
		INSERT INTO partition_inventory 
		(topic_inventory_id, partition_id, leader_id, replica_ids, isr_ids, high_water_mark)
		VALUES (?, ?, ?, ?, ?, ?)`

	_, err := db.Exec(query,
		inventory.TopicInventoryID, inventory.PartitionID,
		inventory.LeaderID, inventory.ReplicaIDs, inventory.IsrIDs,
		inventory.HighWaterMark)
	if err != nil {
		return fmt.Errorf("failed to insert partition inventory: %w", err)
	}

	return nil
}

func InsertConsumerGroupInventory(db *sqlx.DB, inventory ConsumerGroupInventory) (int, error) {
	query := `
		INSERT INTO consumer_group_inventory 
		(snapshot_id, group_id, group_state, protocol_type, protocol, member_count)
		VALUES (?, ?, ?, ?, ?, ?)`

	result, err := db.Exec(query,
		inventory.SnapshotID, inventory.GroupID, inventory.GroupState,
		inventory.ProtocolType, inventory.Protocol, inventory.MemberCount)
	if err != nil {
		return 0, fmt.Errorf("failed to insert consumer group inventory: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to get consumer group inventory ID: %w", err)
	}

	return int(id), nil
}

func InsertConsumerGroupOffset(db *sqlx.DB, offset ConsumerGroupOffset) error {
	query := `
		INSERT INTO consumer_group_offsets 
		(consumer_group_inventory_id, topic_name, partition_id, 
		 current_offset, high_water_mark, lag)
		VALUES (?, ?, ?, ?, ?, ?)`

	_, err := db.Exec(query,
		offset.ConsumerGroupInventoryID, offset.TopicName,
		offset.PartitionID, offset.CurrentOffset,
		offset.HighWaterMark, offset.Lag)
	if err != nil {
		return fmt.Errorf("failed to insert consumer group offset: %w", err)
	}

	return nil
}

func InsertConnectionInventory(db *sqlx.DB, inventory ConnectionInventory) error {
	query := `
		INSERT INTO connection_inventory 
		(snapshot_id, connection_type, provider, brokers, security_protocol, 
		 sasl_mechanism, api_key_prefix, connection_successful, connection_time_ms, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := db.Exec(query,
		inventory.SnapshotID, inventory.ConnectionType, inventory.Provider,
		inventory.Brokers, inventory.SecurityProtocol, inventory.SaslMechanism,
		inventory.APIKeyPrefix, inventory.ConnectionSuccessful,
		inventory.ConnectionTimeMs, inventory.ErrorMessage)
	if err != nil {
		return fmt.Errorf("failed to insert connection inventory: %w", err)
	}

	return nil
}

func GetInventorySnapshots(db *sqlx.DB, jobID string) ([]JobInventorySnapshot, error) {
	query := `
		SELECT id, job_id, snapshot_type, created_at
		FROM job_inventory_snapshots
		WHERE job_id = ?
		ORDER BY created_at DESC`

	var snapshots []JobInventorySnapshot
	err := db.Select(&snapshots, query, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to query inventory snapshots: %w", err)
	}

	return snapshots, nil
}

func GetFullInventoryData(db *sqlx.DB, snapshotID int) (*InventoryData, error) {
	snapshot, err := getSnapshotByID(db, snapshotID)
	if err != nil {
		return nil, err
	}

	clusters, err := getClusterInventories(db, snapshotID)
	if err != nil {
		return nil, err
	}

	consumerGroups, err := getConsumerGroupInventories(db, snapshotID)
	if err != nil {
		return nil, err
	}

	connections, err := getConnectionInventories(db, snapshotID)
	if err != nil {
		return nil, err
	}

	data := &InventoryData{
		Snapshot:       *snapshot,
		ConsumerGroups: consumerGroups,
		Connections:    connections,
	}

	for _, cluster := range clusters {
		topics, err := getTopicInventories(db, cluster.ID)
		if err != nil {
			return nil, err
		}

		var partitions []PartitionInventory
		for _, topic := range topics {
			topicPartitions, err := getPartitionInventories(db, topic.ID)
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, topicPartitions...)
		}

		if cluster.ClusterType == "source" {
			data.SourceCluster = &cluster
			data.SourceTopics = topics
			data.SourcePartitions = partitions
		} else {
			data.TargetCluster = &cluster
			data.TargetTopics = topics
			data.TargetPartitions = partitions
		}
	}

	offsets, err := getConsumerGroupOffsets(db, consumerGroups)
	if err != nil {
		return nil, err
	}
	data.ConsumerOffsets = offsets

	return data, nil
}

func getSnapshotByID(db *sqlx.DB, snapshotID int) (*JobInventorySnapshot, error) {
	query := `
		SELECT id, job_id, snapshot_type, created_at
		FROM job_inventory_snapshots
		WHERE id = ?`

	var snapshot JobInventorySnapshot
	err := db.Get(&snapshot, query, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to get snapshot: %w", err)
	}

	return &snapshot, nil
}

func getClusterInventories(db *sqlx.DB, snapshotID int) ([]ClusterInventory, error) {
	query := `
		SELECT id, snapshot_id, cluster_type, cluster_name, provider, brokers,
		       broker_count, total_topics, controller_id, cluster_id
		FROM cluster_inventory
		WHERE snapshot_id = ?
		ORDER BY cluster_type`

	var clusters []ClusterInventory
	err := db.Select(&clusters, query, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to query cluster inventories: %w", err)
	}

	return clusters, nil
}

func getTopicInventories(db *sqlx.DB, clusterInventoryID int) ([]TopicInventory, error) {
	query := `
		SELECT id, cluster_inventory_id, topic_name, partition_count,
		       replication_factor, is_internal, config_data
		FROM topic_inventory
		WHERE cluster_inventory_id = ?
		ORDER BY topic_name`

	var topics []TopicInventory
	err := db.Select(&topics, query, clusterInventoryID)
	if err != nil {
		return nil, fmt.Errorf("failed to query topic inventories: %w", err)
	}

	return topics, nil
}

func getPartitionInventories(db *sqlx.DB, topicInventoryID int) ([]PartitionInventory, error) {
	query := `
		SELECT id, topic_inventory_id, partition_id, leader_id,
		       replica_ids, isr_ids, high_water_mark
		FROM partition_inventory
		WHERE topic_inventory_id = ?
		ORDER BY partition_id`

	var partitions []PartitionInventory
	err := db.Select(&partitions, query, topicInventoryID)
	if err != nil {
		return nil, fmt.Errorf("failed to query partition inventories: %w", err)
	}

	return partitions, nil
}

func getConsumerGroupInventories(db *sqlx.DB, snapshotID int) ([]ConsumerGroupInventory, error) {
	query := `
		SELECT id, snapshot_id, group_id, group_state, protocol_type, protocol, member_count
		FROM consumer_group_inventory
		WHERE snapshot_id = ?
		ORDER BY group_id`

	var groups []ConsumerGroupInventory
	err := db.Select(&groups, query, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to query consumer group inventories: %w", err)
	}

	return groups, nil
}

func getConsumerGroupOffsets(db *sqlx.DB, groups []ConsumerGroupInventory) ([]ConsumerGroupOffset, error) {
	var allOffsets []ConsumerGroupOffset

	for _, group := range groups {
		query := `
			SELECT id, consumer_group_inventory_id, topic_name, partition_id,
			       current_offset, high_water_mark, lag
			FROM consumer_group_offsets
			WHERE consumer_group_inventory_id = ?
			ORDER BY topic_name, partition_id`

		var offsets []ConsumerGroupOffset
		err := db.Select(&offsets, query, group.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to query consumer group offsets: %w", err)
		}
		allOffsets = append(allOffsets, offsets...)
	}

	return allOffsets, nil
}

func getConnectionInventories(db *sqlx.DB, snapshotID int) ([]ConnectionInventory, error) {
	query := `
		SELECT id, snapshot_id, connection_type, provider, brokers,
		       security_protocol, sasl_mechanism, api_key_prefix,
		       connection_successful, connection_time_ms, error_message
		FROM connection_inventory
		WHERE snapshot_id = ?
		ORDER BY connection_type`

	var connections []ConnectionInventory
	err := db.Select(&connections, query, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to query connection inventories: %w", err)
	}

	return connections, nil
}

func PruneOldInventorySnapshots(db *sqlx.DB) error {
	cutoff := time.Now().AddDate(0, 0, -30)

	query := `DELETE FROM job_inventory_snapshots WHERE created_at < ?`
	_, err := db.Exec(query, cutoff)
	if err != nil {
		return fmt.Errorf("failed to prune old inventory snapshots: %w", err)
	}

	return nil
}

func maskAPIKey(apiKey string) string {
	if len(apiKey) <= 4 {
		return "****"
	}
	return apiKey[:4] + "****"
}

func intSliceToJSON(slice []int) string {
	if len(slice) == 0 {
		return "[]"
	}
	data, _ := json.Marshal(slice)
	return string(data)
}
