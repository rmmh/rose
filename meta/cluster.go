package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rmmh/rose/storage"
	"github.com/rmmh/rose/uid"
)

// bootstrapCluster ensures the singleton cluster identity row exists. The UID is
// generated once on first init and never regenerated (INSERT OR IGNORE), so it is
// a stable cluster identity across restarts.
func bootstrapCluster(db *sql.DB) error {
	u := uid.New()
	if _, err := db.Exec(
		`INSERT OR IGNORE INTO cluster (id, uid, format_version, feature_flags, created_at)
		 VALUES (1, ?, ?, 0, ?)`,
		u[:], storage.PlogFormatVersion, time.Now().UnixNano()); err != nil {
		return fmt.Errorf("bootstrap cluster identity: %w", err)
	}
	return nil
}

// ClusterInfo returns the singleton cluster identity: its UID, on-disk format
// version, and feature flags.
func (d *DB) ClusterInfo(ctx context.Context) (clusterUID uid.UID, formatVersion uint32, flags uint64, err error) {
	var raw []byte
	err = d.db.QueryRowContext(ctx,
		"SELECT uid, format_version, feature_flags FROM cluster WHERE id = 1").
		Scan(&raw, &formatVersion, &flags)
	if err != nil {
		return uid.UID{}, 0, 0, fmt.Errorf("read cluster identity: %w", err)
	}
	clusterUID, err = uid.FromBytes(raw)
	if err != nil {
		return uid.UID{}, 0, 0, fmt.Errorf("decode cluster uid: %w", err)
	}
	return clusterUID, formatVersion, flags, nil
}
