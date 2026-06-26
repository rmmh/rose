package meta

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/rmmh/rose/storage"
	"github.com/rmmh/rose/uid"
)

// bootstrapCluster ensures the singleton cluster identity row exists. The UID is
// generated once on first init and never regenerated (INSERT OR IGNORE), so it is
// a stable cluster identity across restarts.
func bootstrapCluster(db *sql.DB) error {
	u := uid.New()
	key := uid.New()
	res, err := db.Exec(
		`INSERT OR IGNORE INTO cluster (id, uid, encryption_key, encryption_alg, format_version, feature_flags, created_at)
		 VALUES (1, ?, ?, ?, ?, 0, ?)`,
		u[:], key[:], storage.VlogEncryptionAlgorithm, storage.PlogFormatVersion, time.Now().UnixNano())
	if err != nil {
		return fmt.Errorf("bootstrap cluster identity: %w", err)
	}
	if n, err := res.RowsAffected(); err == nil && n == 1 {
		fmt.Fprintf(os.Stderr, "rose cluster encryption key: %s\n", FormatClusterKey(key))
	}
	return nil
}

func FormatClusterKey(key uid.UID) string {
	s := key.String()
	return s[0:6] + "-" + s[6:12] + "-" + s[12:18] + "-" + s[18:24]
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

type ClusterEncryptionInfo struct {
	Key       uid.UID
	Alg       string
	Formatted string
}

func (d *DB) ClusterEncryption(ctx context.Context) (ClusterEncryptionInfo, error) {
	var raw []byte
	var alg string
	if err := d.db.QueryRowContext(ctx,
		"SELECT encryption_key, encryption_alg FROM cluster WHERE id = 1").
		Scan(&raw, &alg); err != nil {
		return ClusterEncryptionInfo{}, fmt.Errorf("read cluster encryption: %w", err)
	}
	key, err := uid.FromBytes(raw)
	if err != nil {
		return ClusterEncryptionInfo{}, fmt.Errorf("decode cluster encryption key: %w", err)
	}
	if alg != storage.VlogEncryptionAlgorithm {
		return ClusterEncryptionInfo{}, fmt.Errorf("unsupported cluster encryption algorithm %q", alg)
	}
	return ClusterEncryptionInfo{Key: key, Alg: alg, Formatted: FormatClusterKey(key)}, nil
}
