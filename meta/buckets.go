package meta

import (
	"context"
	"database/sql"
	"fmt"
)

// BucketPolicy is the protection scheme a bucket's files are written under. It
// is the durable per-bucket generalization of the single fixed scheme the file
// path used to hardcode: a bucket names a top-level path component, and every
// vlog provisioned for that bucket inherits its scheme and shard counts. The
// RoseStorage placement/commit/read predicates already key off a vlog's scheme,
// so making the scheme selectable per bucket changes which vlog a write lands in,
// not how durability is gated.
type BucketPolicy struct {
	Name             string
	ProtectionScheme string // "DUPLICATE", "EC", or "NONE"
	DataShards       int
	ParityShards     int
}

// DefaultBucketPolicy is the scheme an unconfigured bucket falls back to: a
// mirror across every configured disk, matching the behavior the file path had
// before per-bucket policies existed.
func DefaultBucketPolicy(name string) BucketPolicy {
	return BucketPolicy{Name: name, ProtectionScheme: "DUPLICATE", DataShards: 1, ParityShards: 0}
}

// SetBucketPolicy records (or replaces) the protection policy for a bucket.
func (d *DB) SetBucketPolicy(ctx context.Context, p BucketPolicy) error {
	_, err := d.db.ExecContext(ctx,
		`INSERT INTO bucket (name, protection_scheme, data_shards, parity_shards)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET
		   protection_scheme = excluded.protection_scheme,
		   data_shards = excluded.data_shards,
		   parity_shards = excluded.parity_shards`,
		p.Name, p.ProtectionScheme, p.DataShards, p.ParityShards)
	if err != nil {
		return fmt.Errorf("set bucket %q policy: %w", p.Name, err)
	}
	return nil
}

// GetBucketPolicy returns a bucket's configured policy. The boolean is false
// when the bucket has no explicit policy; callers fall back to
// DefaultBucketPolicy in that case.
func (d *DB) GetBucketPolicy(ctx context.Context, name string) (BucketPolicy, bool, error) {
	var p BucketPolicy
	p.Name = name
	err := d.db.QueryRowContext(ctx,
		`SELECT protection_scheme, data_shards, parity_shards FROM bucket WHERE name = ?`, name).
		Scan(&p.ProtectionScheme, &p.DataShards, &p.ParityShards)
	if err == sql.ErrNoRows {
		return BucketPolicy{}, false, nil
	}
	if err != nil {
		return BucketPolicy{}, false, fmt.Errorf("get bucket %q policy: %w", name, err)
	}
	return p, true, nil
}

// ListBucketPolicies returns every configured bucket policy ordered by name, so
// a restarting server can warm its in-memory cache in one scan.
func (d *DB) ListBucketPolicies(ctx context.Context) ([]BucketPolicy, error) {
	rows, err := d.db.QueryContext(ctx,
		`SELECT name, protection_scheme, data_shards, parity_shards FROM bucket ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BucketPolicy
	for rows.Next() {
		var p BucketPolicy
		if err := rows.Scan(&p.Name, &p.ProtectionScheme, &p.DataShards, &p.ParityShards); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
