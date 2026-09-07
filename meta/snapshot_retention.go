package meta

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SnapshotInfo uses Unix nanoseconds for CreatedAt. IDs monotonically increase;
// deleting a name permits a new snapshot with that name and a new ID.
type SnapshotInfo struct {
	ID        uint64
	Name      string
	CreatedAt int64
}

type SnapshotRetention struct {
	Continuous time.Duration
	Daily      time.Duration
	Weekly     time.Duration
}

func listSnapshots(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) ([]SnapshotInfo, error) {
	rows, err := q.QueryContext(ctx, "SELECT id,name,created_at FROM snapshot ORDER BY created_at DESC,id DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotInfo
	for rows.Next() {
		var s SnapshotInfo
		if err := rows.Scan(&s.ID, &s.Name, &s.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
func (d *DB) ListSnapshots(ctx context.Context) ([]SnapshotInfo, error) {
	return listSnapshots(ctx, d.db)
}

// SetSnapshotRetention persists the policy. Nil disables automatic expiration;
// explicit snapshots are retained indefinitely until a policy is configured or
// they are explicitly deleted. Zero windows intentionally expire all past entries.
func (d *DB) SetSnapshotRetention(ctx context.Context, p *SnapshotRetention) error {
	if p == nil {
		_, err := d.db.ExecContext(ctx, "DELETE FROM snapshot_retention WHERE id=1")
		return err
	}
	if p.Continuous < 0 || p.Daily < p.Continuous || p.Weekly < p.Daily {
		return fmt.Errorf("snapshot retention requires 0 <= continuous <= daily <= weekly")
	}
	_, err := d.db.ExecContext(ctx, `INSERT INTO snapshot_retention(id,continuous_ns,daily_ns,weekly_ns) VALUES(1,?,?,?)
 ON CONFLICT(id) DO UPDATE SET continuous_ns=excluded.continuous_ns,daily_ns=excluded.daily_ns,weekly_ns=excluded.weekly_ns`, int64(p.Continuous), int64(p.Daily), int64(p.Weekly))
	return err
}

// ExpireSnapshots selects and releases roots in a single catalog transaction.
// Newest-per-bucket includes representatives retained by a younger tier. Buckets
// are fixed 24-hour and 7-day UTC intervals anchored at the Unix epoch; ties use
// the greater snapshot ID. Future-dated entries survive backward clock steps.
func (d *DB) ExpireSnapshots(ctx context.Context, now time.Time) ([]uint64, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var p SnapshotRetention
	err = tx.QueryRowContext(ctx, "SELECT continuous_ns,daily_ns,weekly_ns FROM snapshot_retention WHERE id=1").Scan(&p.Continuous, &p.Daily, &p.Weekly)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	snapshots, err := listSnapshots(ctx, tx)
	if err != nil {
		return nil, err
	}
	days, weeks := map[int64]bool{}, map[int64]bool{}
	var expired []uint64
	bucket := func(n, width int64) int64 {
		q := n / width
		if n < 0 && n%width != 0 {
			q--
		}
		return q
	}
	for _, s := range snapshots {
		created := time.Unix(0, s.CreatedAt)
		if created.After(now) {
			continue
		}
		age := now.Sub(created)
		day, week := bucket(s.CreatedAt, int64(24*time.Hour)), bucket(s.CreatedAt, int64(7*24*time.Hour))
		keep := age <= p.Continuous || (age <= p.Daily && !days[day]) || (age <= p.Weekly && !weeks[week])
		days[day], weeks[week] = true, true
		if keep {
			continue
		}
		if err := deleteSnapshotTx(ctx, tx, s.ID); err != nil {
			return nil, err
		}
		expired = append(expired, s.ID)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return expired, nil
}
