package meta

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rmmh/rose/storage"
	"github.com/rmmh/rose/uid"
)

// CheckStorageFiles checks one catalog snapshot and the supplied disk roots.
// Files must be quiescent for a coherent cross-file result: the SQLite read
// transaction cannot freeze external writes to physical media. This never calls
// Server.Recover, creates identity markers, truncates tails, or runs repair.
func CheckStorageFiles(ctx context.Context, path string, roots map[uint32]string) ([]ConsistencyIssue, error) {
	db, err := openInspectionCatalog(path)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	issues, err := checkCatalogTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	add := func(code, object, detail string) { issues = append(issues, ConsistencyIssue{code, object, detail}) }
	var cluster []byte
	if err := tx.QueryRowContext(ctx, "SELECT uid FROM cluster WHERE id=1").Scan(&cluster); err != nil {
		return nil, err
	}
	disks := map[uint32][]byte{}
	rows, err := tx.QueryContext(ctx, "SELECT id,uid FROM disk")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id uint32
		var u []byte
		if err := rows.Scan(&id, &u); err != nil {
			rows.Close()
			return nil, err
		}
		disks[id] = u
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for id, u := range disks {
		object := fmt.Sprintf("disk/%d", id)
		root, ok := roots[id]
		if !ok {
			add("disk_root_missing", object, "no root supplied for catalog disk")
			continue
		}
		marker, err := os.ReadFile(filepath.Join(root, "rose_disk_uid"))
		if err != nil {
			add("disk_identity", object, err.Error())
			continue
		}
		parsed, err := uid.Parse(strings.TrimSpace(string(marker)))
		if err != nil || !bytes.Equal(parsed[:], u) {
			add("disk_identity", object, "identity marker does not match catalog")
		}
	}
	type plogRow struct {
		id, disk uint32
		uid      []byte
	}
	var plogs []plogRow
	rows, err = tx.QueryContext(ctx, "SELECT id,disk_id,uid FROM plog ORDER BY id")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var p plogRow
		if err := rows.Scan(&p.id, &p.disk, &p.uid); err != nil {
			rows.Close()
			return nil, err
		}
		plogs = append(plogs, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	known := map[uint32]map[string]bool{}
	for _, p := range plogs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		object := fmt.Sprintf("plog/%d", p.id)
		root, ok := roots[p.disk]
		if !ok {
			add("plog_root_missing", object, fmt.Sprintf("no root supplied for disk %d", p.disk))
			continue
		}
		name := fmt.Sprintf("plog-%05d", p.id)
		if known[p.disk] == nil {
			known[p.disk] = map[string]bool{}
		}
		known[p.disk][name] = true
		inspection, err := storage.InspectPlog(filepath.Join(root, name), p.id)
		if err != nil {
			add("plog_unreadable", object, err.Error())
			continue
		}
		h := inspection.Header
		if h.GetPlogId() != p.id || !bytes.Equal(h.GetPlogUid(), p.uid) || !bytes.Equal(h.GetClusterUid(), cluster) || !bytes.Equal(h.GetDiskUid(), disks[p.disk]) {
			add("plog_identity", object, "superblock disagrees with catalog plog, cluster, or disk identity")
		}
		if inspection.VerificationError != nil {
			add("plog_integrity", object, inspection.VerificationError.Error())
		}
		mappings, err := tx.QueryContext(ctx, `SELECT v.id,v.length,v.protection_scheme,v.data_shards,v.dedup_domain FROM vlog_plog vp JOIN vlog v ON v.id=vp.vlog_id WHERE vp.plog_id=?`, p.id)
		if err != nil {
			return nil, err
		}
		for mappings.Next() {
			var id uint32
			var length int64
			var scheme string
			var data int
			var domain []byte
			if err := mappings.Scan(&id, &length, &scheme, &data, &domain); err != nil {
				mappings.Close()
				return nil, err
			}
			required := length
			if scheme == "EC" {
				if data <= 0 || length%int64(data) != 0 {
					add("plog_geometry", object, fmt.Sprintf("invalid EC geometry for vlog %d", id))
					continue
				}
				required /= int64(data)
			}
			if inspection.LogicalLength < required {
				add("plog_short_prefix", object, fmt.Sprintf("stored %d, vlog %d requires %d", inspection.LogicalLength, id, required))
			}
			if !bytes.Equal(h.GetDedupDomain(), domain) {
				add("plog_domain", object, fmt.Sprintf("domain differs from vlog %d", id))
			}
		}
		err = mappings.Err()
		mappings.Close()
		if err != nil {
			return nil, err
		}
	}
	for id, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			add("disk_unreadable", fmt.Sprintf("disk/%d", id), err.Error())
			continue
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "plog-") && (strings.HasSuffix(e.Name(), ".undo") || strings.HasSuffix(e.Name(), ".undo.tmp")) {
				add("plog_recovery_journal", filepath.Join(root, e.Name()), "pending or interrupted prefix journal; writable recovery is required before treating stored geometry as authoritative")
				continue
			}
			if strings.HasPrefix(e.Name(), "plog-") && !known[id][e.Name()] {
				add("uncataloged_plog", filepath.Join(root, e.Name()), "physical file has no placement in the supplied catalog/disk mapping")
			}
		}
	}
	sort.Slice(issues, func(i, j int) bool {
		a, b := issues[i], issues[j]
		if a.Code != b.Code {
			return a.Code < b.Code
		}
		if a.Object != b.Object {
			return a.Object < b.Object
		}
		return a.Detail < b.Detail
	})
	return issues, nil
}
