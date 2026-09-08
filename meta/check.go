package meta

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net/url"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rmmh/rose/storage"
)

type ConsistencyIssue struct {
	Code   string `json:"code"`
	Object string `json:"object"`
	Detail string `json:"detail"`
}

// CheckCatalogFile opens an existing database in SQLite read-only mode. It never
// initializes schemas, backfills namespace rows, resumes jobs, or repairs data.
func CheckCatalogFile(ctx context.Context, path string) ([]ConsistencyIssue, error) {
	db, err := openInspectionCatalog(path)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return (&DB{db: db}).CheckCatalog(ctx)
}

func openInspectionCatalog(path string) (*sql.DB, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: absolute, RawQuery: "mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(10000)"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// CheckCatalog independently decodes ordered extent occurrences and traverses
// namespace, snapshot, and retry-result roots in one read transaction. It does
// not use chunkHashes, adjustChunkRefs, or the namespace backfill code being checked. This covers
// catalog structure; physical disk contents and volatile owners require the
// server-level portion of the consistency checker.
func (d *DB) CheckCatalog(ctx context.Context) ([]ConsistencyIssue, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return checkCatalogTx(ctx, tx)
}

func checkCatalogTx(ctx context.Context, tx *sql.Tx) ([]ConsistencyIssue, error) {
	var issues []ConsistencyIssue
	issue := func(code, object, detail string) { issues = append(issues, ConsistencyIssue{code, object, detail}) }
	scan := func(query string, visit func(*sql.Rows) error) error {
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if err := visit(rows); err != nil {
				return err
			}
		}
		return rows.Err()
	}
	type extent struct {
		hash   string
		length uint32
	}
	files := map[int64][]extent{}
	if err := scan("SELECT id,chunks FROM file", func(r *sql.Rows) error {
		var id int64
		var blob []byte
		if err := r.Scan(&id, &blob); err != nil {
			return err
		}
		object := fmt.Sprintf("file/%d", id)
		if len(blob)%19 != 0 {
			issue("malformed_extents", object, fmt.Sprintf("blob length %d is not divisible by 19", len(blob)))
		}
		list := []extent{}
		for off := 0; off+19 <= len(blob); off += 19 {
			n := binary.LittleEndian.Uint32(blob[off+15 : off+19])
			if n == 0 {
				issue("empty_extent", object, fmt.Sprintf("extent %d has zero length", off/19))
			}
			list = append(list, extent{string(blob[off : off+15]), n})
		}
		files[id] = list
		return nil
	}); err != nil {
		return nil, err
	}
	expected := map[string]int64{}
	lengths := map[string]uint32{}
	roots := `SELECT 'head:'||path,file_id FROM file_head UNION ALL SELECT 'snapshot:'||snapshot_id||':'||path,file_id FROM snapshot_file UNION ALL SELECT 'retry:'||write_op_id,file_id FROM write_result_root`
	if err := scan(roots, func(r *sql.Rows) error {
		var name string
		var id int64
		if err := r.Scan(&name, &id); err != nil {
			return err
		}
		extents, ok := files[id]
		if !ok {
			issue("missing_file", name, fmt.Sprintf("file %d is absent", id))
			return nil
		}
		for _, e := range extents {
			expected[e.hash]++
			if n, exists := lengths[e.hash]; exists && n != e.length {
				issue("extent_length_conflict", name, fmt.Sprintf("chunk %x has lengths %d and %d", e.hash, n, e.length))
			}
			lengths[e.hash] = e.length
		}
		return nil
	}); err != nil {
		return nil, err
	}
	type vlog struct {
		length   int64
		required int
		scoped   bool
	}
	vlogs := map[int64]vlog{}
	if err := scan("SELECT id,length,required_shards,length(dedup_domain),protection_scheme,data_shards,parity_shards,target_data_shards,target_parity_shards FROM vlog", func(r *sql.Rows) error {
		var id int64
		var v vlog
		var domain int
		var scheme string
		var data, parity, targetData, targetParity int64
		if err := r.Scan(&id, &v.length, &v.required, &domain, &scheme, &data, &parity, &targetData, &targetParity); err != nil {
			return err
		}
		v.scoped = domain != 0
		vlogs[id] = v
		if domain != 0 && domain != 32 {
			issue("invalid_domain", fmt.Sprintf("vlog/%d", id), fmt.Sprintf("domain length %d", domain))
		}
		if v.length < 0 || v.required < 0 || (v.scoped && v.required == 0) {
			issue("invalid_vlog", fmt.Sprintf("vlog/%d", id), "negative prefix or missing protection requirement")
		}
		geometryOK := false
		switch scheme {
		case "NONE":
			geometryOK = data == 1 && parity == 0 && (v.required == 0 || v.required == 1)
		case "DUPLICATE":
			geometryOK = data == 1 && parity == 0
		case "EC":
			geometryOK = data > 0 && parity > 0 && (v.required == 0 || data == int64(v.required)-parity)
		}
		if !geometryOK {
			issue("protection_geometry", fmt.Sprintf("vlog/%d", id), fmt.Sprintf("scheme %q geometry %d+%d requires %d shards", scheme, data, parity, v.required))
		}
		if (targetData != 0 || targetParity != 0) && (scheme != "DUPLICATE" || targetData <= 0 || targetParity <= 0 || (v.required > 0 && int64(v.required) <= targetParity)) {
			issue("staging_geometry", fmt.Sprintf("vlog/%d", id), fmt.Sprintf("scheme %q has invalid EC target %d+%d", scheme, targetData, targetParity))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	present := map[string]bool{}
	referencedVlogs := map[int64]bool{}
	type recordSpan struct {
		start, end int64
		hash       string
	}
	spans := map[int64][]recordSpan{}
	if err := scan("SELECT hash,refcount,vlog_id,vaddr_offset,logical_len FROM chunk", func(r *sql.Rows) error {
		var hash []byte
		var refs, id, offset, n int64
		if err := r.Scan(&hash, &refs, &id, &offset, &n); err != nil {
			return err
		}
		key := string(hash)
		object := fmt.Sprintf("chunk/%x", hash)
		present[key] = true
		if len(hash) != 15 {
			issue("invalid_hash", object, "content address must contain 15 bytes")
		}
		if refs != expected[key] {
			issue("reference_count", object, fmt.Sprintf("stored %d, independently traversed %d", refs, expected[key]))
		}
		if expected[key] > 0 {
			referencedVlogs[id] = true
			if n != int64(lengths[key]) {
				issue("extent_length", object, fmt.Sprintf("catalog %d, extent %d", n, lengths[key]))
			}
		}
		v, ok := vlogs[id]
		if !ok {
			issue("missing_vlog", object, fmt.Sprintf("vlog %d is absent", id))
		} else if offset < 0 || n < 0 || offset > v.length || n > v.length-offset-storage.ChunkHeaderSize {
			issue("extent_bounds", object, fmt.Sprintf("offset %d length %d exceeds durable prefix %d", offset, n, v.length))
		} else if expected[key] > 0 {
			spans[id] = append(spans[id], recordSpan{offset, offset + n + storage.ChunkHeaderSize, key})
		}
		return nil
	}); err != nil {
		return nil, err
	}
	for hash := range expected {
		if !present[hash] {
			issue("missing_chunk", fmt.Sprintf("chunk/%x", hash), "referenced extent has no canonical placement")
		}
	}
	for id, records := range spans {
		sort.Slice(records, func(i, j int) bool {
			if records[i].start != records[j].start {
				return records[i].start < records[j].start
			}
			return records[i].hash < records[j].hash
		})
		var previous recordSpan
		for i, record := range records {
			if i > 0 && record.start < previous.end {
				issue("overlapping_records", fmt.Sprintf("vlog/%d", id), fmt.Sprintf("chunks %x and %x have overlapping stored records", previous.hash, record.hash))
			}
			if i == 0 || record.end > previous.end {
				previous = record
			}
		}
	}
	counts := map[int64]int{}
	disks := map[int64]map[int64]bool{}
	if err := scan(`SELECT vp.vlog_id,vp.shard_idx,vp.plog_id,p.disk_id FROM vlog_plog vp LEFT JOIN plog p ON p.id=vp.plog_id ORDER BY vp.vlog_id,vp.shard_idx`, func(r *sql.Rows) error {
		var id, index, plog int64
		var disk sql.NullInt64
		if err := r.Scan(&id, &index, &plog, &disk); err != nil {
			return err
		}
		object := fmt.Sprintf("vlog/%d", id)
		if _, ok := vlogs[id]; !ok {
			issue("orphan_mapping", object, "mapping names an absent vlog")
		}
		if index != int64(counts[id]) {
			issue("shard_index", object, fmt.Sprintf("index %d, expected contiguous index %d", index, counts[id]))
		}
		counts[id]++
		if !disk.Valid {
			issue("missing_plog", object, fmt.Sprintf("plog %d is absent", plog))
			return nil
		}
		if disks[id] == nil {
			disks[id] = map[int64]bool{}
		}
		if disks[id][disk.Int64] {
			issue("colocated_shards", object, fmt.Sprintf("disk %d hosts multiple shards", disk.Int64))
		}
		disks[id][disk.Int64] = true
		return nil
	}); err != nil {
		return nil, err
	}
	for id := range referencedVlogs {
		v := vlogs[id]
		if v.required > 0 && counts[id] != v.required {
			issue("protection_count", fmt.Sprintf("vlog/%d", id), fmt.Sprintf("mapped %d, required %d", counts[id], v.required))
		}
	}
	if err := scan(`SELECT r.write_op_id,r.file_id,w.file_id,w.state FROM write_result_root r LEFT JOIN write_op w ON w.id=r.write_op_id`, func(r *sql.Rows) error {
		var op, file int64
		var result sql.NullInt64
		var state sql.NullString
		if err := r.Scan(&op, &file, &result, &state); err != nil {
			return err
		}
		if !result.Valid || result.Int64 != file || !state.Valid || state.String != WriteOpCommitted {
			issue("retry_result", fmt.Sprintf("write_op/%d", op), "retry root does not match a committed operation result")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := scan(`SELECT vl.vlog_id,vl.write_op_id,wo.state,v.maintenance_owned FROM vlog_lease vl LEFT JOIN write_op wo ON wo.id=vl.write_op_id LEFT JOIN vlog v ON v.id=vl.vlog_id`, func(r *sql.Rows) error {
		var id, op int64
		var state sql.NullString
		var owned sql.NullBool
		if err := r.Scan(&id, &op, &state, &owned); err != nil {
			return err
		}
		if !state.Valid || state.String != WriteOpPrepared || !owned.Valid || owned.Bool {
			issue("lease_owner", fmt.Sprintf("vlog/%d", id), fmt.Sprintf("lease operation %d is absent/terminal or conflicts with maintenance", op))
		}
		return nil
	}); err != nil {
		return nil, err
	}

	type namespaceRow struct {
		scope, path, parent, name string
		dir                       bool
	}
	names := map[string]namespaceRow{}
	nsquery := `SELECT 'live',path,parent,name,0 FROM file_head UNION ALL SELECT 'live',path,parent,name,1 FROM dir
 UNION ALL SELECT 'snapshot:'||snapshot_id,path,parent,name,0 FROM snapshot_file UNION ALL SELECT 'snapshot:'||snapshot_id,path,parent,name,1 FROM snapshot_dir`
	if err := scan(nsquery, func(r *sql.Rows) error {
		var e namespaceRow
		if err := r.Scan(&e.scope, &e.path, &e.parent, &e.name, &e.dir); err != nil {
			return err
		}
		object := e.scope + ":" + e.path
		canonical := strings.TrimPrefix(pathpkg.Clean("/"+e.path), "/")
		parent, name := "", e.path
		if i := strings.LastIndexByte(e.path, '/'); i >= 0 {
			parent, name = e.path[:i], e.path[i+1:]
		}
		if e.path == "" || e.path != canonical || parent != e.parent || name != e.name {
			issue("namespace_index", object, "path, parent, or name is not canonical")
		}
		key := e.scope + "\x00" + e.path
		if _, exists := names[key]; exists {
			issue("namespace_collision", object, "file and directory occupy the same path")
		}
		names[key] = e
		return nil
	}); err != nil {
		return nil, err
	}
	for _, e := range names {
		if e.parent != "" {
			p, exists := names[e.scope+"\x00"+e.parent]
			if !exists || !p.dir {
				issue("namespace_parent", e.scope+":"+e.path, "parent directory is absent")
			}
		}
	}
	if err := scan(`SELECT kind,target_vlog,COUNT(*) FROM job WHERE state='running' AND kind IN ('compact','promote','scrubrepair') GROUP BY kind,target_vlog HAVING COUNT(*)>1`, func(r *sql.Rows) error {
		var kind string
		var target, count int64
		if err := r.Scan(&kind, &target, &count); err != nil {
			return err
		}
		issue("duplicate_job_owner", fmt.Sprintf("vlog/%d", target), fmt.Sprintf("%d running %s jobs", count, kind))
		return nil
	}); err != nil {
		return nil, err
	}
	if err := scan(`SELECT j.id,j.dest_vlog,v.maintenance_owned FROM job j LEFT JOIN vlog v ON v.id=j.dest_vlog WHERE j.state='running' AND j.dest_vlog!=0`, func(r *sql.Rows) error {
		var id, dest int64
		var owned sql.NullBool
		if err := r.Scan(&id, &dest, &owned); err != nil {
			return err
		}
		if !owned.Valid || !owned.Bool {
			issue("job_destination", fmt.Sprintf("job/%d", id), fmt.Sprintf("destination %d is absent or not maintenance-owned", dest))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := scan("PRAGMA foreign_key_check", func(r *sql.Rows) error {
		var table, parent string
		var row sql.NullInt64
		var constraint int64
		if err := r.Scan(&table, &row, &parent, &constraint); err != nil {
			return err
		}
		issue("foreign_key", fmt.Sprintf("%s/%d", table, row.Int64), fmt.Sprintf("constraint %d references missing %s row", constraint, parent))
		return nil
	}); err != nil {
		return nil, err
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
