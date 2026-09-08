package meta

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/rmmh/rose/storage"
	"github.com/rmmh/rose/uid"
)

type inspectionClient struct {
	reader *storage.InspectionReader
	err    error
}

func (c inspectionClient) Write(context.Context, int64, []byte) (int64, error) {
	return 0, fmt.Errorf("inspection is read-only")
}
func (c inspectionClient) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.err != nil {
		return nil, c.err
	}
	return c.reader.Read(offset, length)
}

func checkPlaintextTx(ctx context.Context, tx *sql.Tx, roots map[uint32]string, add func(string, string, string)) error {
	var rawKey []byte
	var algorithm string
	if err := tx.QueryRowContext(ctx, "SELECT encryption_key,encryption_alg FROM cluster WHERE id=1").Scan(&rawKey, &algorithm); err != nil {
		return err
	}
	key, err := uid.FromBytes(rawKey)
	if err != nil || algorithm != storage.VlogEncryptionAlgorithm {
		add("plaintext_unverifiable", "cluster", "invalid key encoding or unsupported encryption algorithm")
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT v.id,v.uid,v.protection_scheme,v.data_shards,v.parity_shards,v.length,v.dedup_domain FROM vlog v WHERE EXISTS(SELECT 1 FROM chunk WHERE vlog_id=v.id AND refcount>0) ORDER BY v.id`)
	if err != nil {
		return err
	}
	type entry struct {
		id           uint32
		uid, domain  []byte
		scheme       string
		data, parity int
		length       int64
	}
	var vlogs []entry
	for rows.Next() {
		var v entry
		if err := rows.Scan(&v.id, &v.uid, &v.scheme, &v.data, &v.parity, &v.length, &v.domain); err != nil {
			rows.Close()
			return err
		}
		vlogs = append(vlogs, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, v := range vlogs {
		u, err := uid.FromBytes(v.uid)
		if err != nil {
			add("plaintext_unverifiable", fmt.Sprintf("vlog/%d", v.id), "invalid vlog UID")
			continue
		}
		if err := checkVlogPlaintext(ctx, tx, roots, v.id, v.scheme, v.data, v.parity, v.length, v.domain, storage.DeriveVlogKey(key, u), add); err != nil {
			return err
		}
	}
	return nil
}

func checkVlogPlaintext(ctx context.Context, tx *sql.Tx, roots map[uint32]string, id uint32, scheme string, data, parity int, length int64, domain []byte, key [16]byte, add func(string, string, string)) error {
	rows, err := tx.QueryContext(ctx, `SELECT vp.shard_idx,p.id,p.disk_id FROM vlog_plog vp JOIN plog p ON p.id=vp.plog_id WHERE vp.vlog_id=? ORDER BY vp.shard_idx`, id)
	if err != nil {
		return err
	}
	var clients []storage.PlogClient
	var opened []*storage.InspectionReader
	defer func() {
		for _, r := range opened {
			r.Close()
		}
	}()
	for rows.Next() {
		var index int
		var plog, disk uint32
		if err := rows.Scan(&index, &plog, &disk); err != nil {
			rows.Close()
			return err
		}
		if index != len(clients) {
			rows.Close()
			add("plaintext_unverifiable", fmt.Sprintf("vlog/%d", id), "noncontiguous shard mappings")
			return nil
		}
		var reader *storage.InspectionReader
		root, ok := roots[disk]
		var openErr error
		if !ok {
			openErr = fmt.Errorf("disk %d has no inspection root", disk)
		} else {
			reader, openErr = storage.OpenInspectionReader(filepath.Join(root, fmt.Sprintf("plog-%05d", plog)), plog)
		}
		if reader != nil {
			opened = append(opened, reader)
		}
		clients = append(clients, inspectionClient{reader, openErr})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	vlog, err := storage.NewVlog(id, scheme, data, parity, clients, length)
	if err != nil {
		add("plaintext_unverifiable", fmt.Sprintf("vlog/%d", id), err.Error())
		return nil
	}
	if err := inspectStoredProtection(ctx, scheme, data, parity, length, clients); err != nil {
		add("vlog_protection", fmt.Sprintf("vlog/%d", id), err.Error())
	}
	rows, err = tx.QueryContext(ctx, "SELECT hash,vaddr_offset,logical_len FROM chunk WHERE vlog_id=? AND refcount>0 ORDER BY vaddr_offset", id)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var hash []byte
		var offset, n int64
		if err := rows.Scan(&hash, &offset, &n); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := inspectPlaintextRecord(ctx, vlog, key, domain, hash, offset, n, length); err != nil {
			add("chunk_plaintext", fmt.Sprintf("chunk/%x", hash), err.Error())
		}
	}
	return rows.Err()
}

func inspectPlaintextRecord(ctx context.Context, vlog *storage.Vlog, key [16]byte, domain, hash []byte, offset, n, prefix int64) error {
	if len(hash) != 15 || offset < 0 || n < 0 || offset > prefix || n > prefix-offset-storage.ChunkHeaderSize {
		return fmt.Errorf("invalid catalog record bounds or hash")
	}
	raw, err := vlog.Read(ctx, offset, storage.ChunkHeaderSize)
	if err != nil {
		return err
	}
	header, err := storage.DecodeEncryptedChunkHeader(raw, key, hash)
	if err != nil {
		return err
	}
	if int64(header.PayloadLen) != n {
		return fmt.Errorf("header payload length differs from catalog")
	}
	stream, err := storage.DeriveChunkStream(key, hash)
	if err != nil {
		return err
	}
	digest := sha256.New()
	if len(domain) != 0 {
		digest.Write([]byte("rose scoped chunk v1\x00"))
		digest.Write(domain)
	}
	for pos := int64(0); pos < n; {
		if err := ctx.Err(); err != nil {
			return err
		}
		count := int(min(n-pos, 1<<20))
		buf, err := vlog.Read(ctx, offset+storage.ChunkHeaderSize+pos, count)
		if err != nil {
			return err
		}
		if len(buf) != count {
			return fmt.Errorf("short record payload")
		}
		if err := storage.ApplyAES128CTR(key, stream, storage.ChunkHeaderSize+pos, buf); err != nil {
			return err
		}
		digest.Write(buf)
		pos += int64(count)
	}
	if !bytes.Equal(digest.Sum(nil)[:15], hash) {
		return fmt.Errorf("plaintext content hash differs from canonical address")
	}
	return nil
}
