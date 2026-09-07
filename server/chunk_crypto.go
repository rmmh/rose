package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/rmmh/rose/meta"
	"github.com/rmmh/rose/storage"
)

func (s *Server) keyForVlog(ctx context.Context, vlogID uint32) ([16]byte, error) {
	if err := s.ensureClusterKeys(ctx); err != nil {
		return [16]byte{}, err
	}
	s.keyMu.RLock()
	key, ok := s.vlogKeys[vlogID]
	s.keyMu.RUnlock()
	if ok {
		return key, nil
	}
	info, err := s.db.GetVlog(ctx, vlogID)
	if err != nil {
		return [16]byte{}, err
	}
	key = storage.DeriveVlogKey(s.clusterKey, info.UID)
	s.setVlogKey(vlogID, key)
	return key, nil
}

func (s *Server) encryptChunkRecord(ctx context.Context, vlogID uint32, hash []byte, hdr storage.ChunkHeader, payload []byte) ([]byte, error) {
	encoded, err := s.encryptChunkInPlace(ctx, vlogID, hash, hdr, payload)
	if err != nil {
		return nil, err
	}
	record := make([]byte, storage.ChunkHeaderSize+len(payload))
	copy(record[:storage.ChunkHeaderSize], encoded[:])
	copy(record[storage.ChunkHeaderSize:], payload)
	return record, nil
}

func (s *Server) encryptChunkInPlace(ctx context.Context, vlogID uint32, hash []byte, hdr storage.ChunkHeader, payload []byte) ([storage.ChunkHeaderSize]byte, error) {
	key, err := s.keyForVlog(ctx, vlogID)
	if err != nil {
		return [storage.ChunkHeaderSize]byte{}, err
	}
	encoded, err := hdr.EncodeEncrypted(key, hash)
	if err != nil {
		return [storage.ChunkHeaderSize]byte{}, err
	}
	stream, err := storage.DeriveChunkStream(key, hash)
	if err != nil {
		return [storage.ChunkHeaderSize]byte{}, err
	}
	return encoded, storage.ApplyAES128CTR(key, stream, storage.ChunkHeaderSize, payload)
}

func (s *Server) decryptChunkPayload(ctx context.Context, vlogID uint32, hash []byte, payloadOffset int64, ciphertext []byte) error {
	key, err := s.keyForVlog(ctx, vlogID)
	if err != nil {
		return err
	}
	stream, err := storage.DeriveChunkStream(key, hash)
	if err != nil {
		return err
	}
	return storage.ApplyAES128CTR(key, stream, storage.ChunkHeaderSize+payloadOffset, ciphertext)
}

func (s *Server) readChunkPayload(ctx context.Context, vlog *storage.Vlog, p meta.ChunkPlacement, payloadOffset int64, length int) ([]byte, error) {
	data, err := vlog.Read(ctx, p.VaddrOffset+storage.ChunkHeaderSize+payloadOffset, length)
	if err != nil {
		return nil, err
	}
	if err := s.decryptChunkPayload(ctx, p.VlogID, p.Hash, payloadOffset, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *Server) readPlainChunkRecord(ctx context.Context, vlog *storage.Vlog, vlogID uint32, c meta.ChunkLoc) ([]byte, error) {
	key, err := s.keyForVlog(ctx, vlogID)
	if err != nil {
		return nil, err
	}
	raw, err := vlog.Read(ctx, c.VaddrOffset, storage.ChunkHeaderSize+c.LogicalLen)
	if err != nil {
		return nil, err
	}
	if len(raw) < storage.ChunkHeaderSize {
		return nil, fmt.Errorf("short chunk record: %d", len(raw))
	}
	hdr, err := storage.DecodeEncryptedChunkHeader(raw[:storage.ChunkHeaderSize], key, c.Hash)
	if err != nil {
		return nil, err
	}
	if int(hdr.PayloadLen) != c.LogicalLen {
		return nil, fmt.Errorf("chunk payload length %d != metadata length %d", hdr.PayloadLen, c.LogicalLen)
	}
	record := make([]byte, len(raw))
	logical := hdr.Encode()
	copy(record[:storage.ChunkHeaderSize], logical[:])
	copy(record[storage.ChunkHeaderSize:], raw[storage.ChunkHeaderSize:])
	if err := s.decryptChunkPayload(ctx, vlogID, c.Hash, 0, record[storage.ChunkHeaderSize:]); err != nil {
		return nil, err
	}
	info, err := s.db.GetVlog(ctx, vlogID)
	if err != nil {
		return nil, err
	}
	sum := storage.ContentHash(info.DedupDomain, record[storage.ChunkHeaderSize:])
	if !bytes.Equal(sum[:15], c.Hash) {
		return nil, fmt.Errorf("chunk content hash mismatch: %w", storage.ErrBitrot)
	}
	return record, nil
}

func (s *Server) encryptPlainChunkRecord(ctx context.Context, vlogID uint32, hash []byte, record []byte) ([]byte, error) {
	if len(record) < storage.ChunkHeaderSize {
		return nil, fmt.Errorf("short chunk record: %d", len(record))
	}
	hdr, err := storage.DecodeChunkHeader(record[:storage.ChunkHeaderSize])
	if err != nil {
		return nil, err
	}
	if len(hash) >= 8 && hdr.ChunkHash != binary.LittleEndian.Uint64(hash[:8]) {
		return nil, fmt.Errorf("chunk hash64 mismatch")
	}
	return s.encryptChunkRecord(ctx, vlogID, hash, hdr, record[storage.ChunkHeaderSize:])
}

func (s *Server) encryptedChunkRecordValidator(ctx context.Context, vlogID uint32, hash []byte) (func([]byte) bool, error) {
	key, err := s.keyForVlog(ctx, vlogID)
	if err != nil {
		return nil, err
	}
	info, err := s.db.GetVlog(ctx, vlogID)
	if err != nil {
		return nil, err
	}
	hashCopy := append([]byte(nil), hash...)
	return func(record []byte) bool {
		if len(record) < storage.ChunkHeaderSize {
			return false
		}
		hdr, err := storage.DecodeEncryptedChunkHeader(record[:storage.ChunkHeaderSize], key, hashCopy)
		if err != nil {
			return false
		}
		if len(record) != storage.ChunkHeaderSize+int(hdr.PayloadLen) {
			return false
		}
		payload := append([]byte(nil), record[storage.ChunkHeaderSize:]...)
		stream := storage.DeriveChunkStreamHash64(key, hdr.ChunkHash)
		if err := storage.ApplyAES128CTR(key, stream, storage.ChunkHeaderSize, payload); err != nil {
			return false
		}
		sum := storage.ContentHash(info.DedupDomain, payload)
		return bytes.Equal(sum[:15], hashCopy)
	}, nil
}

// verifyProtectedChunk binds content verification and all-shard verification for
// both namespace publication and maintenance repointing. Callers fence placement
// changes, make the destination durable, and validate its required geometry.
func (s *Server) verifyProtectedChunk(ctx context.Context, v *storage.Vlog, vlogID uint32, c meta.ChunkLoc) error {
	plain, err := s.readPlainChunkRecord(ctx, v, vlogID, c)
	if err != nil {
		return err
	}
	expected, err := s.encryptPlainChunkRecord(ctx, vlogID, c.Hash, plain)
	if err != nil {
		return err
	}
	return v.VerifyAll(ctx, c.VaddrOffset, expected)
}
