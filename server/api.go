package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log/slog"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

type FileHandle struct {
	id         int64
	path       string
	snapshotID uint64
	buffer     []byte
}

func (s *Server) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	// Simple implementation
	path := req.GetPath()
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	id, err := s.db.OpenFile(ctx, path)
	if err != nil {
		return nil, err
	}

	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()
	hid := s.handleCounter
	s.handleCounter++

	s.handles[hid] = &FileHandle{
		id:     id,
		path:   path,
		buffer: make([]byte, 0),
	}

	slog.Info("Open", "handle", hid, "id", id, "path", path)

	return &pb.OpenResponse{Handle: hid}, nil
}

func (s *Server) OpenSnapshot(ctx context.Context, req *pb.OpenSnapshotRequest) (*pb.OpenResponse, error) {
	if req.GetPath() == "" || req.GetSnapshotId() == 0 {
		return nil, fmt.Errorf("snapshot_id and path are required")
	}
	id, err := s.db.OpenSnapshotFile(ctx, req.GetSnapshotId(), req.GetPath())
	if err != nil {
		return nil, err
	}
	if id == 0 {
		return nil, fmt.Errorf("path not found in snapshot")
	}
	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()
	hid := s.handleCounter
	s.handleCounter++
	s.handles[hid] = &FileHandle{id: id, path: req.GetPath(), snapshotID: req.GetSnapshotId()}
	return &pb.OpenResponse{Handle: hid}, nil
}

func (s *Server) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.UnlinkResponse, error) {
	if req.GetPath() == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}
	if err := s.db.UnlinkFile(ctx, req.GetPath()); err != nil {
		return nil, err
	}
	return &pb.UnlinkResponse{}, nil
}

func (s *Server) Rename(ctx context.Context, req *pb.RenameRequest) (*pb.RenameResponse, error) {
	if req.GetOldPath() == "" || req.GetNewPath() == "" {
		return nil, fmt.Errorf("old_path and new_path are required")
	}
	if err := s.db.RenameFile(ctx, req.GetOldPath(), req.GetNewPath()); err != nil {
		return nil, err
	}
	return &pb.RenameResponse{}, nil
}

func (s *Server) CreateSnapshot(ctx context.Context, req *pb.CreateSnapshotRequest) (*pb.CreateSnapshotResponse, error) {
	if req.GetName() == "" {
		return nil, fmt.Errorf("snapshot name cannot be empty")
	}
	id, err := s.db.CreateSnapshot(ctx, req.GetName(), time.Now().UnixNano())
	if err != nil {
		return nil, err
	}
	return &pb.CreateSnapshotResponse{SnapshotId: id}, nil
}

func (s *Server) DeleteSnapshot(ctx context.Context, req *pb.DeleteSnapshotRequest) (*pb.DeleteSnapshotResponse, error) {
	if req.GetSnapshotId() == 0 {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	if err := s.db.DeleteSnapshot(ctx, req.GetSnapshotId()); err != nil {
		return nil, err
	}
	return &pb.DeleteSnapshotResponse{}, nil
}

func (s *Server) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()

	h, ok := s.handles[req.GetHandle()]
	if !ok {
		slog.Error("Write failed: invalid handle", "handle", req.GetHandle())
		return nil, fmt.Errorf("invalid handle")
	}
	if h.snapshotID != 0 {
		return nil, fmt.Errorf("snapshot handles are read-only")
	}

	slog.Info("Write", "handle", req.GetHandle())

	h.buffer = append(h.buffer, req.GetBuffer()...)
	return &pb.WriteResponse{}, nil
}

func (s *Server) Read(ctx context.Context, req *pb.ReadRequest) (*pb.ReadResponse, error) {
	s.handlesMu.Lock()
	defer s.handlesMu.Unlock()

	slog.Info("Read", "handle", req.GetHandle(), "offset", req.GetOffset(), "length", req.GetLength())

	h, ok := s.handles[req.GetHandle()]
	if !ok {
		slog.Error("Read failed: invalid handle", "handle", req.GetHandle())
		return nil, fmt.Errorf("invalid handle")
	}

	// If the file handle is currently open and has a buffer (e.g., being written to right now), read from it.
	if len(h.buffer) > 0 {
		slog.Info("Read from buffer", "handle", req.GetHandle(), "offset", req.GetOffset(), "length", req.GetLength())

		if req.GetOffset() >= int64(len(h.buffer)) {
			return &pb.ReadResponse{Buffer: nil}, nil
		}

		end := req.GetOffset() + req.GetLength()
		if end > int64(len(h.buffer)) {
			end = int64(len(h.buffer))
		}

		return &pb.ReadResponse{
			Buffer: h.buffer[req.GetOffset():end],
		}, nil
	}

	if h.id == 0 {
		return &pb.ReadResponse{Buffer: nil}, nil
	}

	// Fetch chunks for this file
	chunks, err := s.db.GetFileChunks(ctx, h.id)
	if err != nil {
		slog.Error("Read failed to get chunks", "fileID", h.id, "error", err)
		return nil, err
	}

	slog.Info("Read chunks", "fileID", h.id, "chunks", len(chunks))

	var out []byte
	var currentLogicalOffset int64 = 0

	for _, chunk := range chunks {
		chunkEnd := currentLogicalOffset + int64(chunk.LogicalLen)

		// Check if this chunk intersects with the read request window
		if req.GetOffset() < chunkEnd && req.GetOffset()+req.GetLength() > currentLogicalOffset {
			vlog, ok := s.vlogs[chunk.VlogID]
			if !ok {
				slog.Error("Read map vlog missing", "vlogID", chunk.VlogID)
				return nil, fmt.Errorf("vlog %d not mounted", chunk.VlogID)
			}

			// Calculate how much to read from this chunk
			readStart := currentLogicalOffset
			if req.GetOffset() > readStart {
				readStart = req.GetOffset()
			}

			readEnd := chunkEnd
			if req.GetOffset()+req.GetLength() < readEnd {
				readEnd = req.GetOffset() + req.GetLength()
			}

			chunkOffset := readStart - currentLogicalOffset
			chunkReadLen := readEnd - readStart

			data, err := vlog.Read(ctx, chunk.VaddrOffset+chunkOffset, int(chunkReadLen))
			if err != nil {
				return nil, err
			}
			out = append(out, data...)
		}

		currentLogicalOffset += int64(chunk.LogicalLen)

		// Optimization: Break early if we've fulfilled the read length
		if currentLogicalOffset >= req.GetOffset()+req.GetLength() {
			break
		}
	}

	return &pb.ReadResponse{Buffer: out}, nil
}
func (s *Server) Getattr(ctx context.Context, req *pb.GetattrRequest) (*pb.GetattrResponse, error) {
	// First check memory buffers for un-closed handles
	s.handlesMu.Lock()
	for _, h := range s.handles {
		if h.path == req.GetPath() {
			size := int64(len(h.buffer))
			s.handlesMu.Unlock()
			return &pb.GetattrResponse{Size: size}, nil
		}
	}
	s.handlesMu.Unlock()

	size, err := s.db.GetFileSize(ctx, req.GetPath())
	if err != nil {
		slog.Error("Getattr failed", "path", req.GetPath(), "error", err)
		return nil, err
	}
	return &pb.GetattrResponse{Size: size}, nil
}

func (s *Server) getOrCreateActiveVlog(ctx context.Context) (uint32, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()

	if s.activeVlog != 0 {
		return s.activeVlog, nil
	}

	// Default protection for now is DUPLICATE across every configured disk.
	id, _, err := s.provisionVlogLocked(ctx, "DUPLICATE", 1, 0)
	if err != nil {
		return 0, err
	}
	s.activeVlog = id
	return id, nil
}

// activeVlogForAppend rolls the active vlog before an append would cross the
// 32-bit virtual-offset boundary. The caller does not hold vlogMu.
func (s *Server) activeVlogForAppend(ctx context.Context, n int) (uint32, *storage.Vlog, error) {
	if int64(n) > MaxVlogBytes {
		return 0, nil, fmt.Errorf("append of %d bytes exceeds max vlog size %d", n, MaxVlogBytes)
	}
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	if s.activeVlog != 0 {
		if v, ok := s.vlogs[s.activeVlog]; ok && v.Length()+int64(n) <= MaxVlogBytes {
			return s.activeVlog, v, nil
		}
		s.activeVlog = 0
	}
	id, v, err := s.provisionVlogLocked(ctx, "DUPLICATE", 1, 0)
	if err != nil {
		return 0, nil, err
	}
	s.activeVlog = id
	return id, v, nil
}

func (s *Server) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	s.handlesMu.Lock()
	h, ok := s.handles[req.GetHandle()]
	if !ok {
		s.handlesMu.Unlock()
		return nil, fmt.Errorf("invalid handle")
	}

	slog.Info("Close", "handle", req.GetHandle(), "path", h.path)
	delete(s.handles, req.GetHandle())
	s.handlesMu.Unlock()

	if len(h.buffer) > 0 {
		rd := bytes.NewReader(h.buffer)
		chunker, err := chunkers.NewChunker("fastcdc", rd, nil)
		if err != nil {
			slog.Error("Failed to create chunker", "error", err)
			return nil, fmt.Errorf("new chunker: %w", err)
		}

		var placements []meta.ChunkPlacement
		usedVlogs := make(map[uint32]*storage.Vlog)
		for {
			chunkData, err := chunker.Next()
			if err != nil && err != io.EOF {
				slog.Error("Chunking error", "error", err)
				return nil, fmt.Errorf("chunking error: %w", err)
			}
			if len(chunkData) > 0 {
				vlogID, v, err := s.activeVlogForAppend(ctx, len(chunkData))
				if err != nil {
					return nil, fmt.Errorf("select active vlog: %w", err)
				}
				hashBytes := sha256.Sum256(chunkData)
				chunkHash := hashBytes[:15]

				offset, wErr := v.Write(ctx, 0, chunkData)
				if wErr != nil {
					slog.Error("Failed to write to vlog", "vlog_id", vlogID, "error", wErr)
					return nil, fmt.Errorf("write vlog: %w", wErr)
				}

				placements = append(placements, meta.ChunkPlacement{
					Hash:          chunkHash,
					VlogID:        vlogID,
					VaddrOffset:   offset,
					LogicalLen:    len(chunkData),
					CompressedLen: len(chunkData),
				})
				usedVlogs[vlogID] = v
			}
			if err == io.EOF {
				break
			}
		}

		// Refuse to acknowledge the write unless the vlog still has enough live
		// shards to hold it durably. With too many backing disks down the server
		// degrades to read-only instead of claiming durability it cannot provide.
		for vlogID, v := range usedVlogs {
			ready, err := s.CommitReady(ctx, vlogID)
			if err != nil {
				return nil, fmt.Errorf("check commit readiness: %w", err)
			}
			if !ready {
				return nil, fmt.Errorf("vlog %d degraded: too few live shards to durably commit", vlogID)
			}
			if err := v.Commit(ctx, 0); err != nil {
				return nil, fmt.Errorf("commit vlog: %w", err)
			}
			if err := s.db.SetVlogLength(ctx, vlogID, v.Length()); err != nil {
				return nil, fmt.Errorf("persist vlog cursor: %w", err)
			}
		}

		// The vlog bytes are durable before metadata publishes references to
		// them. A crash here leaves the chunks as orphan log data reclaimed by
		// later compaction, never a dangling metadata reference.
		if _, err := s.db.CommitFile(ctx, h.path, time.Now().UnixNano(), placements); err != nil {
			return nil, fmt.Errorf("publish file metadata: %w", err)
		}
	}

	return &pb.CloseResponse{}, nil
}

// Vlog Operations
func (s *Server) MakeVlog(ctx context.Context, req *pb.MakeVlogRequest) (*pb.MakeVlogResponse, error) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	id, _, err := s.provisionVlogLocked(ctx, req.GetProtectionScheme(), int(req.GetDataShards()), int(req.GetParityShards()))
	if err != nil {
		return nil, err
	}
	return &pb.MakeVlogResponse{VlogId: id}, nil
}

// Plog Operations
func (s *Server) MakePlog(ctx context.Context, req *pb.MakePlogRequest) (*pb.MakePlogResponse, error) {
	id, err := s.db.MakePlog(ctx, req.GetDiskId())
	if err != nil {
		return nil, err
	}
	plog, err := storage.OpenPlog(s.plogPath(req.GetDiskId(), id), id)
	if err != nil {
		return nil, err
	}
	s.plogs[id] = plog
	return &pb.MakePlogResponse{PlogId: id}, nil
}

func (s *Server) WritePlog(ctx context.Context, req *pb.WritePlogRequest) (*pb.WritePlogResponse, error) {
	plog, ok := s.plogs[req.GetPlogId()]
	if !ok {
		return nil, fmt.Errorf("plog not found")
	}
	offset, err := plog.Write(req.GetTxnId(), req.GetBuffer())
	if err != nil {
		return nil, err
	}
	return &pb.WritePlogResponse{Offset: uint32(offset)}, nil
}

func (s *Server) ReadPlog(ctx context.Context, req *pb.ReadPlogRequest) (*pb.ReadPlogResponse, error) {
	plog, ok := s.plogs[req.GetPlogId()]
	if !ok {
		return nil, fmt.Errorf("plog not found")
	}
	data, err := plog.Read(int64(req.GetOffset()), int(req.GetLength()))
	if err != nil {
		return nil, err
	}
	return &pb.ReadPlogResponse{Buffer: data}, nil
}

func (s *Server) CommitPlog(ctx context.Context, req *pb.CommitPlogRequest) (*pb.CommitPlogResponse, error) {
	for _, plog := range s.plogs {
		if err := plog.Commit(); err != nil {
			return nil, err
		}
	}
	return &pb.CommitPlogResponse{}, nil
}

func (s *Server) ReadVlog(ctx context.Context, req *pb.ReadVlogRequest) (*pb.ReadVlogResponse, error) {
	v, ok := s.vlogs[req.GetVlogId()]
	if !ok {
		return nil, fmt.Errorf("vlog not found")
	}

	data, err := v.Read(ctx, int64(req.GetOffset()), int(req.GetLength()))
	if err != nil {
		return nil, err
	}
	return &pb.ReadVlogResponse{Buffer: data}, nil
}

func (s *Server) WriteVlog(ctx context.Context, req *pb.WriteVlogRequest) (*pb.WriteVlogResponse, error) {
	v, ok := s.vlogs[req.GetVlogId()]
	if !ok {
		return nil, fmt.Errorf("vlog not found")
	}
	if v.Length()+int64(len(req.GetBuffer())) > MaxVlogBytes {
		return nil, fmt.Errorf("vlog %d would exceed max size %d", req.GetVlogId(), MaxVlogBytes)
	}

	offset, err := v.Write(ctx, req.GetTxnId(), req.GetBuffer())
	if err != nil {
		return nil, err
	}
	if err := s.db.SetVlogLength(ctx, req.GetVlogId(), v.Length()); err != nil {
		return nil, err
	}
	return &pb.WriteVlogResponse{Offset: uint32(offset)}, nil
}

func (s *Server) CommitVlog(ctx context.Context, req *pb.CommitVlogRequest) (*pb.CommitVlogResponse, error) {
	for _, vlog := range s.vlogs {
		if err := vlog.Commit(ctx, req.GetTxnId()); err != nil {
			return nil, err
		}
	}
	return &pb.CommitVlogResponse{}, nil
}

// Ensure the server implements pb.RoseServer
var _ pb.RoseServer = &Server{}
