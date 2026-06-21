package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	_ "github.com/PlakarKorp/go-cdc-chunkers/chunkers/fastcdc"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

var (
	handlesMu     sync.Mutex
	handles       = make(map[int64]*FileHandle)
	handleCounter int64
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

	handlesMu.Lock()
	defer handlesMu.Unlock()
	hid := handleCounter
	handleCounter++

	handles[hid] = &FileHandle{
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
	handlesMu.Lock()
	defer handlesMu.Unlock()
	hid := handleCounter
	handleCounter++
	handles[hid] = &FileHandle{id: id, path: req.GetPath(), snapshotID: req.GetSnapshotId()}
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
	handlesMu.Lock()
	defer handlesMu.Unlock()

	h, ok := handles[req.GetHandle()]
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
	handlesMu.Lock()
	defer handlesMu.Unlock()

	slog.Info("Read", "handle", req.GetHandle(), "offset", req.GetOffset(), "length", req.GetLength())

	h, ok := handles[req.GetHandle()]
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
	handlesMu.Lock()
	for _, h := range handles {
		if h.path == req.GetPath() {
			size := int64(len(h.buffer))
			handlesMu.Unlock()
			return &pb.GetattrResponse{Size: size}, nil
		}
	}
	handlesMu.Unlock()

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

	// Create a new vlog. For now, we'll just use DUPLICATE with 1 data shard.
	id, err := s.db.MakeVlog(ctx, "DUPLICATE", 1, 0)
	if err != nil {
		return 0, err
	}

	// Make a plog and assign it to this vlog
	diskID := uint32(1) // stub
	plogID, err := s.db.MakePlog(ctx, diskID)
	if err != nil {
		return 0, err
	}

	err = s.db.AssignPlogToVlog(ctx, id, 0, plogID)
	if err != nil {
		return 0, err
	}

	plog, err := storage.OpenPlog(s.plogPath(plogID), plogID)
	if err != nil {
		return 0, fmt.Errorf("open plog %d: %w", plogID, err)
	}

	s.plogs[plogID] = plog

	vlog, err := storage.NewVlog(id, "DUPLICATE", 1, 0, []storage.PlogClient{&localPlogClient{plog: plog}}, 0)
	if err != nil {
		return 0, fmt.Errorf("create vlog in memory: %w", err)
	}

	s.vlogs[id] = vlog
	s.activeVlog = id
	return id, nil
}

func (s *Server) Close(ctx context.Context, req *pb.CloseRequest) (*pb.CloseResponse, error) {
	handlesMu.Lock()
	h, ok := handles[req.GetHandle()]
	if !ok {
		handlesMu.Unlock()
		return nil, fmt.Errorf("invalid handle")
	}

	slog.Info("Close", "handle", req.GetHandle(), "path", h.path)
	delete(handles, req.GetHandle())
	handlesMu.Unlock()

	if len(h.buffer) > 0 {
		vlogID, err := s.getOrCreateActiveVlog(ctx)
		if err != nil {
			slog.Error("Failed to get active vlog", "error", err)
			return nil, fmt.Errorf("get active vlog: %w", err)
		}

		v, ok := s.vlogs[vlogID]
		if !ok {
			return nil, fmt.Errorf("active vlog %d not mounted in memory", vlogID)
		}

		rd := bytes.NewReader(h.buffer)
		chunker, err := chunkers.NewChunker("fastcdc", rd, nil)
		if err != nil {
			slog.Error("Failed to create chunker", "error", err)
			return nil, fmt.Errorf("new chunker: %w", err)
		}

		var chunksBlob []byte
		for {
			chunkData, err := chunker.Next()
			if err != nil && err != io.EOF {
				slog.Error("Chunking error", "error", err)
				return nil, fmt.Errorf("chunking error: %w", err)
			}
			if len(chunkData) > 0 {
				hashBytes := sha256.Sum256(chunkData)
				chunkHash := hashBytes[:15]

				offset, wErr := v.Write(ctx, 0, chunkData)
				if wErr != nil {
					slog.Error("Failed to write to vlog", "vlog_id", vlogID, "error", wErr)
					return nil, fmt.Errorf("write vlog: %w", wErr)
				}

				if err := s.db.AddChunk(ctx, chunkHash, vlogID, offset, len(chunkData), len(chunkData)); err != nil {
					slog.Error("Failed to add chunk", "error", err)
					return nil, fmt.Errorf("add chunk meta: %w", err)
				}

				chunksBlob = append(chunksBlob, chunkHash...)
				lenBytes := make([]byte, 4)
				binary.LittleEndian.PutUint32(lenBytes, uint32(len(chunkData)))
				chunksBlob = append(chunksBlob, lenBytes...)
			}
			if err == io.EOF {
				break
			}
		}

		if _, err := s.db.CommitFile(ctx, h.path, time.Now().UnixNano(), chunksBlob); err != nil {
			slog.Error("Failed to commit file", "error", err)
		}
	}

	return &pb.CloseResponse{}, nil
}

// Vlog Operations
func (s *Server) MakeVlog(ctx context.Context, req *pb.MakeVlogRequest) (*pb.MakeVlogResponse, error) {
	id, err := s.db.MakeVlog(ctx, req.GetProtectionScheme(), req.GetDataShards(), req.GetParityShards())
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
	return &pb.MakePlogResponse{PlogId: id}, nil
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

	offset, err := v.Write(ctx, req.GetTxnId(), req.GetBuffer())
	if err != nil {
		return nil, err
	}
	return &pb.WriteVlogResponse{Offset: uint32(offset)}, nil
}

// Ensure the server implements pb.RoseServer
var _ pb.RoseServer = &Server{}
