package server

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

func auditWrite(t *testing.T, s *Server, path string, data []byte) {
	t.Helper()
	ctx := context.Background()
	h, err := s.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Write(ctx, &pb.WriteRequest{Handle: h.Handle, Buffer: data}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Close(ctx, &pb.CloseRequest{Handle: h.Handle}); err != nil {
		t.Fatal(err)
	}
}

func auditPlacement(t *testing.T, s *Server, path string) meta.ChunkPlacement {
	t.Helper()
	ctx := context.Background()
	id, err := s.db.OpenFile(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := s.db.FileVersionChunks(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected one chunk, got %d", len(chunks))
	}
	return chunks[0]
}

// Missing integrity metadata must not hide damage to previously published
// bytes. The healthy cases ensure recovery verifies, rather than rejecting all
// partial tails; mirrored cases require successful fallback to a good copy.
func TestRecoveryVerifiesRaggedPayloadWithoutTrailer(t *testing.T) {
	for _, size := range []int{100, 5000} {
		for _, disks := range []int{1, 2} {
			for _, corrupt := range []bool{false, true} {
				t.Run(fmt.Sprintf("bytes=%d/disks=%d/corrupt=%v", size, disks, corrupt), func(t *testing.T) {
					s := newControlPlaneServer(t, disks)
					s.StopMaintenanceDriver()
					defer s.CloseStorage()
					ctx := context.Background()
					data := bytes.Repeat([]byte("a"), size)
					auditWrite(t, s, "bucket/a", data)
					p := auditPlacement(t, s, "bucket/a")
					shards, err := s.db.VlogShardDisks(ctx, p.VlogID)
					if err != nil {
						t.Fatal(err)
					}
					path := s.plogPath(shards[0].DiskID, shards[0].PlogID)
					s.CloseStorage()
					f, err := os.OpenFile(path, os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					end := p.VaddrOffset + storage.ChunkHeaderSize + int64(size)
					if err := f.Truncate(storage.CalcPhysical(end)); err != nil {
						t.Fatal(err)
					}
					if corrupt {
						offset := storage.CalcPhysical(end - 1)
						b := []byte{0}
						if _, err := f.ReadAt(b, offset); err != nil {
							t.Fatal(err)
						}
						b[0] ^= 1
						if _, err := f.WriteAt(b, offset); err != nil {
							t.Fatal(err)
						}
					}
					if err := f.Close(); err != nil {
						t.Fatal(err)
					}
					if err := s.Recover(ctx); err != nil {
						t.Fatal(err)
					}
					s.StopMaintenanceDriver()
					h, err := s.Open(ctx, &pb.OpenRequest{Path: "bucket/a"})
					if err != nil {
						t.Fatal(err)
					}
					r, err := s.Read(ctx, &pb.ReadRequest{Handle: h.Handle, Length: int64(size)})
					if corrupt && disks == 1 {
						if err == nil {
							t.Fatal("corrupt committed bytes were readable without an error")
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(r.Buffer, data) {
							t.Fatal("recovery changed committed plaintext")
						}
					}
				})
			}
		}
	}
}
