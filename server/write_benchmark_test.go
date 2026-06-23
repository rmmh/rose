package server_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
)

func BenchmarkWrite100MB(b *testing.B) {
	const size = 100 << 20
	const block = 128 << 10
	base := make([]byte, size)
	if _, err := rand.New(rand.NewSource(99)).Read(base); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(size)

	for _, tc := range []struct {
		name   string
		bucket string
		setup  func(context.Context, *server.Server) error
	}{
		{
			name:   "duplicate",
			bucket: "mirror",
			setup:  func(context.Context, *server.Server) error { return nil },
		},
		{
			name:   "ec-3+1",
			bucket: "ec",
			setup: func(ctx context.Context, srv *server.Server) error {
				return srv.SetBucketPolicy(ctx, meta.BucketPolicy{Name: "ec", ProtectionScheme: "EC", DataShards: 3, ParityShards: 1})
			},
		},
	} {
		if only := os.Getenv("ROSE_BENCH_SCHEME"); only != "" && only != tc.name {
			continue
		}
		for _, mode := range []struct {
			name            string
			disableBatching bool
		}{
			{name: "batched", disableBatching: false},
			{name: "unbatched", disableBatching: true},
		} {
			if only := os.Getenv("ROSE_BENCH_MODE"); only != "" && only != mode.name {
				continue
			}
			b.Run(tc.name+"/"+mode.name, func(b *testing.B) {
				if mode.disableBatching {
					b.Setenv("ROSE_DISABLE_WRITE_BATCHING", "1")
				} else {
					b.Setenv("ROSE_DISABLE_WRITE_BATCHING", "")
				}
				ctx := context.Background()
				db, err := meta.OpenEphemeral()
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				roots := map[uint32]string{
					1: benchmarkDiskRoot(b),
					2: benchmarkDiskRoot(b),
					3: benchmarkDiskRoot(b),
					4: benchmarkDiskRoot(b),
				}
				srv := server.NewServerWithDiskRoots(db, roots)
				srv.SetMaintenanceInterval(0)
				if err := srv.Recover(ctx); err != nil {
					b.Fatal(err)
				}
				defer srv.StopMaintenanceDriver()
				if err := tc.setup(ctx, srv); err != nil {
					b.Fatal(err)
				}

				data := make([]byte, len(base))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					copy(data, base)
					var scratch [8]byte
					for off := 0; off < len(data); off += 256 << 10 {
						binary.LittleEndian.PutUint64(scratch[:], uint64(i+1+off))
						copy(data[off:min(off+len(scratch), len(data))], scratch[:])
					}
					path := fmt.Sprintf("/%s/bench-%d", tc.bucket, i)
					key := fmt.Sprintf("%s-%s-%d", tc.name, mode.name, i)
					open, err := srv.Open(ctx, &pb.OpenRequest{Path: path, OperationKey: key})
					if err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					for off := 0; off < len(data); off += block {
						end := off + block
						if end > len(data) {
							end = len(data)
						}
						if _, err := srv.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Offset: int64(off), Buffer: data[off:end]}); err != nil {
							b.Fatal(err)
						}
					}
					if _, err := srv.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle(), IdempotencyKey: key}); err != nil {
						b.Fatal(err)
					}
				}
				elapsed := b.Elapsed()
				if elapsed > 0 {
					totalMiB := float64(size*b.N) / float64(1<<20)
					b.ReportMetric(totalMiB/elapsed.Seconds(), "MiB/s")
				}
			})
		}
	}
}

func benchmarkDiskRoot(b *testing.B) string {
	b.Helper()
	if os.Getenv("ROSE_NO_RAMDISK") == "" && runtime.GOOS == "linux" {
		if info, err := os.Stat("/dev/shm"); err == nil && info.IsDir() {
			dir, err := os.MkdirTemp("/dev/shm", "rose-bench-")
			if err == nil {
				b.Cleanup(func() { _ = os.RemoveAll(dir) })
				return dir
			}
		}
	}
	return b.TempDir()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
