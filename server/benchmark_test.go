package server_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
)

func Benchmark100MB(b *testing.B) {
	const size = 100 << 20
	const block = 128 << 10
	base := make([]byte, size)
	if _, err := rand.New(rand.NewSource(99)).Read(base); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(size)

	benchmarkByScheme(b, func(b *testing.B, ctx context.Context, srv *server.Server, bucket, scheme string) {
		var writeTime, readTime time.Duration
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
			path := fmt.Sprintf("/%s/bench-%d", bucket, i)
			key := fmt.Sprintf("%s-%d", scheme, i)
			open, err := srv.Open(ctx, &pb.OpenRequest{Path: path, OperationKey: key})
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			writeStart := time.Now()
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
			writeTime += time.Since(writeStart)
			b.StopTimer()

			openRead, err := srv.Open(ctx, &pb.OpenRequest{Path: path})
			if err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			readStart := time.Now()
			for off := 0; off < len(data); off += block {
				end := off + block
				if end > len(data) {
					end = len(data)
				}
				res, err := srv.Read(ctx, &pb.ReadRequest{Handle: openRead.GetHandle(), Offset: int64(off), Length: int64(end - off)})
				if err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(res.GetBuffer(), data[off:end]) {
					b.Fatalf("read mismatch at offset %d: expected %d bytes, got %d", off, end-off, len(res.GetBuffer()))
				}
			}
			readTime += time.Since(readStart)
			b.StopTimer()

			if _, err := srv.Close(ctx, &pb.CloseRequest{Handle: openRead.GetHandle()}); err != nil {
				b.Fatal(err)
			}
		}
		reportBenchmarkThroughput(b, size, writeTime, readTime)
	})
}

func Benchmark100MBx1kFiles(b *testing.B) {
	const (
		totalSize = 100 << 20
		fileCount = 1000
	)
	base := make([]byte, totalSize)
	if _, err := rand.New(rand.NewSource(99)).Read(base); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(totalSize)

	benchmarkByScheme(b, func(b *testing.B, ctx context.Context, srv *server.Server, bucket, scheme string) {
		var writeTime, readTime time.Duration
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for fileIdx := 0; fileIdx < fileCount; fileIdx++ {
				b.StopTimer()
				start, end := benchmarkFileSpan(totalSize, fileCount, fileIdx)
				data := base[start:end]
				var scratch [8]byte
				binary.LittleEndian.PutUint64(scratch[:], uint64(i+1+fileIdx))
				copy(data[:min(len(data), len(scratch))], scratch[:])

				path := fmt.Sprintf("/%s/bench-%d-%04d", bucket, i, fileIdx)
				key := fmt.Sprintf("%s-%d-%d", scheme, i, fileIdx)
				open, err := srv.Open(ctx, &pb.OpenRequest{Path: path, OperationKey: key})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				writeStart := time.Now()
				if _, err := srv.Write(ctx, &pb.WriteRequest{Handle: open.GetHandle(), Buffer: data}); err != nil {
					b.Fatal(err)
				}
				if _, err := srv.Close(ctx, &pb.CloseRequest{Handle: open.GetHandle(), IdempotencyKey: key}); err != nil {
					b.Fatal(err)
				}
				writeTime += time.Since(writeStart)
				b.StopTimer()

				openRead, err := srv.Open(ctx, &pb.OpenRequest{Path: path})
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				readStart := time.Now()
				res, err := srv.Read(ctx, &pb.ReadRequest{Handle: openRead.GetHandle(), Length: int64(len(data))})
				if err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(res.GetBuffer(), data) {
					b.Fatalf("read mismatch for file %d: expected %d bytes, got %d", fileIdx, len(data), len(res.GetBuffer()))
				}
				readTime += time.Since(readStart)
				b.StopTimer()

				if _, err := srv.Close(ctx, &pb.CloseRequest{Handle: openRead.GetHandle()}); err != nil {
					b.Fatal(err)
				}
			}
		}
		reportBenchmarkThroughput(b, totalSize, writeTime, readTime)
	})
}

func benchmarkByScheme(b *testing.B, bench func(*testing.B, context.Context, *server.Server, string, string)) {
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
		b.Run(tc.name, func(b *testing.B) {
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
			bench(b, ctx, srv, tc.bucket, tc.name)
		})
	}
}

func benchmarkFileSpan(totalSize, fileCount, fileIdx int) (start, end int) {
	baseSize := totalSize / fileCount
	remainder := totalSize % fileCount
	start = fileIdx*baseSize + min(fileIdx, remainder)
	end = start + baseSize
	if fileIdx < remainder {
		end++
	}
	return start, end
}

func reportBenchmarkThroughput(b *testing.B, totalBytes int, writeTime, readTime time.Duration) {
	if writeTime > 0 {
		totalMiB := float64(totalBytes*b.N) / float64(1<<20)
		b.ReportMetric(totalMiB/writeTime.Seconds(), "write-MiB/s")
	}
	if readTime > 0 {
		totalMiB := float64(totalBytes*b.N) / float64(1<<20)
		b.ReportMetric(totalMiB/readTime.Seconds(), "read-MiB/s")
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
