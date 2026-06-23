package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/net/webdav"
	"google.golang.org/grpc"

	rosefuse "github.com/rmmh/rose/fuse"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
	rosewebdav "github.com/rmmh/rose/webdav"
)

var (
	mountPoint = flag.String("mount", "", "Mount point for FUSE (optional)")
	metaDir    = flag.String("metadir", "", "Directory for SQLite metadata storage")
	dataDirs   = flag.String("datadirs", "", "Comma-separated list of directories for physical logs")
	rpcAddr    = flag.String("rpc", ":50051", "RPC listen address")
	webdavAddr = flag.String("webdav", "", "WebDAV listen address (e.g. :8080); empty disables it")
	protection = flag.String("protection", "", "Default protection for new buckets: \"N\" for N-way duplication, or \"N+K\" for erasure coding with N data and K parity shards (e.g. 3 or 3+2). Empty keeps the built-in 2-copy mirror.")
	debug      = flag.Bool("debug", false, "Verbose logging: per-operation FUSE tracing and debug-level logs. Off by default; the per-op trace noticeably slows the write path.")
)

// parseProtection turns a --protection value into a default bucket policy. A bare
// "N" is N-way duplication; "N+K" is erasure coding with N data and K parity
// shards. An empty string returns ok=false, leaving the built-in default.
func parseProtection(s string) (meta.BucketPolicy, bool, error) {
	if s == "" {
		return meta.BucketPolicy{}, false, nil
	}
	if data, parity, found := strings.Cut(s, "+"); found {
		n, err := strconv.Atoi(data)
		if err != nil || n < 1 {
			return meta.BucketPolicy{}, false, fmt.Errorf("protection %q: data shards must be a positive integer", s)
		}
		k, err := strconv.Atoi(parity)
		if err != nil || k < 1 {
			return meta.BucketPolicy{}, false, fmt.Errorf("protection %q: parity shards must be a positive integer", s)
		}
		return meta.BucketPolicy{ProtectionScheme: "EC", DataShards: n, ParityShards: k}, true, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return meta.BucketPolicy{}, false, fmt.Errorf("protection %q: copies must be a positive integer or N+K", s)
	}
	return meta.BucketPolicy{ProtectionScheme: "DUPLICATE", DataShards: n}, true, nil
}

func main() {
	flag.Parse()

	if *metaDir == "" || *dataDirs == "" {
		log.Fatalf("Missing required arguments. Usage: ./rose --metadir <dir> --datadirs <dir1,dir2> [--mount <dir>] [--webdav :8080]")
	}

	logLevel := slog.LevelInfo
	if *debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel})))

	// Initialize Metadata DB
	metaPath := *metaDir + "/rose.db"
	db, err := meta.Open(metaPath)
	if err != nil {
		log.Fatalf("Failed to open metadata db: %v", err)
	}
	defer db.Close()

	// Parse Data Directories
	dirs := strings.Split(*dataDirs, ",")
	diskRoots := make(map[uint32]string, len(dirs))
	for index, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create data directory %s: %v", dir, err)
		}
		diskRoots[uint32(index+1)] = dir
	}

	// Initialize gRPC Server
	lis, err := net.Listen("tcp", *rpcAddr)
	if err != nil {
		log.Fatalf("Failed to listen for RPC: %v", err)
	}

	grpcServer := grpc.NewServer()
	roseServer := server.NewServerWithDiskRoots(db, diskRoots)
	if pol, ok, err := parseProtection(*protection); err != nil {
		log.Fatal(err)
	} else if ok {
		roseServer.SetDefaultProtection(pol)
		log.Printf("Default protection: %s", *protection)
	}
	if err := roseServer.Recover(context.Background()); err != nil {
		log.Fatal("recover storage:", err)
	}
	pb.RegisterRoseServer(grpcServer, roseServer)

	go func() {
		log.Printf("Starting gRPC server on %s", *rpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// Optionally serve WebDAV over the same server: a userspace mount path that
	// needs no kernel extension (mount_webdav on macOS, davfs/Explorer elsewhere).
	var webdavServer *http.Server
	if *webdavAddr != "" {
		handler := &webdav.Handler{
			FileSystem: rosewebdav.New(roseServer),
			LockSystem: webdav.NewMemLS(),
		}
		webdavServer = &http.Server{Addr: *webdavAddr, Handler: handler}
		go func() {
			log.Printf("Starting WebDAV server on %s", *webdavAddr)
			if err := webdavServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("WebDAV server failed: %v", err)
			}
		}()
	}

	// Optionally mount a FUSE filesystem backed by the same server.
	var fuseServer *fuse.Server
	if *mountPoint != "" {
		fuseRoot := rosefuse.NewRoseRoot(roseServer)
		serverOptions := &fs.Options{
			MountOptions: fuse.MountOptions{
				FsName: "rose",
				Debug:  *debug,
				// macFUSE otherwise probes AppleDouble (._*) sidecars and xattrs on
				// every op, emitting macFUSE-private opcodes go-fuse does not
				// implement (surfacing as spurious I/O errors). No-ops on Linux.
				Options: []string{"noappledouble", "noapplexattr"},
			},
		}
		log.Printf("Mounting FUSE on %s...", *mountPoint)
		os.MkdirAll(*mountPoint, 0755)
		fuseServer, err = fs.Mount(*mountPoint, fuseRoot, serverOptions)
		if err != nil {
			log.Fatalf("Mount FUSE failed: %v", err)
		}
	}

	if *mountPoint == "" && *webdavAddr == "" {
		log.Printf("No client mount enabled (--mount and --webdav are unset); serving gRPC only on %s", *rpcAddr)
	}

	// Wait for termination
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case <-stop:
		log.Println("Received termination signal, shutting down...")
	}

	// Unmount and clean up
	if fuseServer != nil {
		fuseServer.Unmount()
	}
	if webdavServer != nil {
		webdavServer.Close()
	}
	grpcServer.GracefulStop()
	log.Println("Shutdown complete.")
}
