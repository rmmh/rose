package main

import (
	"flag"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"

	rosefuse "github.com/rmmh/rose/fuse"
	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
)

var (
	mountPoint = flag.String("mount", "", "Mount point for FUSE")
	metaDir    = flag.String("metadir", "", "Directory for SQLite metadata storage")
	dataDirs   = flag.String("datadirs", "", "Comma-separated list of directories for physical logs")
	rpcAddr    = flag.String("rpc", ":50051", "RPC listen address")
)

func main() {
	flag.Parse()

	if *mountPoint == "" || *metaDir == "" || *dataDirs == "" {
		log.Fatalf("Missing required arguments. Usage: ./rose --mount <dir> --metadir <dir> --datadirs <dir1,dir2>")
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// Initialize Metadata DB
	metaPath := *metaDir + "/rose.db"
	db, err := meta.Open(metaPath)
	if err != nil {
		log.Fatalf("Failed to open metadata db: %v", err)
	}
	defer db.Close()

	// Parse Data Directories
	dirs := strings.Split(*dataDirs, ",")
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create data directory %s: %v", dir, err)
		}
	}

	// Initialize gRPC Server
	lis, err := net.Listen("tcp", *rpcAddr)
	if err != nil {
		log.Fatalf("Failed to listen for RPC: %v", err)
	}

	grpcServer := grpc.NewServer()
	roseServer := server.NewServer(db)
	pb.RegisterRoseServer(grpcServer, roseServer)

	go func() {
		log.Printf("Starting gRPC server on %s", *rpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// Initialize FUSE API Client (connects back to its own gRPC server for simplicity,
	// though it bypasses network if we pass in roseServer directly. Let's pass the server locally for this single-binary pass.)
	fuseRoot := rosefuse.NewRoseRoot(roseServer)

	serverOptions := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug: true,
		},
	}

	log.Printf("Mounting FUSE on %s...", *mountPoint)
	os.MkdirAll(*mountPoint, 0755)
	fuseServer, err := fs.Mount(*mountPoint, fuseRoot, serverOptions)
	if err != nil {
		log.Fatalf("Mount FUSE failed: %v", err)
	}

	// Wait for termination
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case <-stop:
		log.Println("Received termination signal, shutting down...")
	}

	// Unmount and clean up
	fuseServer.Unmount()
	grpcServer.GracefulStop()
	log.Println("Shutdown complete.")
}
