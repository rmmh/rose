package server_test

// End-to-end chaos harness for Rose. This file builds a single-process,
// RAM-disk-backed cluster — one Server owning several node fault domains, each
// with its own disks — fronted by an in-process bufconn gRPC server, and (in
// later commits) drives a steady read/write/delete workload through it while a
// fault injector storms the cluster with disk/node/bitrot damage.
//
// A "node" in Rose is a fault domain inside one Server (diskNodes), not a
// separate process: one Server owns all disks, node failure is SetNodeState, and
// a process restart is modeled by rebuilding the Server on the same disk roots +
// metadata DB and calling Recover. The harness mirrors that exactly.
//
// This commit lands the harness and a smoke test; the workload engine, oracle,
// chaos orchestrator, and invariant checks arrive in subsequent commits. The
// heavy chaos test is gated on ROSE_CHAOS=1; the smoke test runs by default.

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Bucket names the harness configures, one per protection scheme under test.
const (
	bucketEC     = "ec"     // EC 3+1: four shards across four nodes
	bucketMirror = "mirror" // DUPLICATE: one copy per node fault domain
)

// ramVolumeSeq makes each macOS RAM volume name unique within a process, so two
// volumes never collide on /Volumes and every one ejects cleanly.
var ramVolumeSeq atomic.Int64

// ramDiskRoot returns a directory backed by RAM when the platform supports it,
// falling back to t.TempDir() otherwise, and registers cleanup. On Linux it is a
// subdirectory of the /dev/shm tmpfs; on macOS it is a freshly attached
// hdiutil/diskutil RAM volume; everywhere else (or on any failure, or when
// ROSE_NO_RAMDISK is set) it is an ordinary temp dir. The returned path is a
// fresh empty directory the caller owns.
func ramDiskRoot(t *testing.T, sizeMB int) string {
	t.Helper()
	if os.Getenv("ROSE_NO_RAMDISK") != "" {
		return t.TempDir()
	}
	switch runtime.GOOS {
	case "linux":
		if root, ok := shmRoot(t); ok {
			return root
		}
	case "darwin":
		if root, ok := macRAMDisk(t, sizeMB); ok {
			return root
		}
	}
	return t.TempDir()
}

// shmRoot makes a private subdir under /dev/shm (Linux tmpfs) and cleans it up.
func shmRoot(t *testing.T) (string, bool) {
	t.Helper()
	base := "/dev/shm"
	info, err := os.Stat(base)
	if err != nil || !info.IsDir() {
		return "", false
	}
	dir, err := os.MkdirTemp(base, "rose-chaos-")
	if err != nil {
		return "", false
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir, true
}

// macRAMDisk attaches a RAM-backed HFS+ volume via hdiutil/diskutil and ejects
// it on cleanup. 512-byte sectors, so sizeMB MiB needs sizeMB*2048 sectors.
func macRAMDisk(t *testing.T, sizeMB int) (string, bool) {
	t.Helper()
	if sizeMB <= 0 {
		sizeMB = 256
	}
	sectors := sizeMB * 2048
	out, err := exec.Command("hdiutil", "attach", "-nomount", fmt.Sprintf("ram://%d", sectors)).Output()
	if err != nil {
		return "", false
	}
	dev := strings.Fields(string(out))
	if len(dev) == 0 || !strings.HasPrefix(dev[0], "/dev/") {
		return "", false
	}
	device := dev[0]
	name := fmt.Sprintf("rose-chaos-%d-%d", os.Getpid(), ramVolumeSeq.Add(1))
	if err := exec.Command("diskutil", "erasevolume", "HFS+", name, device).Run(); err != nil {
		_ = exec.Command("hdiutil", "detach", device).Run()
		return "", false
	}
	t.Cleanup(func() { _ = exec.Command("diskutil", "eject", device).Run() })
	return filepath.Join("/Volumes", name), true
}

// chaosCluster is a single-process Rose cluster on RAM-backed disks, fronted by
// an in-process bufconn gRPC server. It owns the metadata DB and disk roots so a
// restart can rebuild the Server against the same durable state.
type chaosCluster struct {
	t        *testing.T
	metaPath string            // durable on-disk metadata DB (survives restart)
	roots    map[uint32]string // diskID -> storage root (RAM-backed)
	diskNode map[uint32]uint32 // diskID -> node fault domain
	nodes    int
	disksPer int

	mu    sync.Mutex // guards srv/grpc/lis across a restart swap
	db    *meta.DB
	srv   *server.Server
	grpc  *grpc.Server
	lis   *bufconn.Listener
	conns []*grpc.ClientConn
}

// newChaosCluster builds a cluster with nodes×disksPerNode RAM-backed disks,
// recovers it, and configures the EC and mirror buckets. Disk IDs are assigned
// densely (1..N*M) and mapped to node fault domains before Recover so placement
// honors NodeLevelDurability. EC 3+1 needs four distinct nodes; the caller is
// expected to pass nodes >= 4.
func newChaosCluster(t *testing.T, nodes, disksPerNode int) *chaosCluster {
	t.Helper()
	if nodes < 4 {
		t.Fatalf("chaos cluster needs >= 4 nodes for EC 3+1, got %d", nodes)
	}
	metaDir := t.TempDir() // metadata DB lives on a normal disk so it survives restart
	c := &chaosCluster{
		t:        t,
		metaPath: filepath.Join(metaDir, "meta.db"),
		roots:    map[uint32]string{},
		diskNode: map[uint32]uint32{},
		nodes:    nodes,
		disksPer: disksPerNode,
	}

	// One RAM volume holds every disk as a subdirectory. Node fault domains are
	// modeled by the diskNode map (this is a single process; physically splitting
	// the volumes would buy nothing), so one volume keeps setup and teardown to a
	// single attach/eject.
	base := ramDiskRoot(t, 256)
	diskID := uint32(1)
	for node := uint32(1); node <= uint32(nodes); node++ {
		for d := 0; d < disksPerNode; d++ {
			root := filepath.Join(base, fmt.Sprintf("disk-%d", diskID))
			if err := os.MkdirAll(root, 0o755); err != nil {
				t.Fatal(err)
			}
			c.roots[diskID] = root
			c.diskNode[diskID] = node
			diskID++
		}
	}

	c.start()

	ctx := context.Background()
	if err := c.srv.SetBucketPolicy(ctx, meta.BucketPolicy{Name: bucketEC, ProtectionScheme: "EC", DataShards: 3, ParityShards: 1}); err != nil {
		t.Fatal(err)
	}
	// DUPLICATE mirrors across every node fault domain (one copy per node), so on
	// a four-node cluster the mirror bucket holds four copies.
	if err := c.srv.SetBucketPolicy(ctx, meta.BucketPolicy{Name: bucketMirror, ProtectionScheme: "DUPLICATE", DataShards: 1, ParityShards: 0}); err != nil {
		t.Fatal(err)
	}
	return c
}

// start opens the metadata DB, builds the Server on the configured roots, wires
// the disk->node map, recovers, and serves it over a fresh bufconn listener. The
// caller holds no lock (start is called from the constructor and restart).
func (c *chaosCluster) start() {
	c.t.Helper()
	db, err := meta.Open(c.metaPath)
	if err != nil {
		c.t.Fatal(err)
	}
	srv := server.NewServerWithDiskRoots(db, c.roots)
	for diskID, node := range c.diskNode {
		srv.SetDiskNode(diskID, node)
	}
	if err := srv.Recover(context.Background()); err != nil {
		c.t.Fatal(err)
	}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterRoseServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	c.mu.Lock()
	c.db, c.srv, c.grpc, c.lis = db, srv, gs, lis
	c.mu.Unlock()
}

// server returns the live Server for direct control-plane calls (Scrub, drain,
// reprotect, SetNodeState, ScrubAndRepair, ...). It is swapped on restart.
func (c *chaosCluster) server() *server.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.srv
}

// client opens a new in-process gRPC client. Its dialer reads the cluster's
// current listener at dial time, so a client created before a restart redials
// the rebuilt server on its next reconnect.
func (c *chaosCluster) client() pb.RoseClient {
	c.t.Helper()
	conn, err := grpc.NewClient("passthrough:///rose",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			c.mu.Lock()
			lis := c.lis
			c.mu.Unlock()
			return lis.Dial()
		}))
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.conns = append(c.conns, conn)
	c.mu.Unlock()
	return pb.NewRoseClient(conn)
}

// restart tears down the Server and gRPC listener and rebuilds them on the same
// disk roots and metadata DB, exercising crash recovery (Recover replays running
// maintenance jobs and remounts every vlog). Committed data must survive it.
func (c *chaosCluster) restart() {
	c.t.Helper()
	c.mu.Lock()
	srv, gs, db := c.srv, c.grpc, c.db
	c.mu.Unlock()

	gs.Stop()
	srv.CloseStorage()
	if err := db.Close(); err != nil {
		c.t.Fatal(err)
	}
	c.start()
}

// close tears down clients, the gRPC server, and the metadata DB. RAM-disk roots
// are ejected by their own t.Cleanup hooks.
func (c *chaosCluster) close() {
	c.mu.Lock()
	conns, srv, gs, db := c.conns, c.srv, c.grpc, c.db
	c.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	if gs != nil {
		gs.Stop()
	}
	if srv != nil {
		srv.CloseStorage()
	}
	if db != nil {
		_ = db.Close()
	}
}

// TestChaosClusterSmoke brings up the RAM-disk cluster, writes and reads back a
// file in each protection bucket through two in-process clients, and confirms
// committed data survives a process restart + Recover. It is the harness shake-
// out the workload and fault injector build on; it runs in the default suite.
func TestChaosClusterSmoke(t *testing.T) {
	c := newChaosCluster(t, 4, 1)
	defer c.close()

	clientA, clientB := c.client(), c.client()

	ecData := randomBytes(t, 250_000, 1)
	mirrorData := randomBytes(t, 130_000, 2)

	writeFile(t, clientA, "/ec/alpha", ecData)
	writeFile(t, clientB, "/mirror/beta", mirrorData)

	if got := readFile(t, clientA, "/ec/alpha", len(ecData)); !bytes.Equal(got, ecData) {
		t.Fatal("EC bucket read-back mismatch before restart")
	}
	if got := readFile(t, clientB, "/mirror/beta", len(mirrorData)); !bytes.Equal(got, mirrorData) {
		t.Fatal("mirror bucket read-back mismatch before restart")
	}

	// Both schemes must be reachable end-to-end: confirm the EC bucket actually
	// provisioned an EC 3+1 vlog and the mirror bucket a multi-copy DUPLICATE.
	assertSchemePresent(t, c, "EC")
	assertSchemePresent(t, c, "DUPLICATE")

	c.restart()

	if got := readFile(t, clientA, "/ec/alpha", len(ecData)); !bytes.Equal(got, ecData) {
		t.Fatal("EC bucket read-back mismatch after restart")
	}
	if got := readFile(t, clientB, "/mirror/beta", len(mirrorData)); !bytes.Equal(got, mirrorData) {
		t.Fatal("mirror bucket read-back mismatch after restart")
	}
}

// assertSchemePresent fails unless some provisioned vlog uses the given scheme.
func assertSchemePresent(t *testing.T, c *chaosCluster, scheme string) {
	t.Helper()
	vlogs, err := c.server().GetDB().ListVlogs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vlogs {
		if v.ProtectionScheme == scheme {
			return
		}
	}
	t.Fatalf("no %s vlog provisioned", scheme)
}

// readFile opens path and returns its first n bytes.
func readFile(t *testing.T, client pb.RoseClient, path string, n int) []byte {
	t.Helper()
	ctx := context.Background()
	open, err := client.Open(ctx, &pb.OpenRequest{Path: path})
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	read, err := client.Read(ctx, &pb.ReadRequest{Handle: open.GetHandle(), Offset: 0, Length: int64(n)})
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return read.GetBuffer()
}

// randomBytes returns n deterministic pseudo-random bytes for the given seed.
func randomBytes(t *testing.T, n int, seed int64) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.New(rand.NewSource(seed)).Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}
