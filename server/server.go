package server

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rmmh/rose/meta"
	pb "github.com/rmmh/rose/proto"
	"github.com/rmmh/rose/storage"
)

type Server struct {
	pb.UnimplementedRoseServer
	db    *meta.DB
	plogs map[uint32]*storage.Plog
	vlogs map[uint32]*storage.Vlog
	// offlinePlogs records plogs whose backing disk was unreachable at recovery
	// (a failed disk or a disk on a failed node), so their files were never
	// opened. Their vlogs mount with an offlinePlogClient hole that the redundancy
	// layer treats as a missing shard; reprotect regenerates them and a returning
	// node reopens them. Guarded by vlogMu.
	offlinePlogs map[uint32]bool

	// vlogMu guards the vlog/plog maps, the active-vlog pointer, and the disk
	// lifecycle cache below, so placement and commit-durability decisions see a
	// consistent view of which disks are live.
	vlogMu sync.Mutex
	// activeVlogByBucket is the open vlog each bucket currently appends to, keyed
	// by bucket name (the top-level path component). A bucket's writes roll into a
	// fresh vlog when its active one fills or is retired/relocated out from under
	// it; clearActiveVlogLocked drops whichever bucket points at a vlog that a
	// maintenance step is reworking. Guarded by vlogMu.
	activeVlogByBucket map[string]uint32
	// bucketPolicies caches the durable per-bucket protection policy, warmed from
	// the catalog on Recover and updated by SetBucketPolicy. A bucket absent here
	// falls back to meta.DefaultBucketPolicy. Guarded by vlogMu.
	bucketPolicies map[string]meta.BucketPolicy
	dataDir        string
	diskRoots      map[uint32]string
	// diskState caches the lifecycle state of every configured disk, kept in sync
	// with the durable disk catalog. Configured disks start active.
	diskState map[uint32]string
	// diskNodes maps each configured disk to its node fault domain. A disk with no
	// entry is its own node (disk id == node id), the local one-disk-per-node shape
	// the bounded model uses; SetDiskNode groups several disks onto one node.
	diskNodes map[uint32]uint32
	// nodeState caches node liveness, kept in sync with the durable node catalog.
	// A node absent from the map is treated as working; only failed nodes are
	// recorded. A failed node's disks drop out of the live set (diskLiveLocked).
	nodeState map[uint32]string
	// minCopies is the DUPLICATE commit gate: how many live copies a write must
	// land on before it is acknowledged durable (capped at the copies provisioned).
	minCopies int
	// rebalance bounds how aggressively shard counts are evened across active
	// disks: a hysteresis band so minor imbalance is tolerated, a per-pass move
	// cap, and a cooldown between passes. lastRebalance tracks the cooldown and is
	// guarded by vlogMu.
	rebalance         RebalancePolicy
	lastRebalance     time.Time
	maintenanceMu     sync.Mutex
	maintenanceEvery  time.Duration
	maintenanceCancel context.CancelFunc
	handlesMu         sync.Mutex
	handles           map[int64]*FileHandle
	handleCounter     int64
}

// MaxVlogBytes is the 32-bit byte-addressable virtual-log boundary described
// in plan.txt. Writers must roll to a fresh vlog before crossing this limit.
const MaxVlogBytes int64 = 4 << 30

func NewServer(db *meta.DB) *Server {
	s := &Server{
		db:                 db,
		plogs:              make(map[uint32]*storage.Plog),
		vlogs:              make(map[uint32]*storage.Vlog),
		offlinePlogs:       make(map[uint32]bool),
		activeVlogByBucket: make(map[string]uint32),
		bucketPolicies:     make(map[string]meta.BucketPolicy),
		dataDir:            "data",
		diskRoots:          map[uint32]string{1: "data"},
		diskState:          make(map[uint32]string),
		diskNodes:          make(map[uint32]uint32),
		nodeState:          make(map[uint32]string),
		minCopies:          2,
		rebalance:          DefaultRebalancePolicy(),
		maintenanceEvery:   time.Second,
		handles:            make(map[int64]*FileHandle),
	}
	s.resetDiskStates()
	return s
}

// resetDiskStates marks every configured disk active. It is the in-memory
// default before the durable catalog is consulted during Recover.
func (s *Server) resetDiskStates() {
	s.diskState = make(map[uint32]string, len(s.diskRoots))
	for id := range s.diskRoots {
		s.diskState[id] = meta.DiskActive
	}
}

// NewServerWithDataDir is intended for embedding and integration tests that
// need isolated physical-log files without relying on a FUSE mount.
func NewServerWithDataDir(db *meta.DB, dataDir string) *Server {
	s := NewServer(db)
	s.dataDir = dataDir
	s.diskRoots = map[uint32]string{1: dataDir}
	s.resetDiskStates()
	return s
}

// NewServerWithDiskRoots configures independent local storage roots for each
// disk ID. It is the local multi-disk shape used by recovery and placement.
func NewServerWithDiskRoots(db *meta.DB, diskRoots map[uint32]string) *Server {
	s := NewServer(db)
	s.diskRoots = make(map[uint32]string, len(diskRoots))
	for diskID, root := range diskRoots {
		s.diskRoots[diskID] = root
	}
	s.resetDiskStates()
	return s
}

func (s *Server) plogPath(diskID, plogID uint32) string {
	root, ok := s.diskRoots[diskID]
	if !ok {
		root = filepath.Join(s.dataDir, "disk-"+fmt.Sprint(diskID))
	}
	return filepath.Join(root, "plog-"+fmt.Sprint(plogID))
}

// nodeOf returns the node fault domain a disk belongs to. A disk without an
// explicit mapping is its own node (disk id == node id).
func (s *Server) nodeOf(diskID uint32) uint32 {
	if n, ok := s.diskNodes[diskID]; ok {
		return n
	}
	return diskID
}

// SetDiskNode assigns a disk to a node fault domain, grouping it with the other
// disks on that node. It must be called before Recover; disks default to their
// own node. It is the local stand-in for cluster topology: the one-shard-per-node
// fault domain in PlacementAllowed keys off these assignments.
func (s *Server) SetDiskNode(diskID, nodeID uint32) {
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	s.diskNodes[diskID] = nodeID
}

// diskLiveLocked reports whether a disk can currently hold live shards: its disk
// lifecycle state is active and its node is not failed. This is RoseStorage's
// DiskLive (disk_state = active /\ node_state = working): a failed node's disks
// drop out without any disk_state change, so the loss reverses when it returns.
// The caller must hold vlogMu.
func (s *Server) diskLiveLocked(diskID uint32) bool {
	return s.diskState[diskID] == meta.DiskActive && s.nodeState[s.nodeOf(diskID)] != meta.NodeFailed
}

// diskReachableLocked reports whether a disk's plog files can be opened and read.
// Unlike diskLiveLocked, a draining or detached disk is still reachable (its
// files are local and being evacuated); only a failed disk (hardware loss) or a
// disk on a failed node (offline) is unreachable. Recover skips opening plogs on
// unreachable disks and stubs them offline. The caller must hold vlogMu.
func (s *Server) diskReachableLocked(diskID uint32) bool {
	return s.diskState[diskID] != meta.DiskFailed && s.nodeState[s.nodeOf(diskID)] != meta.NodeFailed
}

// activeDiskIDs returns the configured disks currently eligible to receive new
// shards: those that are live (active lifecycle state on a working node).
// Draining, failed, and detached disks, and disks on a failed node, are excluded
// so placement never lands fresh data on a disk that is leaving the cluster or
// temporarily offline. The caller must hold vlogMu.
func (s *Server) activeDiskIDs() []uint32 {
	ids := make([]uint32, 0, len(s.diskRoots))
	for id := range s.diskRoots {
		if s.diskLiveLocked(id) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// distinctNodeDisksLocked returns live disks, at most one per node fault domain,
// so a vlog's shards each land on a different node. With max <= 0 it returns one
// disk for every distinct node. The caller must hold vlogMu.
func (s *Server) distinctNodeDisksLocked(max int) []uint32 {
	seen := make(map[uint32]bool)
	var out []uint32
	for _, id := range s.activeDiskIDs() {
		n := s.nodeOf(id)
		if seen[n] {
			continue
		}
		seen[n] = true
		out = append(out, id)
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

func (s *Server) GetDB() *meta.DB {
	return s.db
}

// Recover rebuilds local plog and vlog clients from persisted metadata. A
// missing locally configured disk fails startup rather than silently exposing
// metadata that cannot be read.
func (s *Server) Recover(ctx context.Context) error {
	// Declare configured disks in the durable catalog (idempotent) and adopt any
	// non-active lifecycle state a prior run persisted, so a disk that was
	// draining or failed before the crash stays out of placement after restart.
	for id := range s.diskRoots {
		node := s.nodeOf(id)
		if err := s.db.RegisterNode(ctx, node); err != nil {
			return err
		}
		if err := s.db.RegisterDisk(ctx, id, node); err != nil {
			return err
		}
	}
	disks, err := s.db.ListDisks(ctx)
	if err != nil {
		return err
	}
	s.resetDiskStates()
	for _, d := range disks {
		if _, ok := s.diskRoots[d.ID]; ok {
			s.diskState[d.ID] = d.State
		}
	}
	// Adopt any persisted node-liveness state so a node that was failed before a
	// restart keeps its disks out of the live set until it is marked working.
	nodes, err := s.db.ListNodes(ctx)
	if err != nil {
		return err
	}
	s.nodeState = make(map[uint32]string)
	for _, n := range nodes {
		if n.State != meta.NodeWorking {
			s.nodeState[n.ID] = n.State
		}
	}
	// Warm the per-bucket protection-policy cache so file writes resume
	// provisioning each bucket's vlogs under its configured scheme after a restart.
	policies, err := s.db.ListBucketPolicies(ctx)
	if err != nil {
		return err
	}
	s.bucketPolicies = make(map[string]meta.BucketPolicy, len(policies))
	for _, p := range policies {
		s.bucketPolicies[p.Name] = p
	}
	s.activeVlogByBucket = make(map[string]uint32)

	plogInfos, err := s.db.ListPlogs(ctx)
	if err != nil {
		return err
	}
	plogByID := make(map[uint32]*storage.Plog, len(plogInfos))
	s.offlinePlogs = make(map[uint32]bool)
	for _, info := range plogInfos {
		if !s.diskReachableLocked(info.DiskID) {
			// A failed disk's bytes are gone and a failed node's disk is offline;
			// either way the file is unreachable. Stub it offline rather than
			// failing recovery on the first dead disk, so its vlog mounts degraded
			// and reprotect (or a returning node) can restore it.
			s.offlinePlogs[info.ID] = true
			continue
		}
		plog, err := storage.OpenPlog(s.plogPath(info.DiskID, info.ID), info.ID)
		if err != nil {
			return fmt.Errorf("recover plog %d on disk %d: %w", info.ID, info.DiskID, err)
		}
		plogByID[info.ID] = plog
	}
	s.plogs = plogByID

	vlogInfos, err := s.db.ListVlogs(ctx)
	if err != nil {
		return err
	}
	vlogs := make(map[uint32]*storage.Vlog, len(vlogInfos))
	for _, info := range vlogInfos {
		vlog, err := s.mountVlogLocked(ctx, info)
		if err != nil {
			return err
		}
		vlogs[info.ID] = vlog
	}
	s.vlogs = vlogs

	// Resume any maintenance work interrupted by the crash/restart.
	jobs, err := s.db.RunningJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		switch job.Kind {
		case meta.JobCompact:
			if err := s.CompactVlog(ctx, job.TargetVlog); err != nil {
				return fmt.Errorf("resume compaction of vlog %d: %w", job.TargetVlog, err)
			}
		case meta.JobDrain:
			if err := s.DrainDisk(ctx, job.TargetDisk); err != nil {
				return fmt.Errorf("resume drain of disk %d: %w", job.TargetDisk, err)
			}
		case meta.JobReprotect:
			if err := s.ReprotectDisk(ctx, job.TargetDisk); err != nil {
				return fmt.Errorf("resume reprotect of disk %d: %w", job.TargetDisk, err)
			}
		case meta.JobReplace:
			if err := s.ReplaceDiskWith(ctx, job.TargetDisk, job.DestDisk); err != nil {
				return fmt.Errorf("resume replace of disk %d: %w", job.TargetDisk, err)
			}
		case meta.JobRebalance:
			if _, err := s.Rebalance(ctx); err != nil {
				return fmt.Errorf("resume rebalance: %w", err)
			}
			if err := s.db.MarkJobDone(ctx, job.ID); err != nil {
				return fmt.Errorf("finish resumed rebalance: %w", err)
			}
		case meta.JobScrubRepair:
			if _, err := s.RepairVlog(ctx, job.TargetVlog); err != nil {
				return fmt.Errorf("resume scrub-repair of vlog %d: %w", job.TargetVlog, err)
			}
		}
	}
	s.startMaintenanceDriver()
	return nil
}

// CloseStorage stops the maintenance driver and closes every open plog file
// handle, releasing the Server's hold on its disk roots. It is the graceful
// shutdown to call before discarding a Server — modeling a process restart, or
// unmounting the disks underneath it — so the abandoned Server's file
// descriptors do not keep the disks busy or leak across repeated restarts. The
// metadata DB is the caller's to close; CloseStorage does not touch it.
func (s *Server) CloseStorage() {
	s.StopMaintenanceDriver()
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	for id, p := range s.plogs {
		_ = p.Commit()
		_ = p.Close()
		delete(s.plogs, id)
	}
	s.vlogs = make(map[uint32]*storage.Vlog)
}

// bucketOf returns the bucket a path belongs to: its top-level directory (the
// component the README calls a bucket). A bare file at the root has no top-level
// directory and belongs to the root bucket "", which carries the default policy.
func bucketOf(path string) string {
	for len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return ""
}

// SetBucketPolicy records a bucket's protection policy durably and updates the
// in-memory cache. New files in the bucket are written under this scheme; vlogs
// already provisioned for it are unaffected. It is the operator knob that makes
// the file path write EC or N-way DUPLICATE instead of the default mirror.
func (s *Server) SetBucketPolicy(ctx context.Context, p meta.BucketPolicy) error {
	if err := s.db.SetBucketPolicy(ctx, p); err != nil {
		return err
	}
	s.vlogMu.Lock()
	defer s.vlogMu.Unlock()
	s.bucketPolicies[p.Name] = p
	// Drop the bucket's active vlog so the next append provisions one under the
	// new scheme rather than continuing to append under the old one.
	delete(s.activeVlogByBucket, p.Name)
	return nil
}

// bucketPolicyLocked returns the protection policy for a bucket, falling back to
// the default mirror when the bucket has no explicit policy. The caller must
// hold vlogMu.
func (s *Server) bucketPolicyLocked(bucket string) meta.BucketPolicy {
	if p, ok := s.bucketPolicies[bucket]; ok {
		return p
	}
	return meta.DefaultBucketPolicy(bucket)
}

// clearActiveVlogLocked drops any bucket whose active vlog is vlogID, so a
// maintenance step retiring or relocating that vlog does not leave a bucket
// appending into a vlog that is being reworked. The caller must hold vlogMu.
func (s *Server) clearActiveVlogLocked(vlogID uint32) {
	for bucket, id := range s.activeVlogByBucket {
		if id == vlogID {
			delete(s.activeVlogByBucket, bucket)
		}
	}
}

// provisionVlogLocked creates a vlog, its backing plogs across the configured
// disks, and the in-memory clients, registering everything. The caller must
// hold vlogMu.
func (s *Server) provisionVlogLocked(ctx context.Context, scheme string, dataShards, parityShards int) (uint32, *storage.Vlog, error) {
	id, err := s.db.MakeVlog(ctx, scheme, int32(dataShards), int32(parityShards))
	if err != nil {
		return 0, nil, err
	}
	// One disk per node fault domain, so no two shards/copies of this vlog share
	// a node (PlacementAllowed's NodeLevelDurability).
	diskIDs := s.distinctNodeDisksLocked(0)
	if len(diskIDs) == 0 {
		return 0, nil, fmt.Errorf("no active disks configured")
	}
	clientCount := 1
	switch scheme {
	case "DUPLICATE":
		clientCount = len(diskIDs)
	case "EC":
		clientCount = dataShards + parityShards
		if clientCount == 0 || clientCount > len(diskIDs) {
			return 0, nil, fmt.Errorf("EC vlog needs 1..%d distinct-node disks, got %d", len(diskIDs), clientCount)
		}
	}
	clients := make([]storage.PlogClient, 0, clientCount)
	for shard := 0; shard < clientCount; shard++ {
		diskID := diskIDs[shard]
		plogID, err := s.db.MakePlog(ctx, diskID)
		if err != nil {
			return 0, nil, err
		}
		if err := s.db.AssignPlogToVlog(ctx, id, shard, plogID); err != nil {
			return 0, nil, err
		}
		plog, err := storage.OpenPlog(s.plogPath(diskID, plogID), plogID)
		if err != nil {
			return 0, nil, fmt.Errorf("open plog %d: %w", plogID, err)
		}
		s.plogs[plogID] = plog
		clients = append(clients, &localPlogClient{plog: plog})
	}
	vlog, err := storage.NewVlog(id, scheme, dataShards, parityShards, clients, 0)
	if err != nil {
		return 0, nil, fmt.Errorf("create vlog in memory: %w", err)
	}
	s.vlogs[id] = vlog
	return id, vlog, nil
}

// plogClientLocked returns the in-memory client for a shard's backing plog: the
// live local client when its file is open, or an offline stub when the plog sits
// on an unreachable disk (recorded in offlinePlogs). A plog that is neither open
// nor known-offline is a genuine inconsistency and errors. The caller must hold
// vlogMu.
func (s *Server) plogClientLocked(plogID uint32) (storage.PlogClient, error) {
	if p, ok := s.plogs[plogID]; ok {
		return &localPlogClient{plog: p}, nil
	}
	if s.offlinePlogs[plogID] {
		return offlinePlogClient{plogID: plogID}, nil
	}
	return nil, fmt.Errorf("references missing plog %d", plogID)
}

// mountVlogLocked builds a vlog's in-memory client set from current placement
// metadata, stubbing any shard on an unreachable disk offline. The caller must
// hold vlogMu.
func (s *Server) mountVlogLocked(ctx context.Context, info meta.VlogInfo) (*storage.Vlog, error) {
	mappings, err := s.db.ListVlogPlogs(ctx, info.ID)
	if err != nil {
		return nil, err
	}
	clients := make([]storage.PlogClient, len(mappings))
	for index, mapping := range mappings {
		if mapping.ShardIndex != index {
			return nil, fmt.Errorf("vlog %d has non-contiguous shard mapping", info.ID)
		}
		client, err := s.plogClientLocked(mapping.PlogID)
		if err != nil {
			return nil, fmt.Errorf("mount vlog %d: %w", info.ID, err)
		}
		clients[index] = client
	}
	vlog, err := storage.NewVlog(info.ID, info.ProtectionScheme, int(info.DataShards), int(info.ParityShards), clients, info.Length)
	if err != nil {
		return nil, fmt.Errorf("mount vlog %d: %w", info.ID, err)
	}
	return vlog, nil
}

type localPlogClient struct {
	plog *storage.Plog
}

func (c *localPlogClient) Write(ctx context.Context, txnID int64, data []byte) (int64, error) {
	return c.plog.Write(txnID, data)
}

func (c *localPlogClient) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	return c.plog.Read(offset, length)
}

func (c *localPlogClient) Commit(ctx context.Context, txnID int64) error {
	return c.plog.Commit()
}

func (c *localPlogClient) Scrub() (storage.ScrubResult, error) {
	return c.plog.Scrub()
}

// GC reclaims unreferenced chunk metadata (refcount zero) and reports how many
// chunks were collected. The chunks' log bytes become orphan data eligible for
// later segment compaction.
func (s *Server) GC(ctx context.Context) (int, error) {
	collected, err := s.db.GCChunks(ctx)
	if err != nil {
		return 0, err
	}
	return len(collected), nil
}

// VlogScrub reports a scrub of one mounted virtual log.
type VlogScrub struct {
	VlogID uint32
	Shards []storage.ShardScrub
}

// Scrub validates every mounted vlog's backing shards in order. It is the bulk
// sequential integrity pass the README describes; callers can use the reported
// corrupt shards to schedule repair from surviving redundancy.
func (s *Server) Scrub() ([]VlogScrub, error) {
	out := make([]VlogScrub, 0, len(s.vlogs))
	for id, vlog := range s.vlogs {
		shards, err := vlog.Scrub()
		if err != nil {
			return nil, err
		}
		out = append(out, VlogScrub{VlogID: id, Shards: shards})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VlogID < out[j].VlogID })
	return out, nil
}

// offlinePlogClient stands in for a shard whose backing plog is unreachable: its
// disk is failed or on a failed node, so Recover never opened the file. Every
// operation errors, which the redundancy layer treats as a missing shard (EC
// reconstructs, DUPLICATE falls through) and reprotect regenerates from the
// surviving copies. It lets a vlog mount with a hole instead of failing recovery
// on the first dead disk.
type offlinePlogClient struct {
	plogID uint32
}

func (c offlinePlogClient) err() error {
	return fmt.Errorf("plog %d is offline (disk unreachable)", c.plogID)
}

func (c offlinePlogClient) Write(ctx context.Context, txnID int64, data []byte) (int64, error) {
	return 0, c.err()
}

func (c offlinePlogClient) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	return nil, c.err()
}

func (c offlinePlogClient) Commit(ctx context.Context, txnID int64) error {
	return c.err()
}

func (c offlinePlogClient) Scrub() (storage.ScrubResult, error) {
	return storage.ScrubResult{}, c.err()
}

// Ensure the plog clients implement storage.PlogClient
var (
	_ storage.PlogClient = &localPlogClient{}
	_ storage.PlogClient = offlinePlogClient{}
)
