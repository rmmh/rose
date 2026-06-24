package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/rmmh/rose/uid"
)

// diskUIDMarker is the file at the root of a disk's directory that records the
// disk's persistent UID. It travels with the physical media, so a disk that is
// physically moved or re-enumerated to a different configured directory keeps
// its identity rather than silently adopting whichever numeric disk_id the
// config now binds to that path.
const diskUIDMarker = "rose_disk_uid"

// diskUIDForRoot returns the persistent UID of the disk mounted at root, reading
// it from the rose_disk_uid marker. A disk with no marker (freshly provisioned)
// gets a new UID written atomically; an existing marker is adopted verbatim.
func diskUIDForRoot(root string) (uid.UID, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return uid.UID{}, fmt.Errorf("create disk dir %q: %w", root, err)
	}
	markerPath := filepath.Join(root, diskUIDMarker)
	data, err := os.ReadFile(markerPath)
	if err == nil {
		u, perr := uid.Parse(strings.TrimSpace(string(data)))
		if perr != nil {
			return uid.UID{}, fmt.Errorf("parse disk uid marker %q: %w", markerPath, perr)
		}
		return u, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return uid.UID{}, fmt.Errorf("read disk uid marker %q: %w", markerPath, err)
	}
	u := uid.New()
	tmp := markerPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(u.String()+"\n"), 0o644); err != nil {
		return uid.UID{}, fmt.Errorf("write disk uid marker %q: %w", markerPath, err)
	}
	if err := os.Rename(tmp, markerPath); err != nil {
		return uid.UID{}, fmt.Errorf("install disk uid marker %q: %w", markerPath, err)
	}
	return u, nil
}

// reconcileDiskRoots rebinds the configured disk roots to catalog disk ids by the
// rose_disk_uid marker each directory carries, making the disk's UID -- not the
// numeric id its mount point is configured as -- the authoritative identity. The
// small integer id stays the internal handle used throughout placement and the
// catalog; this only decides which directory each id resolves to.
//
// A directory whose marker UID is already in the catalog is bound to that disk's
// id, so a relocated or re-enumerated disk is recognized by content. A directory
// with an unknown marker is a fresh disk registered under its configured id. A
// configured root that does not currently exist is left bound to its configured
// id untouched -- unless that id is already served by a relocated disk, in which
// case the stale entry is dropped -- so an unplugged known disk still fails the
// per-disk reachability probe instead of being silently recreated here.
func (s *Server) reconcileDiskRoots(ctx context.Context) error {
	disks, err := s.db.ListDisks(ctx)
	if err != nil {
		return err
	}
	idByUID := make(map[uid.UID]uint32, len(disks))
	idKnown := make(map[uint32]bool, len(disks))
	for _, d := range disks {
		idKnown[d.ID] = true
		if !d.UID.IsZero() {
			idByUID[d.UID] = d.ID
		}
	}

	reconciled := make(map[uint32]string, len(s.diskRoots))
	register := func(id uint32, u uid.UID) error {
		node := s.nodeOf(id)
		if err := s.db.RegisterNode(ctx, node); err != nil {
			return err
		}
		return s.db.RegisterDisk(ctx, id, node, u)
	}

	// Pass 1: directories that exist carry an authoritative marker. Read (or, for a
	// never-provisioned disk, mint) it and bind by UID.
	var missing []uint32
	for configID, path := range s.diskRoots {
		if fi, statErr := os.Stat(path); statErr != nil || !fi.IsDir() {
			missing = append(missing, configID)
			continue
		}
		markerUID, err := diskUIDForRoot(path)
		if err != nil {
			return err
		}
		effectiveID := configID
		if knownID, ok := idByUID[markerUID]; ok {
			effectiveID = knownID
		}
		if other, taken := reconciled[effectiveID]; taken && other != path {
			return fmt.Errorf("disk id %d claimed by two roots %q and %q (uid %s)", effectiveID, other, path, markerUID)
		}
		reconciled[effectiveID] = path
		if err := register(effectiveID, markerUID); err != nil {
			return err
		}
		if effectiveID != configID {
			slog.Info("bound relocated disk to its catalog id by uid",
				"config_id", configID, "disk_id", effectiveID, "uid", markerUID.String(), "path", path)
		}
	}

	// Pass 2: directories that do not exist yet. A configured id already claimed by
	// a relocated disk is an obsolete mount point -- drop it. An id unknown to the
	// catalog is a fresh disk whose directory has not been created: provision it
	// now (dir + marker) so it has a stable identity. An id known to the catalog
	// with a vanished directory is a real loss -- keep it bound so the per-disk
	// reachability probe fails it rather than recreating an empty disk here.
	for _, configID := range missing {
		path := s.diskRoots[configID]
		if _, claimed := reconciled[configID]; claimed {
			continue
		}
		if !idKnown[configID] {
			markerUID, err := diskUIDForRoot(path)
			if err != nil {
				return err
			}
			if err := register(configID, markerUID); err != nil {
				return err
			}
		}
		reconciled[configID] = path
	}

	s.diskRoots = reconciled
	return nil
}
