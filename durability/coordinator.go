// Package durability contains the transaction ordering shared by production
// storage adapters and deterministic durability simulation.
package durability

import (
	"context"
	"errors"
	"fmt"
)

var ErrInjectedCrash = errors.New("injected crash")

type Point string

const (
	AfterBegin            Point = "after-begin"
	AfterPrepare          Point = "after-prepare"
	AfterDiskSync         Point = "after-disk-sync"
	BeforeMetadataPublish Point = "before-metadata-publish"
	AfterMetadataPublish  Point = "after-metadata-publish"
	BeforeAcknowledgement Point = "before-acknowledgement"
)

// Hook is a deterministic scheduling/fault boundary. Production uses a nil
// hook; simulations may return ErrInjectedCrash at any boundary.
type Hook func(Point) error

type PreparedRecord struct {
	TxnID string
	Shard int
	Data  []byte
}

type Disk interface {
	ID() string
	Prepare(context.Context, PreparedRecord) error
	Sync(context.Context) error
}

type Placement struct {
	DiskID string
	Shard  int
}

// Metadata is the single authoritative publication point. Publish must be an
// atomic, durable SQLite transaction in the production adapter.
type Metadata interface {
	Begin(context.Context, string) error
	Publish(context.Context, string, []Placement) error
}

type ShardWrite struct {
	Disk  Disk
	Shard int
	Data  []byte
}

type Coordinator struct {
	Metadata Metadata
	Hook     Hook
}

func (c Coordinator) yield(point Point) error {
	if c.Hook == nil {
		return nil
	}
	return c.Hook(point)
}

// Commit implements strict full-protection publication. Every shard is first
// prepared and fsynced on a distinct disk; only then may metadata become
// visible. A crash after Publish but before acknowledgement is intentionally
// ambiguous to the caller and must be retried with the same transaction ID.
func (c Coordinator) Commit(ctx context.Context, txnID string, writes []ShardWrite) error {
	if c.Metadata == nil {
		return errors.New("metadata is required")
	}
	if len(writes) == 0 {
		return errors.New("at least one shard is required")
	}
	seenDisks := make(map[string]struct{}, len(writes))
	seenShards := make(map[int]struct{}, len(writes))
	placements := make([]Placement, 0, len(writes))
	for _, write := range writes {
		if write.Disk == nil {
			return errors.New("shard disk is required")
		}
		if _, ok := seenDisks[write.Disk.ID()]; ok {
			return fmt.Errorf("duplicate disk placement: %s", write.Disk.ID())
		}
		if _, ok := seenShards[write.Shard]; ok {
			return fmt.Errorf("duplicate shard placement: %d", write.Shard)
		}
		seenDisks[write.Disk.ID()] = struct{}{}
		seenShards[write.Shard] = struct{}{}
		placements = append(placements, Placement{DiskID: write.Disk.ID(), Shard: write.Shard})
	}

	if err := c.Metadata.Begin(ctx, txnID); err != nil {
		return err
	}
	if err := c.yield(AfterBegin); err != nil {
		return err
	}
	for _, write := range writes {
		if err := write.Disk.Prepare(ctx, PreparedRecord{TxnID: txnID, Shard: write.Shard, Data: write.Data}); err != nil {
			return err
		}
		if err := c.yield(AfterPrepare); err != nil {
			return err
		}
	}
	for _, write := range writes {
		if err := write.Disk.Sync(ctx); err != nil {
			return err
		}
		if err := c.yield(AfterDiskSync); err != nil {
			return err
		}
	}
	if err := c.yield(BeforeMetadataPublish); err != nil {
		return err
	}
	if err := c.Metadata.Publish(ctx, txnID, placements); err != nil {
		return err
	}
	if err := c.yield(AfterMetadataPublish); err != nil {
		return err
	}
	return c.yield(BeforeAcknowledgement)
}
