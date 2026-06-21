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

type transactionPhase uint8

const (
	phaseBegin transactionPhase = iota
	phasePrepare
	phaseSync
	phaseBeforePublish
	phasePublish
	phaseAcknowledge
	phaseDone
)

// Transaction is a cloneable, one-boundary-at-a-time commit state machine.
// Production drives it to completion with Commit; deterministic tests choose
// which transaction's next Step executes.
type Transaction struct {
	ID         string
	Writes     []ShardWrite
	placements []Placement
	phase      transactionPhase
	index      int
}

func (c Coordinator) yield(point Point) error {
	if c.Hook == nil {
		return nil
	}
	return c.Hook(point)
}

// Start validates strict, unique placement before any side effect occurs.
func (c Coordinator) Start(txnID string, writes []ShardWrite) (*Transaction, error) {
	if c.Metadata == nil {
		return nil, errors.New("metadata is required")
	}
	if len(writes) == 0 {
		return nil, errors.New("at least one shard is required")
	}
	seenDisks := make(map[string]struct{}, len(writes))
	seenShards := make(map[int]struct{}, len(writes))
	placements := make([]Placement, 0, len(writes))
	for _, write := range writes {
		if write.Disk == nil {
			return nil, errors.New("shard disk is required")
		}
		if _, ok := seenDisks[write.Disk.ID()]; ok {
			return nil, fmt.Errorf("duplicate disk placement: %s", write.Disk.ID())
		}
		if _, ok := seenShards[write.Shard]; ok {
			return nil, fmt.Errorf("duplicate shard placement: %d", write.Shard)
		}
		seenDisks[write.Disk.ID()] = struct{}{}
		seenShards[write.Shard] = struct{}{}
		placements = append(placements, Placement{DiskID: write.Disk.ID(), Shard: write.Shard})
	}
	return &Transaction{ID: txnID, Writes: append([]ShardWrite(nil), writes...), placements: placements}, nil
}

func (t *Transaction) Done() bool { return t.phase == phaseDone }

// Step executes exactly one deterministic transaction boundary.
func (c Coordinator) Step(ctx context.Context, t *Transaction) error {
	if t == nil || t.Done() {
		return nil
	}
	switch t.phase {
	case phaseBegin:
		if err := c.Metadata.Begin(ctx, t.ID); err != nil {
			return err
		}
		t.phase = phasePrepare
		return c.yield(AfterBegin)
	case phasePrepare:
		write := t.Writes[t.index]
		if err := write.Disk.Prepare(ctx, PreparedRecord{TxnID: t.ID, Shard: write.Shard, Data: write.Data}); err != nil {
			return err
		}
		t.index++
		if t.index == len(t.Writes) {
			t.phase, t.index = phaseSync, 0
		}
		return c.yield(AfterPrepare)
	case phaseSync:
		write := t.Writes[t.index]
		if err := write.Disk.Sync(ctx); err != nil {
			return err
		}
		t.index++
		if t.index == len(t.Writes) {
			t.phase, t.index = phaseBeforePublish, 0
		}
		return c.yield(AfterDiskSync)
	case phaseBeforePublish:
		t.phase = phasePublish
		return c.yield(BeforeMetadataPublish)
	case phasePublish:
		if err := c.Metadata.Publish(ctx, t.ID, t.placements); err != nil {
			return err
		}
		t.phase = phaseAcknowledge
		return c.yield(AfterMetadataPublish)
	case phaseAcknowledge:
		t.phase = phaseDone
		return c.yield(BeforeAcknowledgement)
	default:
		return fmt.Errorf("unknown transaction phase %d", t.phase)
	}
}

// Commit implements strict full-protection publication by driving the same
// stepper used by deterministic simulation to completion. A crash after
// Publish but before acknowledgement is intentionally ambiguous to the caller.
func (c Coordinator) Commit(ctx context.Context, txnID string, writes []ShardWrite) error {
	txn, err := c.Start(txnID, writes)
	if err != nil {
		return err
	}
	for !txn.Done() {
		if err := c.Step(ctx, txn); err != nil {
			return err
		}
	}
	return nil
}
