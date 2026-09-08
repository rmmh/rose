// Package durability contains the publication ordering used by the server and
// deterministic scheduling tests. Storage and catalog effects belong to adapters.
package durability

import "context"

// Publication supplies effects under one caller-owned namespace, topology and
// pin ownership interval. Sync returns the exact durable prefix, never a later
// sample of an append cursor. Verify resolves and checks every final placement.
// Publish is the atomic namespace/result/root transaction. An adapter must keep
// verification valid until Publish completes.
type Publication interface {
	Admit(context.Context) ([]uint32, error)
	Sync(context.Context, uint32) (int64, error)
	RecordPrefix(context.Context, uint32, int64) error
	Verify(context.Context) error
	Publish(context.Context) error
}

type Point string

const (
	ShardsSynced       Point = "shards-synced"
	PrefixRecorded     Point = "prefix-recorded"
	PlacementsVerified Point = "placements-verified"
	NamespacePublished Point = "namespace-published"
)

type Hook func(Point) error

type phase uint8

const (
	admit phase = iota
	syncPrefix
	recordPrefix
	verify
	publish
	done
)

// Transaction stores protocol control state only. Copying it does not clone the
// adapter's disk/catalog state or its ownership; a simulator must clone those too.
// An error terminates this invocation. Recovery/retry starts a new transaction
// from durable catalog state, including ambiguous publication outcomes.
type Transaction struct {
	phase  phase
	vlogs  []uint32
	index  int
	prefix int64
	err    error
}

func (t *Transaction) Done() bool { return t.phase == done }

// Coordinator executes the same protocol in production and scheduled tests.
// Its zero-valued Transaction starts at admission; each Step performs one effect.
type Coordinator struct {
	Publication Publication
	Hook        Hook
}

func (c Coordinator) Step(ctx context.Context, t *Transaction) error {
	if t.err != nil {
		return t.err
	}
	if t.Done() {
		return nil
	}
	var point Point
	switch t.phase {
	case admit:
		var ids []uint32
		ids, t.err = c.Publication.Admit(ctx)
		if t.err == nil {
			t.vlogs = append([]uint32(nil), ids...)
			t.phase = syncPrefix
			if len(ids) == 0 {
				t.phase = verify
			}
		}
	case syncPrefix:
		t.prefix, t.err = c.Publication.Sync(ctx, t.vlogs[t.index])
		if t.err == nil {
			t.phase, point = recordPrefix, ShardsSynced
		}
	case recordPrefix:
		t.err = c.Publication.RecordPrefix(ctx, t.vlogs[t.index], t.prefix)
		if t.err == nil {
			t.index++
			t.phase, point = syncPrefix, PrefixRecorded
			if t.index == len(t.vlogs) {
				t.phase = verify
			}
		}
	case verify:
		t.err = c.Publication.Verify(ctx)
		if t.err == nil {
			t.phase, point = publish, PlacementsVerified
		}
	case publish:
		t.err = c.Publication.Publish(ctx)
		if t.err == nil {
			t.phase, point = done, NamespacePublished
		}
	}
	if t.err == nil && point != "" && c.Hook != nil {
		t.err = c.Hook(point)
	}
	return t.err
}

// Commit drives the stepper while the caller retains the adapter's ownership.
// Success means publication, not that a client has received its response.
func (c Coordinator) Commit(ctx context.Context) error {
	t := new(Transaction)
	for !t.Done() {
		if err := c.Step(ctx, t); err != nil {
			return err
		}
	}
	return nil
}
