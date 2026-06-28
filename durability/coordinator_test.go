package durability

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type simulatedDisk struct {
	id       string
	volatile map[string]PreparedRecord
	durable  map[string]PreparedRecord
}

func newSimulatedDisk(id string) *simulatedDisk {
	return &simulatedDisk{id: id, volatile: map[string]PreparedRecord{}, durable: map[string]PreparedRecord{}}
}

func (d *simulatedDisk) ID() string { return d.id }

func recordKey(record PreparedRecord) string { return fmt.Sprintf("%s/%d", record.TxnID, record.Shard) }

func (d *simulatedDisk) Prepare(_ context.Context, record PreparedRecord) error {
	d.volatile[recordKey(record)] = record
	return nil
}

func (d *simulatedDisk) Sync(context.Context) error {
	for key, record := range d.volatile {
		d.durable[key] = record
		delete(d.volatile, key)
	}
	return nil
}

func (d *simulatedDisk) Crash() { d.volatile = map[string]PreparedRecord{} }

type simulatedMetadata struct {
	state     map[string]string
	published map[string][]Placement
	diskByID  map[string]*simulatedDisk
}

func newSimulatedMetadata(disks []*simulatedDisk) *simulatedMetadata {
	diskByID := make(map[string]*simulatedDisk, len(disks))
	for _, disk := range disks {
		diskByID[disk.id] = disk
	}
	return &simulatedMetadata{state: map[string]string{}, published: map[string][]Placement{}, diskByID: diskByID}
}

func (m *simulatedMetadata) Begin(_ context.Context, txnID string) error {
	m.state[txnID] = "open"
	return nil
}

func (m *simulatedMetadata) Publish(_ context.Context, txnID string, placements []Placement) error {
	for _, placement := range placements {
		disk := m.diskByID[placement.DiskID]
		if disk == nil {
			return fmt.Errorf("unknown disk %q", placement.DiskID)
		}
		if _, ok := disk.durable[fmt.Sprintf("%s/%d", txnID, placement.Shard)]; !ok {
			return fmt.Errorf("placement %s/%d is not durable", placement.DiskID, placement.Shard)
		}
	}
	m.state[txnID] = "published"
	m.published[txnID] = append([]Placement(nil), placements...)
	return nil
}

func (m *simulatedMetadata) Recover(txnID string) {
	if m.state[txnID] != "published" {
		m.state[txnID] = "abandoned"
	}
}

func (m *simulatedMetadata) IsPublished(txnID string) bool { return m.state[txnID] == "published" }

func permutations(disks []*simulatedDisk) [][]*simulatedDisk {
	if len(disks) == 0 {
		return [][]*simulatedDisk{{}}
	}
	var out [][]*simulatedDisk
	for i, disk := range disks {
		rest := append([]*simulatedDisk(nil), disks[:i]...)
		rest = append(rest, disks[i+1:]...)
		for _, tail := range permutations(rest) {
			out = append(out, append([]*simulatedDisk{disk}, tail...))
		}
	}
	return out
}

func numberedDisks(count int) []*simulatedDisk {
	disks := make([]*simulatedDisk, 0, count)
	for i := 1; i <= count; i++ {
		disks = append(disks, newSimulatedDisk(fmt.Sprintf("d%d", i)))
	}
	return disks
}

func disksByID(disks []*simulatedDisk) map[string]*simulatedDisk {
	byID := make(map[string]*simulatedDisk, len(disks))
	for _, disk := range disks {
		byID[disk.id] = disk
	}
	return byID
}

func writesForOrder(order []*simulatedDisk, byID map[string]*simulatedDisk) []ShardWrite {
	writes := make([]ShardWrite, 0, len(order))
	for shard, disk := range order {
		writes = append(writes, ShardWrite{Disk: byID[disk.id], Shard: shard + 1, Data: []byte{byte(shard)}})
	}
	return writes
}

func assertRecoveredTransactionSafe(t *testing.T, metadata *simulatedMetadata, byID map[string]*simulatedDisk, txnID string) {
	t.Helper()
	metadata.Recover(txnID)
	if metadata.IsPublished(txnID) {
		for _, placement := range metadata.published[txnID] {
			_, ok := byID[placement.DiskID].durable[fmt.Sprintf("%s/%d", txnID, placement.Shard)]
			assert.True(t, ok, "published placement lost after crash: txn=%s placement=%+v", txnID, placement)
		}
		return
	}
	assert.Equal(t, "abandoned", metadata.state[txnID], "unpublished transaction was not abandoned: txn=%s", txnID)
}

func TestStrictCommitExhaustsCrashBarriersAndDiskOrders(t *testing.T) {
	// Commit emits nine deterministic boundaries for three shards.  Explore a
	// crash at each boundary, plus a successful run, across all 3! disk orders.
	const barriers = 10
	for _, order := range permutations([]*simulatedDisk{newSimulatedDisk("d1"), newSimulatedDisk("d2"), newSimulatedDisk("d3")}) {
		for crashAt := 0; crashAt <= barriers; crashAt++ {
			disks := numberedDisks(3)
			byID := disksByID(disks)
			writes := writesForOrder(order, byID)
			metadata := newSimulatedMetadata(disks)
			seen := 0
			coordinator := Coordinator{Metadata: metadata, Hook: func(Point) error {
				seen++
				if crashAt != 0 && seen == crashAt {
					return ErrInjectedCrash
				}
				return nil
			}}
			err := coordinator.Commit(context.Background(), "txn", writes)
			if crashAt == 0 {
				require.NoError(t, err, "successful order %v", order)
			} else {
				require.ErrorIs(t, err, ErrInjectedCrash, "order %v crash barrier %d", order, crashAt)
			}
			for _, disk := range disks {
				disk.Crash()
			}
			assertRecoveredTransactionSafe(t, metadata, byID, "txn")
		}
	}
}

func TestStrictCommitCrashBarriersAcrossShardCounts(t *testing.T) {
	for _, shardCount := range []int{1, 2, 4} {
		barriers := 2*shardCount + 4
		for _, order := range permutations(numberedDisks(shardCount)) {
			for crashAt := 0; crashAt <= barriers; crashAt++ {
				disks := numberedDisks(shardCount)
				byID := disksByID(disks)
				metadata := newSimulatedMetadata(disks)
				seen := 0
				coordinator := Coordinator{Metadata: metadata, Hook: func(Point) error {
					seen++
					if crashAt != 0 && seen == crashAt {
						return ErrInjectedCrash
					}
					return nil
				}}
				err := coordinator.Commit(context.Background(), "txn", writesForOrder(order, byID))
				if crashAt == 0 {
					require.NoError(t, err, "%d shards successful order %v", shardCount, order)
				} else {
					require.ErrorIs(t, err, ErrInjectedCrash, "%d shards order %v crash barrier %d", shardCount, order, crashAt)
				}
				for _, disk := range disks {
					disk.Crash()
				}
				assertRecoveredTransactionSafe(t, metadata, byID, "txn")
			}
		}
	}
}

func TestPostPublishCrashIsRecoveredAsPublished(t *testing.T) {
	const shardCount = 2
	for _, crashAt := range []int{2*shardCount + 3, 2*shardCount + 4} {
		disks := numberedDisks(shardCount)
		metadata := newSimulatedMetadata(disks)
		seen := 0
		coordinator := Coordinator{Metadata: metadata, Hook: func(Point) error {
			seen++
			if seen == crashAt {
				return ErrInjectedCrash
			}
			return nil
		}}
		err := coordinator.Commit(context.Background(), "txn", []ShardWrite{
			{Disk: disks[0], Shard: 1, Data: []byte("a")},
			{Disk: disks[1], Shard: 2, Data: []byte("b")},
		})
		require.ErrorIs(t, err, ErrInjectedCrash, "crash barrier %d", crashAt)
		for _, disk := range disks {
			disk.Crash()
		}
		metadata.Recover("txn")
		assert.True(t, metadata.IsPublished("txn"), "crash barrier %d state=%q", crashAt, metadata.state["txn"])
	}
}

func TestCoordinatorStepHookOrder(t *testing.T) {
	disks := numberedDisks(2)
	metadata := newSimulatedMetadata(disks)
	var got []Point
	coordinator := Coordinator{Metadata: metadata, Hook: func(point Point) error {
		got = append(got, point)
		return nil
	}}
	err := coordinator.Commit(context.Background(), "txn", []ShardWrite{
		{Disk: disks[0], Shard: 1, Data: []byte("a")},
		{Disk: disks[1], Shard: 2, Data: []byte("b")},
	})
	require.NoError(t, err)
	want := []Point{
		AfterBegin,
		AfterPrepare,
		AfterPrepare,
		AfterDiskSync,
		AfterDiskSync,
		BeforeMetadataPublish,
		AfterMetadataPublish,
		BeforeAcknowledgement,
	}
	assert.Equal(t, want, got)
}

func TestCoordinatorStartRejectsInvalidPlacementsWithoutSideEffects(t *testing.T) {
	disks := numberedDisks(2)
	tests := []struct {
		name   string
		writes []ShardWrite
	}{
		{name: "empty"},
		{name: "nil disk", writes: []ShardWrite{{Shard: 1, Data: []byte("a")}}},
		{name: "duplicate disk", writes: []ShardWrite{
			{Disk: disks[0], Shard: 1, Data: []byte("a")},
			{Disk: disks[0], Shard: 2, Data: []byte("b")},
		}},
		{name: "duplicate shard", writes: []ShardWrite{
			{Disk: disks[0], Shard: 1, Data: []byte("a")},
			{Disk: disks[1], Shard: 1, Data: []byte("b")},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := newSimulatedMetadata(disks)
			coordinator := Coordinator{Metadata: metadata}
			txn, err := coordinator.Start("txn", tt.writes)
			require.Error(t, err)
			assert.Nil(t, txn)
			assert.Empty(t, metadata.state)
			for _, disk := range disks {
				assert.Empty(t, disk.volatile, "disk %s volatile side effects", disk.id)
				assert.Empty(t, disk.durable, "disk %s durable side effects", disk.id)
			}
		})
	}
}

func TestStrictCommitExhaustsTwoTransactionInterleavings(t *testing.T) {
	// Each transaction has ten coordinator boundaries.  Execute every one of
	// C(20, 10) scheduler choices against fresh virtual disks and metadata.
	var schedules int
	var explore func([]int, int, int)
	explore = func(schedule []int, remainingA, remainingB int) {
		if remainingA == 0 && remainingB == 0 {
			schedules++
			disks := []*simulatedDisk{newSimulatedDisk("d1"), newSimulatedDisk("d2"), newSimulatedDisk("d3")}
			metadata := newSimulatedMetadata(disks)
			writes := []ShardWrite{
				{Disk: disks[0], Shard: 1, Data: []byte("a")},
				{Disk: disks[1], Shard: 2, Data: []byte("b")},
				{Disk: disks[2], Shard: 3, Data: []byte("c")},
			}
			coordinator := Coordinator{Metadata: metadata}
			txnA, err := coordinator.Start("txn-a", writes)
			require.NoError(t, err)
			txnB, err := coordinator.Start("txn-b", writes)
			require.NoError(t, err)
			for _, choice := range schedule {
				txn := txnA
				if choice == 1 {
					txn = txnB
				}
				require.NoError(t, coordinator.Step(context.Background(), txn), "schedule %v step txn %d", schedule, choice)
			}
			assert.True(t, txnA.Done(), "schedule %v txn-a incomplete", schedule)
			assert.True(t, txnB.Done(), "schedule %v txn-b incomplete", schedule)
			assert.True(t, metadata.IsPublished("txn-a"), "schedule %v txn-a unpublished", schedule)
			assert.True(t, metadata.IsPublished("txn-b"), "schedule %v txn-b unpublished", schedule)
			return
		}
		if remainingA > 0 {
			next := append(append([]int(nil), schedule...), 0)
			explore(next, remainingA-1, remainingB)
		}
		if remainingB > 0 {
			next := append(append([]int(nil), schedule...), 1)
			explore(next, remainingA, remainingB-1)
		}
	}
	explore(nil, 10, 10)
	assert.Equal(t, 184756, schedules)
}
