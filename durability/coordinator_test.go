package durability

import (
	"context"
	"errors"
	"fmt"
	"testing"
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

func TestStrictCommitExhaustsCrashBarriersAndDiskOrders(t *testing.T) {
	// Commit emits nine deterministic boundaries for three shards.  Explore a
	// crash at each boundary, plus a successful run, across all 3! disk orders.
	const barriers = 10
	for _, order := range permutations([]*simulatedDisk{newSimulatedDisk("d1"), newSimulatedDisk("d2"), newSimulatedDisk("d3")}) {
		for crashAt := 0; crashAt <= barriers; crashAt++ {
			disks := []*simulatedDisk{newSimulatedDisk("d1"), newSimulatedDisk("d2"), newSimulatedDisk("d3")}
			byID := map[string]*simulatedDisk{"d1": disks[0], "d2": disks[1], "d3": disks[2]}
			writes := make([]ShardWrite, 0, len(order))
			for shard, disk := range order {
				writes = append(writes, ShardWrite{Disk: byID[disk.id], Shard: shard + 1, Data: []byte{byte(shard)}})
			}
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
				if err != nil {
					t.Fatalf("successful order %v: %v", order, err)
				}
			} else if !errors.Is(err, ErrInjectedCrash) {
				t.Fatalf("order %v crash barrier %d: got %v, want injected crash", order, crashAt, err)
			}
			for _, disk := range disks {
				disk.Crash()
			}
			metadata.Recover("txn")
			if metadata.IsPublished("txn") {
				for _, placement := range metadata.published["txn"] {
					if _, ok := byID[placement.DiskID].durable[fmt.Sprintf("txn/%d", placement.Shard)]; !ok {
						t.Fatalf("published placement lost after crash: order=%v barrier=%d placement=%+v", order, crashAt, placement)
					}
				}
			} else if metadata.state["txn"] != "abandoned" {
				t.Fatalf("unpublished transaction was not abandoned: order=%v barrier=%d state=%q", order, crashAt, metadata.state["txn"])
			}
		}
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
			if err != nil {
				t.Fatal(err)
			}
			txnB, err := coordinator.Start("txn-b", writes)
			if err != nil {
				t.Fatal(err)
			}
			for _, choice := range schedule {
				txn := txnA
				if choice == 1 {
					txn = txnB
				}
				if err := coordinator.Step(context.Background(), txn); err != nil {
					t.Fatalf("schedule %v step txn %d: %v", schedule, choice, err)
				}
			}
			if !txnA.Done() || !txnB.Done() || !metadata.IsPublished("txn-a") || !metadata.IsPublished("txn-b") {
				t.Fatalf("incomplete schedule %v", schedule)
			}
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
	if schedules != 184756 {
		t.Fatalf("explored %d schedules, want 184756", schedules)
	}
}
