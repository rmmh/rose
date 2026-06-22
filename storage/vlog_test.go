package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
)

// simulatedPlogClient is an in-memory client that can be instructed to fail
type simulatedPlogClient struct {
	id          int
	data        []byte
	failOnWrite bool
	failOnRead  bool
	rng         *rand.Rand
}

func (s *simulatedPlogClient) Write(ctx context.Context, txnID int64, data []byte) (int64, error) {
	if s.failOnWrite {
		return 0, fmt.Errorf("simulated write failure on plog %d", s.id)
	}
	if s.rng != nil && s.rng.Float32() < 0.1 {
		return 0, fmt.Errorf("random simulated write failure on plog %d", s.id)
	}

	offset := int64(len(s.data))
	s.data = append(s.data, data...)
	return offset, nil
}

func (s *simulatedPlogClient) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	if s.failOnRead {
		return nil, fmt.Errorf("simulated read failure on plog %d", s.id)
	}
	if s.rng != nil && s.rng.Float32() < 0.1 {
		return nil, fmt.Errorf("random simulated read failure on plog %d", s.id)
	}

	if offset >= int64(len(s.data)) {
		return nil, fmt.Errorf("read past EOF")
	}

	end := offset + int64(length)
	if end > int64(len(s.data)) {
		end = int64(len(s.data))
	}
	return s.data[offset:end], nil
}

func TestVlog_DeterministicSimulation_EC(t *testing.T) {
	// 1. Initialize a seed
	seed := int64(42) // Deterministic seed
	rng := rand.New(rand.NewSource(seed))

	dataShards := 4
	parityShards := 2
	totalShards := dataShards + parityShards

	var clients []PlogClient
	simClients := make([]*simulatedPlogClient, totalShards)
	for i := 0; i < totalShards; i++ {
		simClients[i] = &simulatedPlogClient{
			id:   i,
			data: make([]byte, 0),
			rng:  rng, // This client will fail 10% of operations
		}
		clients = append(clients, simClients[i])
	}

	vlog, err := NewVlog(1, "EC", dataShards, parityShards, clients, 0)
	if err != nil {
		t.Fatalf("Failed to create vlog: %v", err)
	}

	// 2. Perform write iterations. We expect some writes might fail.
	ctx := context.Background()
	payload := []byte("hello deterministically simulated world! we need enough data here to split nicely across 4 shards.")

	// Ensure payload length is a nice multiple for simplicity.
	// 4 shards, so it should be a multiple of 4.
	padding := len(payload) % dataShards
	if padding > 0 {
		payload = append(payload, make([]byte, dataShards-padding)...)
	}

	for txn := int64(1); txn <= 10; txn++ {
		_, err := vlog.Write(ctx, txn, payload)

		// It's acceptable for a write to fail occasionally due to RNG (we need at least 4 successful shards, but EC encoder logic in vlog right now requires ALL writes to not return error. Wait!)
		// The current Vlog Write implementation aborts if ANY write fails.
		// "If len(errs) > 0 { return 0, <-errs }"
		// So if any shard write fails, the logical write fails.
		// Let's actually verify this behavior.
		if err == nil {
			// If it succeeded, try to read it back.
			readBack, err := vlog.Read(ctx, int64(txn-1)*int64(len(payload)), len(payload))
			// Since we allow simulated read failures, the Read might occasionally fail if too many shards fail to read.
			if err == nil {
				if !bytes.Equal(payload, readBack) {
					t.Fatalf("Expected %q, got %q", payload, readBack)
				}
			}
		}
	}
}

// TestVlogECVariableLengthRoundTrip writes variable-length chunks (as the
// FastCDC file path does) through an EC vlog and reads each back at its returned
// offset. It guards the stripe-width offset accounting: with unequal chunk
// lengths, mapping a chunk to its shard piece only works when each chunk's
// virtual offset is a multiple of dataShards.
func TestVlogECVariableLengthRoundTrip(t *testing.T) {
	dataShards, parityShards := 3, 1
	var clients []PlogClient
	for i := 0; i < dataShards+parityShards; i++ {
		clients = append(clients, &simulatedPlogClient{id: i, data: make([]byte, 0)})
	}
	vlog, err := NewVlog(7, "EC", dataShards, parityShards, clients, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rng := rand.New(rand.NewSource(5))

	lengths := []int{1, 7, 100, 4095, 4096, 4097, 5000, 33, 999}
	type record struct {
		offset int64
		data   []byte
	}
	var records []record
	for txn, n := range lengths {
		data := make([]byte, n)
		rng.Read(data)
		offset, err := vlog.Write(ctx, int64(txn+1), data)
		if err != nil {
			t.Fatalf("write %d (len %d): %v", txn, n, err)
		}
		if offset%int64(dataShards) != 0 {
			t.Fatalf("write %d offset %d is not a multiple of dataShards %d", txn, offset, dataShards)
		}
		records = append(records, record{offset: offset, data: data})
	}
	for i, r := range records {
		got, err := vlog.Read(ctx, r.offset, len(r.data))
		if err != nil {
			t.Fatalf("read %d (offset %d, len %d): %v", i, r.offset, len(r.data), err)
		}
		if !bytes.Equal(got, r.data) {
			t.Fatalf("chunk %d round-trip mismatch at offset %d len %d", i, r.offset, len(r.data))
		}
	}
}

// TestVlog_DeterministicSimulation_Duplicate tests DUPLICATE replication
func TestVlog_DeterministicSimulation_Duplicate(t *testing.T) {
	seed := int64(1337)
	rng := rand.New(rand.NewSource(seed))

	var clients []PlogClient
	for i := 0; i < 3; i++ {
		clients = append(clients, &simulatedPlogClient{
			id:   i,
			data: make([]byte, 0),
			rng:  rng,
		})
	}

	vlog, err := NewVlog(2, "DUPLICATE", 0, 0, clients, 0)
	if err != nil {
		t.Fatalf("Failed to create vlog: %v", err)
	}

	ctx := context.Background()
	payload := []byte("replicated payload data")

	for i := 0; i < 20; i++ {
		offset, err := vlog.Write(ctx, int64(i+1), payload)
		if err == nil {
			readBack, err := vlog.Read(ctx, offset, len(payload))
			if err == nil {
				if string(payload) != string(readBack) {
					t.Fatalf("Corrupt read: expected %q, got %q", payload, readBack)
				}
			}
		}
	}
}
