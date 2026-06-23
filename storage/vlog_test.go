package storage

import (
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
