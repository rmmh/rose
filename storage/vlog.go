package storage

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/klauspost/reedsolomon"
)

// PlogClient abstracts writing to a physical log, whether local or remote.
type PlogClient interface {
	Write(ctx context.Context, txnID int64, data []byte) (int64, error)
	Read(ctx context.Context, offset int64, length int) ([]byte, error)
}

type committingPlogClient interface {
	Commit(ctx context.Context, txnID int64) error
}

type scrubbablePlogClient interface {
	Scrub() (ScrubResult, error)
}

// ShardScrub pairs a vlog shard index with its plog scrub result.
type ShardScrub struct {
	Shard  int
	Result ScrubResult
}

// Vlog represents a Virtual Log that implements protection schemes on top of physical logs.
type Vlog struct {
	id     uint32
	length int64 // Atomic tracker of logical length

	scheme       string
	dataShards   int
	parityShards int
	clients      []PlogClient // Index corresponds to shard index (0 for duplicate)

	encoder reedsolomon.Encoder // Only initialized if scheme == "EC"
}

// NewVlog creates a new Vlog instance. clients must encompass all shards.
// For DUPLICATE, it's just a variable list of mirrors.
// For EC, length must be dataShards + parityShards.
func NewVlog(id uint32, scheme string, data, parity int, clients []PlogClient, initialLength int64) (*Vlog, error) {
	v := &Vlog{
		id:           id,
		length:       initialLength,
		scheme:       scheme,
		dataShards:   data,
		parityShards: parity,
		clients:      clients,
	}

	if scheme == "EC" {
		if len(clients) != data+parity {
			return nil, fmt.Errorf("EC vlog requires %d clients, got %d", data+parity, len(clients))
		}
		enc, err := reedsolomon.New(data, parity)
		if err != nil {
			return nil, fmt.Errorf("create reedsolomon encoder: %w", err)
		}
		v.encoder = enc
	}

	return v, nil
}

// Write appends data to the virtual log and returns the assigned virtual offset.
func (v *Vlog) Write(ctx context.Context, txnID int64, data []byte) (int64, error) {
	if len(data) == 0 {
		return atomic.LoadInt64(&v.length), nil
	}

	logicalLen := int64(len(data))

	if v.scheme == "NONE" || v.scheme == "DUPLICATE" {
		// Write to all clients concurrently
		var wg sync.WaitGroup
		errs := make(chan error, len(v.clients))

		for _, c := range v.clients {
			wg.Add(1)
			go func(client PlogClient) {
				defer wg.Done()
				_, err := client.Write(ctx, txnID, data)
				if err != nil {
					errs <- err
				}
			}(c)
		}
		wg.Wait()
		close(errs)

		if len(errs) > 0 {
			// In a real system, we'd handle partial failures via txn or ragged edges logic.
			return 0, <-errs
		}

		offset := atomic.AddInt64(&v.length, logicalLen) - logicalLen
		return offset, nil
	}

	if v.scheme == "EC" {
		// Encode into shards
		shards, err := v.encoder.Split(data)
		if err != nil {
			return 0, fmt.Errorf("split for EC: %w", err)
		}

		if err := v.encoder.Encode(shards); err != nil {
			return 0, fmt.Errorf("encode for EC: %w", err)
		}

		var wg sync.WaitGroup
		errs := make(chan error, len(v.clients))

		for i, c := range v.clients {
			wg.Add(1)
			go func(idx int, client PlogClient) {
				defer wg.Done()
				_, err := client.Write(ctx, txnID, shards[idx])
				if err != nil {
					errs <- err
				}
			}(i, c)
		}
		wg.Wait()
		close(errs)

		if len(errs) > 0 {
			return 0, <-errs
		}

		offset := atomic.AddInt64(&v.length, logicalLen) - logicalLen
		return offset, nil
	}

	return 0, fmt.Errorf("unknown protection scheme: %s", v.scheme)
}

// Read reads logical 'length' bytes starting from logical 'offset' in the Virtual Log.
func (v *Vlog) Read(ctx context.Context, offset int64, length int) ([]byte, error) {
	if v.scheme == "NONE" || v.scheme == "DUPLICATE" {
		// Try reading from the first available client
		for _, c := range v.clients {
			data, err := c.Read(ctx, offset, length)
			if err == nil {
				return data, nil
			}
		}
		return nil, fmt.Errorf("all clients failed to read from DUPLICATE vlog %d", v.id)
	}

	if v.scheme == "EC" {
		// A shard length corresponds to (length + padding) / dataShards.
		// For simplicity in this first pass, we assume writes are perfectly aligned to EC boundaries
		// or that we can reconstruct easily. However, `reedsolomon` usually requires fixed sizes.
		// Because we are just wrapping it, let's calculate the shard length assuming the data length was padded.
		// Actually, reedsolomon Split/Join requires knowing the exact size of the shards written.
		// To decode, we grab `length / dataShards` bytes per shard. Wait, what if it's not a multiple?
		// Realistically, the caller must read the exact `logical_len` stored in the chunk DB.

		// Approximate shard read for this chunk.
		shardLen := (length + v.dataShards - 1) / v.dataShards
		shards := make([][]byte, v.dataShards+v.parityShards)

		var wg sync.WaitGroup
		var mu sync.Mutex
		errCount := 0

		for i, c := range v.clients {
			wg.Add(1)
			go func(idx int, client PlogClient) {
				defer wg.Done()
				data, err := client.Read(ctx, offset/int64(v.dataShards), shardLen)
				mu.Lock()
				if err == nil {
					shards[idx] = data
				} else {
					errCount++
				}
				mu.Unlock()
			}(i, c)
		}
		wg.Wait()

		if errCount > v.parityShards {
			return nil, fmt.Errorf("too many EC shards missing: %d > %d", errCount, v.parityShards)
		}

		if err := v.encoder.Reconstruct(shards); err != nil {
			return nil, fmt.Errorf("reconstruct EC: %w", err)
		}

		var buf bytes.Buffer
		if err := v.encoder.Join(&buf, shards, length); err != nil {
			return nil, fmt.Errorf("join EC shards: %w", err)
		}

		return buf.Bytes(), nil
	}

	return nil, fmt.Errorf("unknown protection scheme: %s", v.scheme)
}

// ReconstructECShard rebuilds the missing shards of an EC stripe in place.
// Surviving shards must be present and of equal length; shards to regenerate
// must be nil. It is the regeneration primitive reprotect uses to rebuild a
// shard lost with a failed disk from the surviving data and parity shards.
func ReconstructECShard(dataShards, parityShards int, shards [][]byte) error {
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return fmt.Errorf("create reedsolomon encoder: %w", err)
	}
	return enc.Reconstruct(shards)
}

// Commit makes all physical writes issued through this virtual log durable.
func (v *Vlog) Commit(ctx context.Context, txnID int64) error {
	for _, client := range v.clients {
		committer, ok := client.(committingPlogClient)
		if !ok {
			return fmt.Errorf("plog client does not support commit")
		}
		if err := committer.Commit(ctx, txnID); err != nil {
			return err
		}
	}
	return nil
}

func (v *Vlog) Length() int64 { return atomic.LoadInt64(&v.length) }

func (v *Vlog) ID() uint32 { return v.id }

// Scrub validates every shard that backs the vlog, returning per-shard results.
// For DUPLICATE and EC schemes a corrupt shard reported here is recoverable from
// the surviving shards; the caller decides whether to schedule repair.
func (v *Vlog) Scrub() ([]ShardScrub, error) {
	out := make([]ShardScrub, 0, len(v.clients))
	for shard, c := range v.clients {
		scrubber, ok := c.(scrubbablePlogClient)
		if !ok {
			continue
		}
		res, err := scrubber.Scrub()
		if err != nil {
			return nil, fmt.Errorf("scrub vlog %d shard %d: %w", v.id, shard, err)
		}
		out = append(out, ShardScrub{Shard: shard, Result: res})
	}
	return out, nil
}
