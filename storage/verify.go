package storage

import (
	"bytes"
	"context"
	"fmt"
)

// VerifyAll verifies a logical range against independent expected bytes on every
// backing shard. The caller must prevent relocation and concurrent mutation of
// the range. Read's reconstruction quorum is deliberately insufficient here:
// publication promises protection, not merely current readability.
//
// EC verification reads complete intersecting rows, checks codeword consistency,
// and compares the requested logical bytes. Thus a corrupt parity shard cannot
// hide behind healthy data shards. Per-shard reads must validate their own stored
// integrity metadata; this check also detects inconsistent yet readable copies.
func (v *Vlog) VerifyAll(ctx context.Context, offset int64, expected []byte) error {
	if offset < 0 || offset > v.Length() || int64(len(expected)) > v.Length()-offset {
		return fmt.Errorf("vlog %d verify range outside logical prefix", v.id)
	}
	if len(expected) == 0 {
		return nil
	}
	if v.scheme == "NONE" || v.scheme == "DUPLICATE" {
		return v.fanout(ctx, func(ctx context.Context, shard int, client PlogClient) error {
			got, err := client.Read(ctx, offset, len(expected))
			if err != nil {
				return fmt.Errorf("verify shard %d: %w", shard, err)
			}
			if !bytes.Equal(got, expected) {
				return fmt.Errorf("verify shard %d differs from expected bytes: %w", shard, ErrBitrot)
			}
			return nil
		})
	}
	if v.scheme != "EC" {
		return fmt.Errorf("unknown protection scheme %q", v.scheme)
	}
	end := offset + int64(len(expected))
	sw := v.stripeWidth()
	for row := offset / sw * sw; row < end; row += sw {
		shards := make([][]byte, len(v.clients))
		if err := v.fanout(ctx, func(ctx context.Context, shard int, client PlogClient) error {
			got, err := client.Read(ctx, row/sw*ecColumnBytes, int(ecColumnBytes))
			if err != nil {
				return fmt.Errorf("verify row %d shard %d: %w", row, shard, err)
			}
			if int64(len(got)) != ecColumnBytes {
				return fmt.Errorf("verify row %d shard %d: short read", row, shard)
			}
			shards[shard] = got
			return nil
		}); err != nil {
			return err
		}
		valid, err := v.encoder.Verify(shards)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("verify EC row %d inconsistent codeword: %w", row, ErrBitrot)
		}
		for shard := 0; shard < v.dataShards; shard++ {
			start := row + int64(shard)*ecColumnBytes
			lo, hi := max(start, offset), min(start+ecColumnBytes, end)
			if lo < hi && !bytes.Equal(shards[shard][lo-start:hi-start], expected[lo-offset:hi-offset]) {
				return fmt.Errorf("verify EC row %d data shard %d differs from expected bytes: %w", row, shard, ErrBitrot)
			}
		}
	}
	return nil
}
