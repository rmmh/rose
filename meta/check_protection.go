package meta

import (
	"bytes"
	"context"
	"fmt"

	"github.com/klauspost/reedsolomon"
	"github.com/rmmh/rose/storage"
)

// inspectStoredProtection checks all copies/codewords of the recorded prefix.
// This traversal is independent of Vlog.VerifyAll and never reconstructs over a
// missing shard: readability is checked separately by plaintext inspection.
func inspectStoredProtection(ctx context.Context, scheme string, data, parity int, prefix int64, clients []storage.PlogClient) error {
	if prefix < 0 {
		return fmt.Errorf("negative recorded prefix")
	}
	if scheme == "NONE" || scheme == "DUPLICATE" {
		for offset := int64(0); offset < prefix; {
			count := int(min(prefix-offset, 1<<20))
			var expected []byte
			for shard, client := range clients {
				got, err := client.Read(ctx, offset, count)
				if err != nil {
					return fmt.Errorf("copy %d at %d: %w", shard, offset, err)
				}
				if len(got) != count {
					return fmt.Errorf("copy %d at %d: short read", shard, offset)
				}
				if shard == 0 {
					expected = got
				} else if !bytes.Equal(expected, got) {
					return fmt.Errorf("copy %d disagrees with copy 0 at %d", shard, offset)
				}
			}
			offset += int64(count)
		}
		return nil
	}
	column := storage.ECStripeWidth(1)
	if scheme != "EC" || data <= 0 || parity <= 0 || prefix%int64(data) != 0 || (prefix/int64(data))%column != 0 {
		return fmt.Errorf("invalid coded prefix geometry")
	}
	encoder, err := reedsolomon.New(data, parity)
	if err != nil {
		return err
	}
	for offset := int64(0); offset < prefix/int64(data); offset += column {
		shards := make([][]byte, len(clients))
		for index, client := range clients {
			shards[index], err = client.Read(ctx, offset, int(column))
			if err != nil {
				return fmt.Errorf("coded column %d shard %d: %w", offset, index, err)
			}
			if int64(len(shards[index])) != column {
				return fmt.Errorf("coded column %d shard %d: short read", offset, index)
			}
		}
		valid, err := encoder.Verify(shards)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("inconsistent EC codeword at physical column offset %d", offset)
		}
	}
	return nil
}
