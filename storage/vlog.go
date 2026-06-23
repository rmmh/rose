package storage

import (
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

// positionedPlogClient provides an idempotent physical append at a reserved
// logical offset.  It is deliberately separate from PlogClient so old read
// paths and simple test clients do not accidentally claim retry safety.
type positionedPlogClient interface {
	EnsureAppend(ctx context.Context, offset int64, data []byte) error
}

type scrubbablePlogClient interface {
	Scrub() (ScrubResult, error)
}

// truncatablePlogClient discards a shard's uncommitted tail past a logical
// length. Only live local plogs implement it; an offline shard (unreachable
// disk) is skipped during length reconciliation.
type truncatablePlogClient interface {
	TruncateTo(logical int64) error
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

	// writeMu serializes appends to this vlog. A virtual offset is reserved from
	// length and the same bytes are appended to every backing plog; those two
	// steps must be atomic per vlog or two concurrent writers interleave and a
	// chunk's reserved virtual offset stops matching where its bytes physically
	// landed. Distinct vlogs (e.g. different buckets' active vlogs) still write
	// concurrently; reads never take this lock.
	writeMu sync.Mutex

	scheme       string
	dataShards   int
	parityShards int
	clients      []PlogClient // Index corresponds to shard index (0 for duplicate)

	encoder reedsolomon.Encoder // Only initialized if scheme == "EC"
}

// ecColumnBytes is the EC stripe column width: one full plog block, so each
// stripe column maps to exactly one hash-protected block and a row's column is
// appended (and later scrubbed/reconstructed) as a unit. It is a var, not a
// const, only so tests can shrink it to keep fixtures small.
var ecColumnBytes int64 = dataPerBlock

// stripeWidth is the logical span of one complete EC stripe row: dataShards
// columns of ecColumnBytes each. EC vlogs only ever store whole rows, so their
// logical length is always a multiple of this.
func (v *Vlog) stripeWidth() int64 { return ecColumnBytes * int64(v.dataShards) }

// ECStripeWidth is the logical span of one complete EC stripe row for the given
// data-shard count, the unit promotion must pack staged chunks into before it
// can append them to an EC vlog (whose writes are whole rows only).
func ECStripeWidth(dataShards int) int64 { return ecColumnBytes * int64(dataShards) }

// SetECColumnBytesForTest overrides the EC stripe column width and returns a
// function that restores the previous value. It lets tests in other packages
// shrink stripes so EC fixtures stay small; production never calls it.
func SetECColumnBytesForTest(n int64) func() {
	prev := ecColumnBytes
	ecColumnBytes = n
	return func() { ecColumnBytes = prev }
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

	// Serialize appends: the offset reservation below and the physical plog
	// appends must happen as one unit, or concurrent writers interleave and a
	// chunk's virtual offset no longer matches where its bytes landed.
	v.writeMu.Lock()
	defer v.writeMu.Unlock()

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
		// EC vlogs store whole stripe rows only: the caller (promotion) hands us a
		// multiple of the stripe width, so every column is exactly ecColumnBytes and
		// both data and parity are appended once and sealed immutably. A partial row
		// would force parity to keep changing after its block was already full and
		// sealed (columns fill left-to-right), which the append-only plog cannot do;
		// the trailing partial stripe lives in the replicated staging vlog instead.
		sw := v.stripeWidth()
		if len(data) == 0 || int64(len(data))%sw != 0 {
			return 0, fmt.Errorf("EC vlog %d write: length %d is not a positive multiple of stripe width %d", v.id, len(data), sw)
		}
		offset := atomic.LoadInt64(&v.length)
		for rowOff := int64(0); rowOff < int64(len(data)); rowOff += sw {
			shards, err := v.encodeRow(data[rowOff : rowOff+sw])
			if err != nil {
				return 0, err
			}
			if err := v.fanout(func(idx int, client PlogClient) error {
				_, werr := client.Write(ctx, txnID, shards[idx])
				return werr
			}); err != nil {
				return 0, err
			}
		}
		atomic.AddInt64(&v.length, int64(len(data)))
		return offset, nil
	}

	return 0, fmt.Errorf("unknown protection scheme: %s", v.scheme)
}

// encodeRow splits one complete stripe row (dataShards columns of ecColumnBytes)
// into shards and computes the parity columns. The data shards alias rowData
// (Encode only writes the freshly allocated parity shards), so callers must not
// mutate rowData until the shards have been handed to the plogs.
func (v *Vlog) encodeRow(rowData []byte) ([][]byte, error) {
	shards := make([][]byte, v.dataShards+v.parityShards)
	for j := 0; j < v.dataShards; j++ {
		shards[j] = rowData[int64(j)*ecColumnBytes : int64(j+1)*ecColumnBytes]
	}
	for k := v.dataShards; k < v.dataShards+v.parityShards; k++ {
		shards[k] = make([]byte, ecColumnBytes)
	}
	if err := v.encoder.Encode(shards); err != nil {
		return nil, fmt.Errorf("encode EC row: %w", err)
	}
	return shards, nil
}

// fanout runs op against every backing plog client concurrently and returns the
// first error. Each EC stripe row appends its data and parity columns this way.
func (v *Vlog) fanout(op func(idx int, client PlogClient) error) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(v.clients))
	for i, c := range v.clients {
		wg.Add(1)
		go func(idx int, client PlogClient) {
			defer wg.Done()
			if err := op(idx, client); err != nil {
				errs <- err
			}
		}(i, c)
	}
	wg.Wait()
	close(errs)
	return <-errs
}

// EnsureWrite makes data durable at an already-reserved virtual offset.  A
// lease gives one write operation exclusive ownership of a vlog, while this
// method makes retries safe if a previous fan-out reached only some shards.
// The caller must Commit before metadata records the chunk as durable.
func (v *Vlog) EnsureWrite(ctx context.Context, offset int64, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	v.writeMu.Lock()
	defer v.writeMu.Unlock()
	if offset != atomic.LoadInt64(&v.length) {
		return fmt.Errorf("ensure write vlog %d: offset %d does not match length %d", v.id, offset, v.length)
	}

	if v.scheme == "EC" {
		sw := v.stripeWidth()
		if int64(len(data))%sw != 0 {
			return fmt.Errorf("EC vlog %d ensure write: length %d is not a multiple of stripe width %d", v.id, len(data), sw)
		}
		for rowOff := int64(0); rowOff < int64(len(data)); rowOff += sw {
			shards, err := v.encodeRow(data[rowOff : rowOff+sw])
			if err != nil {
				return err
			}
			// Every column of row r occupies plog-logical offset r*ecColumnBytes in
			// its shard plog; EnsureAppend makes the retry idempotent.
			plogOffset := (offset + rowOff) / sw * ecColumnBytes
			if err := v.fanout(func(idx int, client PlogClient) error {
				positioned, ok := client.(positionedPlogClient)
				if !ok {
					return fmt.Errorf("vlog %d plog client does not support positioned writes", v.id)
				}
				return positioned.EnsureAppend(ctx, plogOffset, shards[idx])
			}); err != nil {
				return err
			}
		}
		atomic.AddInt64(&v.length, int64(len(data)))
		return nil
	}

	if v.scheme != "NONE" && v.scheme != "DUPLICATE" {
		return fmt.Errorf("unknown protection scheme: %s", v.scheme)
	}
	// Mirror schemes write the same bytes at the same logical offset to every copy.
	if err := v.fanout(func(_ int, client PlogClient) error {
		positioned, ok := client.(positionedPlogClient)
		if !ok {
			return fmt.Errorf("vlog %d plog client does not support positioned writes", v.id)
		}
		return positioned.EnsureAppend(ctx, offset, data)
	}); err != nil {
		return err
	}
	atomic.AddInt64(&v.length, int64(len(data)))
	return nil
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
		if offset < 0 || length < 0 {
			return nil, fmt.Errorf("EC vlog %d read: invalid offset %d length %d", v.id, offset, length)
		}
		// Walk the request one stripe row at a time. Within a row the logical bytes
		// map to contiguous slices of the data columns, so the healthy path reads
		// only the data plogs the range actually touches -- not every shard.
		sw := v.stripeWidth()
		out := make([]byte, 0, length)
		end := offset + int64(length)
		for cur := offset; cur < end; {
			row := cur / sw
			rowEnd := (row + 1) * sw
			if rowEnd > end {
				rowEnd = end
			}
			rowBytes, err := v.readRowRange(ctx, row, cur-row*sw, rowEnd-row*sw)
			if err != nil {
				return nil, err
			}
			out = append(out, rowBytes...)
			cur = rowEnd
		}
		return out, nil
	}

	return nil, fmt.Errorf("unknown protection scheme: %s", v.scheme)
}

// readRowRange returns logical bytes [a, b) within stripe row, where a and b are
// offsets from the row start in 0..stripeWidth. It reads the touched data
// columns directly and only reconstructs the row from parity if a column read
// fails, after which the rest of the range is served from the rebuilt shards.
func (v *Vlog) readRowRange(ctx context.Context, row, a, b int64) ([]byte, error) {
	out := make([]byte, 0, b-a)
	var shards [][]byte // populated lazily on the first failed column read
	for cur := a; cur < b; {
		col := cur / ecColumnBytes
		pos := cur % ecColumnBytes
		n := ecColumnBytes - pos
		if cur+n > b {
			n = b - cur
		}
		if shards == nil {
			data, err := v.clients[col].Read(ctx, row*ecColumnBytes+pos, int(n))
			if err == nil {
				out = append(out, data...)
				cur += n
				continue
			}
			shards, err = v.reconstructRow(ctx, row)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, shards[col][pos:pos+n]...)
		cur += n
	}
	return out, nil
}

// reconstructRow reads every surviving column of a stripe row and rebuilds the
// missing ones from parity. Each shard is a full ecColumnBytes column because EC
// vlogs only ever store complete rows.
func (v *Vlog) reconstructRow(ctx context.Context, row int64) ([][]byte, error) {
	shards := make([][]byte, len(v.clients))
	var mu sync.Mutex
	missing := 0
	_ = v.fanout(func(idx int, client PlogClient) error {
		data, err := client.Read(ctx, row*ecColumnBytes, int(ecColumnBytes))
		mu.Lock()
		defer mu.Unlock()
		if err == nil && int64(len(data)) == ecColumnBytes {
			shards[idx] = data
		} else {
			missing++
		}
		return nil
	})
	if missing > v.parityShards {
		return nil, fmt.Errorf("EC vlog %d row %d: %d shards missing > %d parity", v.id, row, missing, v.parityShards)
	}
	if err := v.encoder.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("reconstruct EC vlog %d row %d: %w", v.id, row, err)
	}
	return shards, nil
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

// ReconcileShardLengths discards any uncommitted tail a backing plog grew past
// the committed vlog length, restoring the invariant that each shard's plog
// cursor matches where the vlog -- whose length is restored authoritatively from
// the metadata DB -- expects the next append to land. It is called on mount: a
// crash after new rows were sealed to the plog files but before the vlog length
// was committed leaves the plogs physically longer than the DB records, and an
// unreconciled append would be placed at the inflated plog cursor while reads
// resolve against the smaller vlog length. Each data and parity shard carries an
// equal share of the logical stream (length for DUPLICATE/NONE, length/dataShards
// for EC, since every stripe row contributes one column per shard). Shards on
// unreachable disks (offline stubs) are skipped; they carry no live file.
func (v *Vlog) ReconcileShardLengths() error {
	perShard := atomic.LoadInt64(&v.length)
	if v.scheme == "EC" {
		perShard /= int64(v.dataShards)
	}
	for shard, c := range v.clients {
		t, ok := c.(truncatablePlogClient)
		if !ok {
			continue
		}
		if err := t.TruncateTo(perShard); err != nil {
			return fmt.Errorf("reconcile vlog %d shard %d length: %w", v.id, shard, err)
		}
	}
	return nil
}

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
