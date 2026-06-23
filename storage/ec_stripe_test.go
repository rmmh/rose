package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

// withECColumn shrinks the EC stripe column width for the duration of a test so
// fixtures stay small, restoring the production value afterward.
func withECColumn(t *testing.T, n int64) {
	t.Helper()
	prev := ecColumnBytes
	ecColumnBytes = n
	t.Cleanup(func() { ecColumnBytes = prev })
}

// ecVlogOnPlogs builds an EC vlog backed by real on-disk plogs in a temp dir.
func ecVlogOnPlogs(t *testing.T, data, parity int) (*Vlog, []*Plog) {
	t.Helper()
	dir := t.TempDir()
	total := data + parity
	plogs := make([]*Plog, total)
	clients := make([]PlogClient, total)
	for i := 0; i < total; i++ {
		p, err := OpenPlog(filepath.Join(dir, fmt.Sprintf("plog-%d", i)), uint32(i))
		if err != nil {
			t.Fatal(err)
		}
		plogs[i] = p
		clients[i] = plogClientAdapter{p}
	}
	v, err := NewVlog(1, "EC", data, parity, clients, 0)
	if err != nil {
		t.Fatal(err)
	}
	return v, plogs
}

// TestECStripeRoundTrip writes several complete stripe rows and reads them back
// at offsets that fall within a column, span columns, and span rows.
func TestECStripeRoundTrip(t *testing.T) {
	withECColumn(t, 64)
	v, _ := ecVlogOnPlogs(t, 4, 2)
	ctx := context.Background()

	sw := v.stripeWidth() // 64 * 4 = 256
	rows := 3
	src := make([]byte, int(sw)*rows)
	rand.New(rand.NewSource(99)).Read(src)

	off, err := v.Write(ctx, 1, src)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if off != 0 {
		t.Fatalf("first write offset = %d, want 0", off)
	}
	if got := v.Length(); got != int64(len(src)) {
		t.Fatalf("length = %d, want %d", got, len(src))
	}

	cases := []struct{ off, n int64 }{
		{0, int64(len(src))}, // everything
		{0, 10},              // start of first column
		{30, 20},             // within first column
		{60, 16},             // spans column 0 -> column 1 boundary
		{int64(sw) - 8, 32},  // spans a row boundary
		{int64(sw), 64},      // a full middle column
		{200, int64(len(src)) - 200},
	}
	for _, c := range cases {
		got, err := v.Read(ctx, c.off, int(c.n))
		if err != nil {
			t.Fatalf("read [%d,+%d): %v", c.off, c.n, err)
		}
		if !bytes.Equal(got, src[c.off:c.off+c.n]) {
			t.Fatalf("read [%d,+%d) mismatch", c.off, c.n)
		}
	}
}

// TestECStripeRejectsPartialRow guards that only whole stripe rows reach an EC
// vlog: a sub-row write would create a trailing partial stripe whose parity the
// append-only plogs cannot keep mutable.
func TestECStripeRejectsPartialRow(t *testing.T) {
	withECColumn(t, 64)
	v, _ := ecVlogOnPlogs(t, 4, 2)
	ctx := context.Background()
	sw := v.stripeWidth()

	for _, n := range []int{1, int(sw) - 1, int(sw) + 1, int(sw) + 7} {
		if _, err := v.Write(ctx, 1, make([]byte, n)); err == nil {
			t.Fatalf("Write of %d bytes (stripe width %d) should have failed", n, sw)
		}
		if err := v.EnsureWrite(ctx, 0, [][]byte{make([]byte, n)}); err == nil {
			t.Fatalf("EnsureWrite of %d bytes should have failed", n)
		}
	}
	if v.Length() != 0 {
		t.Fatalf("length advanced on rejected writes: %d", v.Length())
	}
}

// TestECStripeReconstructsMissingShards knocks out data shards on read and
// confirms the row is rebuilt from parity up to the parity limit, then fails
// once too many are gone.
func TestECStripeReconstructsMissingShards(t *testing.T) {
	withECColumn(t, 64)
	data, parity := 4, 2
	sim := make([]*simulatedPlogClient, data+parity)
	clients := make([]PlogClient, data+parity)
	for i := range sim {
		sim[i] = &simulatedPlogClient{id: i, data: make([]byte, 0)}
		clients[i] = sim[i]
	}
	v, err := NewVlog(1, "EC", data, parity, clients, 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	src := make([]byte, int(v.stripeWidth())*2)
	rand.New(rand.NewSource(7)).Read(src)
	if _, err := v.Write(ctx, 1, src); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Lose two shards (== parity): reads must still reconstruct.
	sim[1].failOnRead = true
	sim[4].failOnRead = true // a parity shard
	got, err := v.Read(ctx, 0, len(src))
	if err != nil {
		t.Fatalf("read with 2 shards down: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatal("reconstructed data mismatch")
	}

	// Lose a third: now beyond the parity budget, the read must fail cleanly.
	sim[2].failOnRead = true
	if _, err := v.Read(ctx, 0, len(src)); err == nil {
		t.Fatal("read with 3 shards down should have failed")
	}
}

// TestECStripeEnsureWriteRetriesPartialFanout simulates a prior fan-out that
// reached only one shard, then replays the whole write through EnsureWrite and
// confirms it converges without duplicating bytes.
func TestECStripeEnsureWriteRetriesPartialFanout(t *testing.T) {
	withECColumn(t, 64)
	v, plogs := ecVlogOnPlogs(t, 4, 2)
	ctx := context.Background()

	src := make([]byte, int(v.stripeWidth())*2)
	rand.New(rand.NewSource(11)).Read(src)

	// Simulate a partial earlier attempt: row 0's column 0 already landed on data
	// plog 0 (column width bytes), the rest of the fan-out never happened.
	if _, err := plogs[0].Write(1, src[:ecColumnBytes]); err != nil {
		t.Fatal(err)
	}

	if err := v.EnsureWrite(ctx, 0, [][]byte{src}); err != nil {
		t.Fatalf("ensure write: %v", err)
	}
	if err := v.Commit(ctx, 1); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// A second identical replay after restart must converge against the persisted
	// bytes rather than appending duplicates.
	v2, _ := reopenEC(t, plogs, 4, 2, 0)
	if err := v2.EnsureWrite(ctx, 0, [][]byte{src}); err != nil {
		t.Fatalf("ensure write replay: %v", err)
	}
	if got := v2.Length(); got != int64(len(src)) {
		t.Fatalf("length = %d, want %d", got, len(src))
	}
	got, err := v2.Read(ctx, 0, len(src))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatal("round-trip mismatch after retried ensure write")
	}
}

// TestECStripeReopen confirms committed rows survive a restart: the vlog is
// reconstructed at its recorded length over freshly reopened plogs.
func TestECStripeReopen(t *testing.T) {
	withECColumn(t, 64)
	v, plogs := ecVlogOnPlogs(t, 3, 1)
	ctx := context.Background()

	src := make([]byte, int(v.stripeWidth())*4)
	rand.New(rand.NewSource(13)).Read(src)
	if _, err := v.Write(ctx, 1, src); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := v.Commit(ctx, 1); err != nil {
		t.Fatalf("commit: %v", err)
	}

	v2, _ := reopenEC(t, plogs, 3, 1, v.Length())
	got, err := v2.Read(ctx, 0, len(src))
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatal("data did not survive reopen")
	}
}

// reopenEC closes the given plogs and reopens them at the same paths, returning
// a fresh EC vlog at the supplied length -- the restart path.
func reopenEC(t *testing.T, plogs []*Plog, data, parity int, length int64) (*Vlog, []*Plog) {
	t.Helper()
	reopened := make([]*Plog, len(plogs))
	clients := make([]PlogClient, len(plogs))
	for i, p := range plogs {
		path := p.file.Name()
		id := p.id
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		np, err := OpenPlog(path, id)
		if err != nil {
			t.Fatal(err)
		}
		reopened[i] = np
		clients[i] = plogClientAdapter{np}
	}
	v, err := NewVlog(1, "EC", data, parity, clients, length)
	if err != nil {
		t.Fatal(err)
	}
	return v, reopened
}
