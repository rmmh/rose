package storage

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type writeFailingPlogClient struct {
	PlogClient
	fail bool
}

func (a plogClientAdapter) TruncateTo(logical int64) error { return a.p.TruncateTo(logical) }

func (c *writeFailingPlogClient) Write(ctx context.Context, txnID int64, data []byte) (int64, error) {
	if c.fail {
		return 0, fmt.Errorf("injected write failure")
	}
	return c.PlogClient.Write(ctx, txnID, data)
}

func (c *writeFailingPlogClient) EnsureAppend(ctx context.Context, offset int64, data []byte) error {
	if c.fail {
		return fmt.Errorf("injected write failure")
	}
	positioned, ok := c.PlogClient.(positionedPlogClient)
	if !ok {
		return fmt.Errorf("wrapped client does not support positioned writes")
	}
	return positioned.EnsureAppend(ctx, offset, data)
}

func (c *writeFailingPlogClient) Commit(ctx context.Context, txnID int64) error {
	committer, ok := c.PlogClient.(committingPlogClient)
	if !ok {
		return fmt.Errorf("wrapped client does not support commit")
	}
	return committer.Commit(ctx, txnID)
}

type commitFailingPlogClient struct {
	PlogClient
	fail bool
}

func (c *commitFailingPlogClient) Commit(ctx context.Context, txnID int64) error {
	if c.fail {
		return fmt.Errorf("injected commit failure")
	}
	committer, ok := c.PlogClient.(committingPlogClient)
	if !ok {
		return fmt.Errorf("wrapped client does not support commit")
	}
	return committer.Commit(ctx, txnID)
}

func TestPlogEnsureAppendRetriesPartialRange(t *testing.T) {
	p, err := OpenPlog(filepath.Join(t.TempDir(), "plog"), 1)
	require.NoError(t, err)
	defer p.Close()
	data := bytes.Repeat([]byte("retry-safe"), 900)
	_, err = p.Write(1, data[:4096])
	require.NoError(t, err)
	require.NoError(t, p.EnsureAppend(0, data))
	require.NoError(t, p.EnsureAppend(0, data))
	assert.Equal(t, int64(len(data)), p.LogicalLength())
	got, err := p.Read(0, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestVlogEnsureWriteRetriesPartialReplicaFanout(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenPlog(filepath.Join(dir, "a"), 1)
	require.NoError(t, err)
	defer a.Close()
	b, err := OpenPlog(filepath.Join(dir, "b"), 2)
	require.NoError(t, err)
	defer b.Close()
	v, err := NewVlog(1, "DUPLICATE", 0, 0, []PlogClient{plogClientAdapter{a}, plogClientAdapter{b}}, 0)
	require.NoError(t, err)
	data := bytes.Repeat([]byte("fanout"), 1200)
	_, err = a.Write(1, data) // simulate a prior partial fan-out
	require.NoError(t, err)
	require.NoError(t, v.EnsureWrite(context.Background(), 0, [][]byte{data}))
	require.NoError(t, v.EnsureWrite(context.Background(), v.Length(), [][]byte{data}))
	assert.Equal(t, int64(2*len(data)), v.Length())
	got, err := v.Read(context.Background(), 0, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestDuplicateVlogWritePartialFanoutRetryDoesNotAdvanceLength(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenPlog(filepath.Join(dir, "a"), 1)
	require.NoError(t, err)
	defer a.Close()
	b, err := OpenPlog(filepath.Join(dir, "b"), 2)
	require.NoError(t, err)
	defer b.Close()

	failing := &writeFailingPlogClient{PlogClient: plogClientAdapter{b}, fail: true}
	v, err := NewVlog(1, "DUPLICATE", 0, 0, []PlogClient{plogClientAdapter{a}, failing}, 0)
	require.NoError(t, err)
	data := bytes.Repeat([]byte("partial-fanout"), 700)
	_, err = v.Write(context.Background(), 1, data)
	require.Error(t, err)
	assert.Zero(t, v.Length())

	failing.fail = false
	offset, err := v.Write(context.Background(), 1, data)
	require.NoError(t, err)
	assert.Zero(t, offset)
	assert.Equal(t, int64(len(data)), v.Length())
	next := bytes.Repeat([]byte("next-record"), 300)
	nextOffset, err := v.Write(context.Background(), 2, next)
	require.NoError(t, err)
	assert.Equal(t, int64(len(data)), nextOffset)
	for name, p := range map[string]*Plog{"a": a, "b": b} {
		assert.Equal(t, int64(len(data)+len(next)), p.LogicalLength(), "plog %s length", name)
	}
	got, err := v.Read(context.Background(), nextOffset, len(next))
	require.NoError(t, err)
	assert.Equal(t, next, got)
}

func TestECVlogFailedWriteReconcileTruncatesUncommittedShardTails(t *testing.T) {
	restore := SetECColumnBytesForTest(SectorSize)
	defer restore()

	dir := t.TempDir()
	plogs := make([]*Plog, 3)
	clients := make([]PlogClient, 3)
	for i := range plogs {
		p, err := OpenPlog(filepath.Join(dir, fmt.Sprintf("plog-%d", i)), uint32(i))
		require.NoError(t, err)
		plogs[i] = p
		clients[i] = plogClientAdapter{p}
	}
	failing := &writeFailingPlogClient{PlogClient: clients[2], fail: true}
	clients[2] = failing
	v, err := NewVlog(1, "EC", 2, 1, clients, 0)
	require.NoError(t, err)
	src := make([]byte, int(v.stripeWidth()))
	for i := range src {
		src[i] = byte(i*17 + 3)
	}
	_, err = v.Write(context.Background(), 1, src)
	require.Error(t, err)
	assert.Zero(t, v.Length())

	paths := make([]string, len(plogs))
	for i, p := range plogs {
		paths[i] = p.file.Name()
		require.NoError(t, p.Close())
	}
	reopened := make([]*Plog, len(plogs))
	reopenedClients := make([]PlogClient, len(plogs))
	for i, path := range paths {
		p, err := OpenPlog(path, uint32(i))
		require.NoError(t, err)
		defer p.Close()
		reopened[i] = p
		reopenedClients[i] = plogClientAdapter{p}
	}
	v2, err := NewVlog(1, "EC", 2, 1, reopenedClients, 0)
	require.NoError(t, err)
	require.NoError(t, v2.ReconcileShardLengths())
	for i, p := range reopened {
		assert.Zero(t, p.LogicalLength(), "reconciled plog %d length", i)
	}
}

func TestECVlogWritePartialFanoutRetryUsesSameShardOffsets(t *testing.T) {
	restore := SetECColumnBytesForTest(SectorSize)
	defer restore()

	dir := t.TempDir()
	plogs := make([]*Plog, 3)
	clients := make([]PlogClient, 3)
	for i := range plogs {
		p, err := OpenPlog(filepath.Join(dir, fmt.Sprintf("plog-%d", i)), uint32(i))
		require.NoError(t, err)
		defer p.Close()
		plogs[i] = p
		clients[i] = plogClientAdapter{p}
	}
	failing := &writeFailingPlogClient{PlogClient: clients[2], fail: true}
	clients[2] = failing
	v, err := NewVlog(1, "EC", 2, 1, clients, 0)
	require.NoError(t, err)
	first := make([]byte, int(v.stripeWidth()))
	second := make([]byte, int(v.stripeWidth()))
	for i := range first {
		first[i] = byte(i*17 + 3)
		second[i] = byte(i*29 + 11)
	}

	_, err = v.Write(context.Background(), 1, first)
	require.Error(t, err)
	assert.Zero(t, v.Length())
	failing.fail = false
	offset, err := v.Write(context.Background(), 1, first)
	require.NoError(t, err)
	assert.Zero(t, offset)
	nextOffset, err := v.Write(context.Background(), 2, second)
	require.NoError(t, err)
	assert.Equal(t, int64(len(first)), nextOffset)
	require.NoError(t, v.Commit(context.Background(), 2))

	for i, p := range plogs {
		assert.Equal(t, 2*ecColumnBytes, p.LogicalLength(), "plog %d length", i)
	}
	got, err := v.Read(context.Background(), nextOffset, len(second))
	require.NoError(t, err)
	assert.Equal(t, second, got)
}

func TestECStripeEnsureWriteRetriesMultiRowPartialFanout(t *testing.T) {
	withECColumn(t, 64)
	v, plogs := ecVlogOnPlogs(t, 4, 2)
	ctx := context.Background()

	src := make([]byte, int(v.stripeWidth())*3)
	for i := range src {
		src[i] = byte(i*31 + 7)
	}

	for row := 0; row < 2; row++ {
		shards, err := v.encodeRow(src[row*int(v.stripeWidth()) : (row+1)*int(v.stripeWidth())])
		require.NoError(t, err)
		_, err = plogs[0].Write(1, shards[0])
		require.NoError(t, err)
	}
	shards, err := v.encodeRow(src[:int(v.stripeWidth())])
	require.NoError(t, err)
	_, err = plogs[5].Write(1, shards[5])
	require.NoError(t, err)

	require.NoError(t, v.EnsureWrite(ctx, 0, [][]byte{src[:100], src[100:]}))
	require.NoError(t, v.Commit(ctx, 1))
	v2, _ := reopenEC(t, plogs, 4, 2, v.Length())
	got, err := v2.Read(ctx, 0, len(src))
	require.NoError(t, err)
	assert.Equal(t, src, got)
}

func TestVlogCommitFailureCanBeRetriedAndReopened(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenPlog(filepath.Join(dir, "a"), 1)
	require.NoError(t, err)
	b, err := OpenPlog(filepath.Join(dir, "b"), 2)
	require.NoError(t, err)
	failing := &commitFailingPlogClient{PlogClient: plogClientAdapter{b}, fail: true}
	v, err := NewVlog(1, "DUPLICATE", 0, 0, []PlogClient{plogClientAdapter{a}, failing}, 0)
	require.NoError(t, err)
	data := bytes.Repeat([]byte("commit-retry"), 800)
	_, err = v.Write(context.Background(), 1, data)
	require.NoError(t, err)
	require.Error(t, v.Commit(context.Background(), 1))
	failing.fail = false
	require.NoError(t, v.Commit(context.Background(), 1))

	pathA, pathB := a.file.Name(), b.file.Name()
	require.NoError(t, a.Close())
	require.NoError(t, b.Close())
	ra, err := OpenPlog(pathA, 1)
	require.NoError(t, err)
	defer ra.Close()
	rb, err := OpenPlog(pathB, 2)
	require.NoError(t, err)
	defer rb.Close()
	v2, err := NewVlog(1, "DUPLICATE", 0, 0, []PlogClient{plogClientAdapter{ra}, plogClientAdapter{rb}}, int64(len(data)))
	require.NoError(t, err)
	got, err := v2.Read(context.Background(), 0, len(data))
	require.NoError(t, err)
	assert.Equal(t, data, got)
}
