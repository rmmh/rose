package server

import (
	"testing"

	"github.com/rmmh/rose/meta"
)

func TestCompactionPolicyCandidates(t *testing.T) {
	usages := []meta.VlogUsage{
		{VlogID: 1, TotalBytes: 100 << 20, LiveBytes: 95 << 20}, // 5% dead, skip on ratio
		{VlogID: 2, TotalBytes: 100 << 20, LiveBytes: 40 << 20}, // 60% dead, 60MB
		{VlogID: 3, TotalBytes: 100 << 20, LiveBytes: 70 << 20}, // 30% dead, 30MB
		{VlogID: 4, TotalBytes: 1 << 20, LiveBytes: 0},          // 100% dead but only 1MB, skip on floor
		{VlogID: 5, TotalBytes: 0, LiveBytes: 0},                // empty, skip
	}
	policy := CompactionPolicy{MinWasteRatio: 0.25, MinDeadBytes: 4 << 20, MaxJobs: 4}
	got := policy.Candidates(usages)

	if len(got) != 2 {
		t.Fatalf("candidates = %d, want 2: %+v", len(got), got)
	}
	// Most wasteful first.
	if got[0].VlogID != 2 || got[1].VlogID != 3 {
		t.Fatalf("candidate order = [%d, %d], want [2, 3]", got[0].VlogID, got[1].VlogID)
	}
}

func TestCompactionPolicyMaxJobsCaps(t *testing.T) {
	usages := []meta.VlogUsage{
		{VlogID: 1, TotalBytes: 100, LiveBytes: 0},
		{VlogID: 2, TotalBytes: 100, LiveBytes: 0},
		{VlogID: 3, TotalBytes: 100, LiveBytes: 0},
	}
	policy := CompactionPolicy{MinWasteRatio: 0.5, MinDeadBytes: 1, MaxJobs: 2}
	if got := policy.Candidates(usages); len(got) != 2 {
		t.Fatalf("MaxJobs cap = %d, want 2", len(got))
	}
}

func TestVlogUsageArithmetic(t *testing.T) {
	u := meta.VlogUsage{TotalBytes: 1000, LiveBytes: 250}
	if u.DeadBytes() != 750 {
		t.Fatalf("dead = %d, want 750", u.DeadBytes())
	}
	if u.WasteRatio() != 0.75 {
		t.Fatalf("waste = %v, want 0.75", u.WasteRatio())
	}
	// Live exceeding total (e.g. shared dedup accounting) clamps to zero dead.
	neg := meta.VlogUsage{TotalBytes: 100, LiveBytes: 140}
	if neg.DeadBytes() != 0 || neg.WasteRatio() != 0 {
		t.Fatalf("clamp failed: dead=%d waste=%v", neg.DeadBytes(), neg.WasteRatio())
	}
}
