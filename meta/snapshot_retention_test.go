package meta

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotRetentionTiersAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0).Add(28 * 24 * time.Hour)
	hours := []int{27*24 + 12, 27*24 + 6, 26*24 + 18, 26*24 + 6, 24*24 + 12, 19 * 24, 18 * 24, 12 * 24, 10 * 24, 5 * 24, 29 * 24}
	kept := map[int]bool{0: true, 1: true, 2: true, 4: true, 5: true, 7: true, 10: true}
	ids := make([]uint64, len(hours))
	for i, h := range hours {
		ids[i], err = db.CreateSnapshot(ctx, fmt.Sprint(i), int64(time.Duration(h)*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
	}
	if expired, err := db.ExpireSnapshots(ctx, now); err != nil || len(expired) != 0 {
		t.Fatalf("unconfigured retention=%v err=%v", expired, err)
	}
	p := &SnapshotRetention{Continuous: 24 * time.Hour, Daily: 7 * 24 * time.Hour, Weekly: 21 * 24 * time.Hour}
	if err := db.SetSnapshotRetention(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expired, err := db.ExpireSnapshots(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	got := map[uint64]bool{}
	for _, id := range expired {
		got[id] = true
	}
	for i, id := range ids {
		if got[id] == kept[i] {
			t.Fatalf("snapshot %d: expired=%v want kept=%v", i, got[id], kept[i])
		}
	}
	entries, err := db.ListSnapshots(ctx)
	if err != nil || len(entries) != len(kept) {
		t.Fatalf("retained=%v err=%v", entries, err)
	}
	if expired, err := db.ExpireSnapshots(ctx, now); err != nil || len(expired) != 0 {
		t.Fatalf("repeat expiration=%v err=%v", expired, err)
	}
	if err := db.SetSnapshotRetention(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if expired, err := db.ExpireSnapshots(ctx, now.Add(100*24*time.Hour)); err != nil || len(expired) != 0 {
		t.Fatalf("disabled expiration=%v err=%v", expired, err)
	}
}

func TestSnapshotRetentionDeterministicTiesAndInvalidPolicy(t *testing.T) {
	ctx := context.Background()
	db, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Unix(0, 0).Add(10 * 24 * time.Hour)
	a, err := db.CreateSnapshot(ctx, "a", now.Add(-2*24*time.Hour).UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.CreateSnapshot(ctx, "b", now.Add(-2*24*time.Hour).UnixNano())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSnapshotRetention(ctx, &SnapshotRetention{Continuous: 2, Daily: 1, Weekly: 3}); err == nil {
		t.Fatal("invalid windows accepted")
	}
	if err := db.SetSnapshotRetention(ctx, &SnapshotRetention{Daily: 7 * 24 * time.Hour, Weekly: 14 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	expired, err := db.ExpireSnapshots(ctx, now)
	if err != nil || len(expired) != 1 || expired[0] != a {
		t.Fatalf("tie expiration=%v newer=%d err=%v", expired, b, err)
	}
}

func TestSnapshotRetentionWindowBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  time.Duration
		want int
	}{
		{"continuous inclusive", time.Hour, 0},
		{"after continuous", time.Hour + 1, 1},
		{"weekly inclusive", 14 * 24 * time.Hour, 1},
		{"after weekly", 14*24*time.Hour + 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := Open(filepath.Join(t.TempDir(), "meta.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			now := time.Unix(0, 0).Add(30 * 24 * time.Hour)
			for _, name := range []string{"a", "b"} {
				if _, err := db.CreateSnapshot(ctx, name, now.Add(-tc.age).UnixNano()); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.SetSnapshotRetention(ctx, &SnapshotRetention{Continuous: time.Hour, Daily: 7 * 24 * time.Hour, Weekly: 14 * 24 * time.Hour}); err != nil {
				t.Fatal(err)
			}
			expired, err := db.ExpireSnapshots(ctx, now)
			if err != nil || len(expired) != tc.want {
				t.Fatalf("expired=%v want=%d err=%v", expired, tc.want, err)
			}
		})
	}
}
