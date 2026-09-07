package main

import (
	"reflect"
	"testing"
)

func TestParseDataDirsAcceptsCommaAndBraceExpandedArgs(t *testing.T) {
	got := parseDataDirs("/dev/shm/r/d1", []string{"/dev/shm/r/d2", "/dev/shm/r/d3,/dev/shm/r/d4"})
	want := []string{"/dev/shm/r/d1", "/dev/shm/r/d2", "/dev/shm/r/d3", "/dev/shm/r/d4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseDataDirs = %#v, want %#v", got, want)
	}
}

func TestSnapshotRetentionConfiguration(t *testing.T) {
	p, err := parseSnapshotRetention("24h,720h,8760h")
	if err != nil || p == nil || p.Weekly.Hours() != 8760 {
		t.Fatalf("policy=%v err=%v", p, err)
	}
	if p, err := parseSnapshotRetention("off"); err != nil || p != nil {
		t.Fatalf("disable=%v err=%v", p, err)
	}
	for _, invalid := range []string{"", "24h,1h,720h", "-1h,24h,720h", "24h,720h", "1d,30d,365d"} {
		if _, err := parseSnapshotRetention(invalid); err == nil {
			t.Fatalf("invalid policy %q accepted", invalid)
		}
	}
}
