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
