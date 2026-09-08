package server

import (
	"context"
	"testing"
	"time"

	"github.com/rmmh/rose/storage"
)

type pausedScrubClient struct {
	*localPlogClient
	entered chan struct{}
	resume  chan struct{}
}

func (c *pausedScrubClient) Scrub() (storage.ScrubResult, error) {
	close(c.entered)
	<-c.resume
	return c.localPlogClient.Scrub()
}

func TestScrubRetainsClientsUntilInspectionCompletes(t *testing.T) {
	s := newControlPlaneServer(t, 1)
	s.StopMaintenanceDriver()
	defer s.CloseStorage()
	ctx := context.Background()
	id := provision(t, s, "NONE", 1, 0)
	shards, err := s.db.VlogShardDisks(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	client := &pausedScrubClient{localPlogClient: &localPlogClient{plog: s.plogs[shards[0].PlogID]}, entered: make(chan struct{}), resume: make(chan struct{})}
	vlog, err := storage.NewVlog(id, "NONE", 1, 0, []storage.PlogClient{client}, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.vlogs[id] = vlog
	scrubDone := make(chan error, 1)
	go func() { _, err := s.Scrub(); scrubDone <- err }()
	<-client.entered
	compactDone := make(chan error, 1)
	go func() { compactDone <- s.CompactVlog(ctx, id) }()
	var premature bool
	select {
	case err := <-compactDone:
		premature = true
		t.Errorf("maintenance finished while source inspection was pending: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(client.resume)
	if err := <-scrubDone; err != nil {
		t.Errorf("inspection lost its backing client: %v", err)
	}
	if !premature {
		select {
		case err := <-compactDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("maintenance did not resume after inspection")
		}
	}
}
