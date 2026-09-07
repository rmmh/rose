package server

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestOperationLockSerializesWaitersAndReclaimsEntries(t *testing.T) {
	s := &Server{writeOps: make(map[int64]*operationLock)}
	var active [4]atomic.Int32
	var collided atomic.Bool
	var workers sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 64; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for round := 0; round < 100; round++ {
				id := int64(worker % 4)
				release := s.writeOperationLock(id)
				if active[id].Add(1) != 1 {
					collided.Store(true)
				}
				runtime.Gosched()
				active[id].Add(-1)
				release()
			}
		}(worker)
	}
	close(start)
	workers.Wait()
	if collided.Load() {
		t.Fatal("same operation used distinct locks concurrently")
	}
	if len(s.writeOps) != 0 {
		t.Fatalf("completed operations retained %d locks", len(s.writeOps))
	}
	for id := int64(0); id < 1000; id++ {
		release := s.writeOperationLock(id)
		release()
	}
	if len(s.writeOps) != 0 {
		t.Fatal("terminal operation IDs grew the registry")
	}
}
