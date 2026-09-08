package durability

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

var injected = errors.New("injected boundary failure")

// Effects intentionally do not enforce protocol ordering. The test oracle
// independently checks that the stepper never exposes an unsafe publication.
type publicationEffects struct {
	ids                      []uint32
	cursor, synced, recorded map[uint32]int64
	verified, published      bool
	calls                    int
	failAt                   int
}

func newEffects(ids []uint32) *publicationEffects {
	p := &publicationEffects{ids: ids, cursor: map[uint32]int64{}, synced: map[uint32]int64{}, recorded: map[uint32]int64{}}
	for _, id := range ids {
		p.cursor[id] = int64(id) * 11
	}
	return p
}
func (p *publicationEffects) effect() error {
	p.calls++
	if p.calls == p.failAt {
		return injected
	}
	return nil
}
func (p *publicationEffects) Admit(context.Context) ([]uint32, error) { return p.ids, p.effect() }
func (p *publicationEffects) Sync(_ context.Context, id uint32) (int64, error) {
	if err := p.effect(); err != nil {
		return 0, err
	}
	p.synced[id] = p.cursor[id]
	// Simulate the cursor moving after sync. Only the returned prefix is durable.
	p.cursor[id]++
	return p.synced[id], nil
}
func (p *publicationEffects) RecordPrefix(_ context.Context, id uint32, prefix int64) error {
	if err := p.effect(); err != nil {
		return err
	}
	p.recorded[id] = prefix
	return nil
}
func (p *publicationEffects) Verify(context.Context) error {
	if err := p.effect(); err != nil {
		return err
	}
	p.verified = true
	return nil
}
func (p *publicationEffects) Publish(context.Context) error {
	if err := p.effect(); err != nil {
		return err
	}
	p.published = true
	return nil
}
func checkSafe(t *testing.T, p *publicationEffects) {
	t.Helper()
	for id, prefix := range p.recorded {
		if prefix != p.synced[id] {
			t.Fatalf("catalog prefix %d differs from synced %d", prefix, p.synced[id])
		}
	}
	if p.verified || p.published {
		for _, id := range p.ids {
			if p.recorded[id] == 0 || p.recorded[id] != p.synced[id] {
				t.Fatalf("validated before durable prefix recorded for %d", id)
			}
		}
	}
	if p.published && !p.verified {
		t.Fatal("published without placement verification")
	}
}

func TestPublicationEveryEffectFailure(t *testing.T) {
	for _, ids := range [][]uint32{nil, {1}, {2, 1}} {
		effects := 3 + 2*len(ids)
		for fail := 0; fail <= effects; fail++ {
			t.Run(fmt.Sprintf("vlogs=%v/fail=%d", ids, fail), func(t *testing.T) {
				p := newEffects(ids)
				p.failAt = fail
				c := Coordinator{Publication: p}
				txn := new(Transaction)
				var err error
				for !txn.Done() {
					err = c.Step(context.Background(), txn)
					checkSafe(t, p)
					if err != nil {
						break
					}
				}
				if fail == 0 {
					if err != nil || !p.published {
						t.Fatalf("commit=%v published=%v", err, p.published)
					}
					return
				}
				if !errors.Is(err, injected) || p.published {
					t.Fatalf("failed commit=%v published=%v", err, p.published)
				}
				calls := p.calls
				if !errors.Is(c.Step(context.Background(), txn), injected) || p.calls != calls {
					t.Fatal("failed invocation continued executing effects")
				}
			})
		}
	}
}

func TestPublicationHooksAndAmbiguousResult(t *testing.T) {
	expected := []Point{ShardsSynced, PrefixRecorded, ShardsSynced, PrefixRecorded, PlacementsVerified, NamespacePublished}
	for cut := 0; cut <= len(expected); cut++ {
		p := newEffects([]uint32{1, 2})
		var points []Point
		c := Coordinator{Publication: p, Hook: func(point Point) error {
			points = append(points, point)
			checkSafe(t, p)
			if len(points) == cut {
				return injected
			}
			return nil
		}}
		err := c.Commit(context.Background())
		if cut == 0 {
			if err != nil || !reflect.DeepEqual(points, expected) {
				t.Fatalf("hooks=%v err=%v", points, err)
			}
		} else {
			if !errors.Is(err, injected) || !reflect.DeepEqual(points, expected[:cut]) {
				t.Fatalf("cut=%d hooks=%v err=%v", cut, points, err)
			}
			if p.published != (cut == len(expected)) {
				t.Fatalf("cut=%d published=%v", cut, p.published)
			}
		}
	}
}
