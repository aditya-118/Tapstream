package window

import (
	"sync"
	"testing"
	"time"
)

// A fixed base time keeps the tests independent of the wall clock.
var base = time.Unix(1_700_000_000, 0)

func at(seconds int) time.Time { return base.Add(time.Duration(seconds) * time.Second) }

func TestSumCountsBurstWithinWindow(t *testing.T) {
	var s Stats
	for range 5 {
		s.RecordClick(at(0))
	}
	s.RecordOrder(at(0), 12.50)
	s.RecordOrder(at(1), 7.50)

	got := s.Sum(at(1))
	want := Totals{Clicks: 5, Orders: 2, Revenue: 20}
	if got != want {
		t.Errorf("Sum = %+v, want %+v", got, want)
	}
}

func TestSumWindowBoundary(t *testing.T) {
	var s Stats
	s.RecordClick(at(0))

	// The window is inclusive of both ends: [now-59, now].
	if got := s.Sum(at(Size - 1)); got.Clicks != 1 {
		t.Errorf("at the oldest second still in window: Clicks = %d, want 1", got.Clicks)
	}
	if got := s.Sum(at(Size)); got.Clicks != 0 {
		t.Errorf("one second after expiry: Clicks = %d, want 0", got.Clicks)
	}
}

// A bucket reused on the next pass around the ring must be cleared, not added
// to. Resetting only when the ring index changes misses this: t=0 and t=60
// map to the same index, so the older second's count would survive.
func TestBucketReusedNextMinuteIsCleared(t *testing.T) {
	var s Stats
	s.RecordClick(at(0))
	s.RecordOrder(at(0), 100)
	s.RecordClick(at(Size)) // same ring index as at(0), one minute later
	s.RecordOrder(at(Size), 5)

	// Every field must be cleared, not just the one the ring is indexed by.
	got := s.Sum(at(Size))
	want := Totals{Clicks: 1, Orders: 1, Revenue: 5}
	if got != want {
		t.Errorf("Sum = %+v, want %+v (the stale second must not be counted)", got, want)
	}
}

// Reading at a time before the recorded second - what a backward clock step
// produces - must not count buckets stamped in the future.
func TestClockStepBackwardExcludesFutureBuckets(t *testing.T) {
	var s Stats
	s.RecordClick(at(100))

	if got := s.Sum(at(50)); got != (Totals{}) {
		t.Errorf("Sum = %+v, want zero (the bucket is ahead of now)", got)
	}
	// Once the clock catches up the bucket counts again.
	if got := s.Sum(at(100)); got.Clicks != 1 {
		t.Errorf("Clicks = %d, want 1 once now has caught up", got.Clicks)
	}
}

// After a silence longer than the window every bucket is stale, so the totals
// must read zero rather than reporting a minute-old figure as current.
func TestSilenceDecaysToZero(t *testing.T) {
	var s Stats
	for range 10 {
		s.RecordClick(at(0))
	}
	s.RecordOrder(at(0), 99)

	if got := s.Sum(at(2 * Size)); got != (Totals{}) {
		t.Errorf("Sum after silence = %+v, want zero", got)
	}
}

// A gap shorter than the window must clear only the seconds that actually
// elapsed, leaving the rest of the window intact.
func TestGapClearsOnlyElapsedSeconds(t *testing.T) {
	var s Stats
	s.RecordClick(at(0))
	s.RecordClick(at(5)) // five second gap

	if got := s.Sum(at(5)); got.Clicks != 2 {
		t.Errorf("Clicks = %d, want 2 (both are still inside the window)", got.Clicks)
	}
}

func TestRegistryKeepsCategoriesSeparate(t *testing.T) {
	r := NewRegistry()
	r.RecordClick(at(0), "books")
	r.RecordClick(at(0), "books")
	r.RecordOrder(at(0), "toys", 30)

	byCategory, total := r.Snapshot(at(0))

	if got := byCategory["books"]; got != (Totals{Clicks: 2}) {
		t.Errorf("books = %+v, want {Clicks:2}", got)
	}
	if got := byCategory["toys"]; got != (Totals{Orders: 1, Revenue: 30}) {
		t.Errorf("toys = %+v, want {Orders:1 Revenue:30}", got)
	}
	if want := (Totals{Clicks: 2, Orders: 1, Revenue: 30}); total != want {
		t.Errorf("total = %+v, want %+v", total, want)
	}
}

func TestRegistryOmitsDecayedCategories(t *testing.T) {
	r := NewRegistry()
	r.RecordClick(at(0), "books")

	byCategory, total := r.Snapshot(at(2 * Size))
	if len(byCategory) != 0 {
		t.Errorf("byCategory = %+v, want empty", byCategory)
	}
	if total != (Totals{}) {
		t.Errorf("total = %+v, want zero", total)
	}
}

// Run with -race: recording and reading must not conflict, including the
// first write for a category that does not exist yet.
func TestConcurrentRecordAndSnapshot(t *testing.T) {
	r := NewRegistry()
	categories := []string{"books", "toys", "home", "apparel", "electronics"}

	var wg sync.WaitGroup
	for i, category := range categories {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := range 200 {
				r.RecordClick(at(j%Size), category)
				r.RecordOrder(at(j%Size), category, 1)
			}
		}()
		go func() {
			defer wg.Done()
			for j := range 200 {
				r.Snapshot(at((j + i) % Size))
			}
		}()
	}
	wg.Wait()
}
