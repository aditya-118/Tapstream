// Package window maintains sliding-window counts of events over the trailing
// minute, keyed by category.
package window

import (
	"sync"
	"time"
)

// Size is the window length in seconds, and the number of one-second buckets
// in the ring.
const Size = 60

// Totals is a window's contents at a point in time.
type Totals struct {
	Clicks  int64
	Orders  int64
	Revenue float64
}

// bucket holds one second of activity. sec records which epoch second the
// bucket belongs to, so a bucket left over from an earlier pass around the
// ring is recognisable as stale rather than being counted as current.
type bucket struct {
	sec     int64
	clicks  int64
	orders  int64
	revenue float64
}

// Stats is a ring of one-second buckets covering the trailing Size seconds.
// The zero value is ready to use and safe for concurrent use.
type Stats struct {
	mu      sync.Mutex
	buckets [Size]bucket
}

func (s *Stats) RecordClick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current(now).clicks++
}

func (s *Stats) RecordOrder(now time.Time, amount float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.current(now)
	b.orders++
	b.revenue += amount
}

// current returns the bucket for now, clearing it first if it still holds an
// older second. Callers must hold s.mu.
//
// sec%Size assumes a clock at or after the epoch; a negative second would
// index out of range. Not guarded, because reaching it requires a host clock
// set before 1970.
func (s *Stats) current(now time.Time) *bucket {
	sec := now.Unix()
	b := &s.buckets[sec%Size]
	if b.sec != sec {
		*b = bucket{sec: sec}
	}
	return b
}

// Sum totals the buckets covering the trailing window ending at now. Buckets
// outside that range are skipped, so a quiet period decays the totals to zero
// instead of leaving stale counts in the ring.
func (s *Stats) Sum(now time.Time) Totals {
	oldest, newest := now.Unix()-(Size-1), now.Unix()

	s.mu.Lock()
	defer s.mu.Unlock()

	var t Totals
	for _, b := range s.buckets {
		if b.sec < oldest || b.sec > newest {
			continue
		}
		t.Clicks += b.clicks
		t.Orders += b.orders
		t.Revenue += b.revenue
	}
	return t
}

// Registry holds one window per category. Categories appear at runtime, so the
// map needs its own lock in addition to the per-window one.
type Registry struct {
	mu         sync.RWMutex
	byCategory map[string]*Stats
}

func NewRegistry() *Registry {
	return &Registry{byCategory: make(map[string]*Stats)}
}

func (r *Registry) RecordClick(now time.Time, category string) {
	r.stats(category).RecordClick(now)
}

func (r *Registry) RecordOrder(now time.Time, category string, amount float64) {
	r.stats(category).RecordOrder(now, amount)
}

// Snapshot returns the trailing-window totals per category, and their sum.
// Categories whose window has decayed to nothing are omitted.
func (r *Registry) Snapshot(now time.Time) (map[string]Totals, Totals) {
	r.mu.RLock()
	stats := make(map[string]*Stats, len(r.byCategory))
	for category, s := range r.byCategory {
		stats[category] = s
	}
	r.mu.RUnlock()

	byCategory := make(map[string]Totals, len(stats))
	var total Totals
	for category, s := range stats {
		t := s.Sum(now)
		if t == (Totals{}) {
			continue
		}
		byCategory[category] = t
		total.Clicks += t.Clicks
		total.Orders += t.Orders
		total.Revenue += t.Revenue
	}
	return byCategory, total
}

func (r *Registry) stats(category string) *Stats {
	r.mu.RLock()
	s, ok := r.byCategory[category]
	r.mu.RUnlock()
	if ok {
		return s
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok = r.byCategory[category]; ok {
		return s // another goroutine created it first
	}
	s = &Stats{}
	r.byCategory[category] = s
	return s
}
