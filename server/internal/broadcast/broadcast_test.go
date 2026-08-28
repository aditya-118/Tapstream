package broadcast

import (
	"sync"
	"testing"

	pb "github.com/adityabansal/tapstream/server/gen/tapstream/v1"
)

func update(seconds int64) *pb.DashboardUpdate {
	return &pb.DashboardUpdate{WindowSeconds: seconds}
}

func TestPublishReachesEverySubscriber(t *testing.T) {
	b := New()
	a := make(chan *pb.DashboardUpdate, 1)
	c := make(chan *pb.DashboardUpdate, 1)
	b.Subscribe(a)
	b.Subscribe(c)

	if dropped := b.Publish(update(1)); dropped != 0 {
		t.Fatalf("dropped = %d, want 0", dropped)
	}
	for name, ch := range map[string]chan *pb.DashboardUpdate{"a": a, "c": c} {
		if got := <-ch; got.GetWindowSeconds() != 1 {
			t.Errorf("%s received %d, want 1", name, got.GetWindowSeconds())
		}
	}
}

// The point of the non-blocking send: one subscriber that never reads must not
// delay delivery to the others.
func TestSlowSubscriberDoesNotBlockOthers(t *testing.T) {
	b := New()
	slow := make(chan *pb.DashboardUpdate, 1) // filled and never drained
	fast := make(chan *pb.DashboardUpdate, 8)
	b.Subscribe(slow)
	b.Subscribe(fast)

	for i := range 5 {
		b.Publish(update(int64(i)))
	}

	if len(fast) != 5 {
		t.Errorf("fast subscriber got %d updates, want 5", len(fast))
	}
	if len(slow) != 1 {
		t.Errorf("slow subscriber buffered %d updates, want 1", len(slow))
	}
}

func TestPublishReportsDrops(t *testing.T) {
	b := New()
	full := make(chan *pb.DashboardUpdate, 1)
	b.Subscribe(full)

	if dropped := b.Publish(update(1)); dropped != 0 {
		t.Fatalf("first publish dropped = %d, want 0", dropped)
	}
	if dropped := b.Publish(update(2)); dropped != 1 {
		t.Errorf("second publish dropped = %d, want 1", dropped)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	b := New()
	ch := make(chan *pb.DashboardUpdate, 4)
	b.Subscribe(ch)
	b.Unsubscribe(ch)

	b.Publish(update(1))

	if b.Len() != 0 {
		t.Errorf("Len = %d, want 0", b.Len())
	}
	if len(ch) != 0 {
		t.Errorf("received %d updates after unsubscribing, want 0", len(ch))
	}
}

// Run with -race: subscribing, unsubscribing and publishing must be safe
// concurrently, which is exactly what happens as clients connect and drop.
func TestConcurrentSubscribeUnsubscribePublish(t *testing.T) {
	b := New()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range 1000 {
			b.Publish(update(int64(i)))
		}
	}()

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				ch := make(chan *pb.DashboardUpdate, 4)
				b.Subscribe(ch)
				b.Unsubscribe(ch)
			}
		}()
	}
	wg.Wait()

	if b.Len() != 0 {
		t.Errorf("Len = %d, want 0", b.Len())
	}
}
