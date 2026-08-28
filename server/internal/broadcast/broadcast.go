// Package broadcast fans a single stream of updates out to many subscribers.
package broadcast

import (
	"sync"

	pb "github.com/adityabansal/tapstream/server/gen/tapstream/v1"
)

// Broadcaster delivers every published update to every subscriber.
//
// Subscribers own their channels: Broadcaster never closes one, because a
// close racing with a send in Publish would panic. Unsubscribe removes the
// channel under the same lock Publish holds, so once it returns no further
// send can be in flight.
type Broadcaster struct {
	mu      sync.Mutex
	clients map[chan *pb.DashboardUpdate]struct{}
}

func New() *Broadcaster {
	return &Broadcaster{clients: make(map[chan *pb.DashboardUpdate]struct{})}
}

// Subscribe registers ch for updates. Give it some buffer: a send that would
// block is dropped rather than delaying every other subscriber.
func (b *Broadcaster) Subscribe(ch chan *pb.DashboardUpdate) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients[ch] = struct{}{}
}

func (b *Broadcaster) Unsubscribe(ch chan *pb.DashboardUpdate) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.clients, ch)
}

// Publish sends update to every subscriber, skipping any whose buffer is
// full, and reports how many were skipped. Dropping keeps one stalled client
// from holding up the rest. Note that the buffer is a FIFO, so it is the
// newest update that is dropped and the queued older ones that survive; that
// is tolerable only because each update is a complete snapshot, so a
// subscriber drains its backlog and renders the last one.
//
// Every subscriber receives the same *pb.DashboardUpdate. Concurrent reads
// and marshalling are safe, but a published update must never be mutated.
func (b *Broadcaster) Publish(update *pb.DashboardUpdate) (dropped int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for ch := range b.clients {
		select {
		case ch <- update:
		default:
			dropped++
		}
	}
	return dropped
}

// Len reports the current subscriber count.
func (b *Broadcaster) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.clients)
}
