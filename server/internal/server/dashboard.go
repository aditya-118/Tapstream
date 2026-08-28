// Package server implements the DashboardService streaming API.
package server

import (
	"context"
	"net/http"
	"time"

	"connectrpc.com/connect"
	pb "github.com/adityabansal/tapstream/server/gen/tapstream/v1"
	"github.com/adityabansal/tapstream/server/internal/broadcast"
)

// subscriberBuffer is how far a client may fall behind before its updates are
// dropped. A few seconds of slack absorbs a scheduling hiccup without letting
// a stalled client accumulate stale frames.
const subscriberBuffer = 8

// writeTimeout bounds a single Send. The server sets no WriteTimeout, because
// that would cut off healthy long-lived streams; without any deadline though,
// a client that stops reading without closing its connection parks this
// handler in a socket write forever, leaking the goroutine and its
// subscription. A deadline refreshed per send bounds the wedge instead.
const writeTimeout = 30 * time.Second

type controllerKey struct{}

// WithWriteDeadlines makes the underlying connection's write deadline
// reachable from the handler. connect-go deliberately hides the
// http.ResponseWriter, so the deadline has to be captured on the way in.
func WithWriteDeadlines(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), controllerKey{}, http.NewResponseController(w))
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}

type Dashboard struct {
	broadcaster *broadcast.Broadcaster
}

func NewDashboard(b *broadcast.Broadcaster) *Dashboard {
	return &Dashboard{broadcaster: b}
}

// SubscribeUpdates streams snapshots until the client goes away.
func (d *Dashboard) SubscribeUpdates(
	ctx context.Context,
	req *connect.Request[pb.SubscribeUpdatesRequest],
	stream *connect.ServerStream[pb.DashboardUpdate],
) error {
	ch := make(chan *pb.DashboardUpdate, subscriberBuffer)
	d.broadcaster.Subscribe(ch)
	defer d.broadcaster.Unsubscribe(ch)

	allowed := allowedSet(req.Msg.GetCategories())

	for {
		select {
		case <-ctx.Done():
			// The client disconnected, or the server is shutting down.
			return nil

		case update := <-ch:
			setWriteDeadline(ctx, time.Now().Add(writeTimeout))
			if err := stream.Send(filter(update, allowed)); err != nil {
				return err
			}
		}
	}
}

func setWriteDeadline(ctx context.Context, t time.Time) {
	if rc, ok := ctx.Value(controllerKey{}).(*http.ResponseController); ok {
		_ = rc.SetWriteDeadline(t)
	}
}

func allowedSet(categories []string) map[string]bool {
	if len(categories) == 0 {
		return nil // no filter
	}
	allowed := make(map[string]bool, len(categories))
	for _, c := range categories {
		allowed[c] = true
	}
	return allowed
}

// filter narrows a snapshot to the requested categories, recomputing the total
// so it stays the sum of what the message actually contains. A subscription
// that matches nothing still gets ticking snapshots reading zero, rather than
// a stream that silently goes quiet.
//
// CategoryStats pointers are shared with the published update and with every
// other subscriber, so nothing here may mutate them.
func filter(update *pb.DashboardUpdate, allowed map[string]bool) *pb.DashboardUpdate {
	if allowed == nil {
		return update
	}

	out := &pb.DashboardUpdate{
		Ts:            update.GetTs(),
		WindowSeconds: update.GetWindowSeconds(),
		Total:         &pb.CategoryStats{},
	}
	for _, stats := range update.GetCategories() {
		if !allowed[stats.GetCategory()] {
			continue
		}
		out.Categories = append(out.Categories, stats)
		out.Total.ClicksPerSec += stats.GetClicksPerSec()
		out.Total.OrdersPerSec += stats.GetOrdersPerSec()
		out.Total.RevenuePerSec += stats.GetRevenuePerSec()
	}
	return out
}
