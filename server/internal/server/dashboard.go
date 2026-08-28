// Package server implements the DashboardService streaming API.
package server

import (
	"context"

	"connectrpc.com/connect"
	pb "github.com/adityabansal/tapstream/server/gen/tapstream/v1"
	"github.com/adityabansal/tapstream/server/internal/broadcast"
)

// subscriberBuffer is how far a client may fall behind before its updates are
// dropped. A few seconds of slack absorbs a scheduling hiccup without letting
// a stalled client accumulate stale frames.
const subscriberBuffer = 8

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
			// The client disconnected or cancelled; not an error.
			return nil

		case update := <-ch:
			if err := stream.Send(filter(update, allowed)); err != nil {
				return err
			}
		}
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
