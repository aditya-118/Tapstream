// Command aggregator consumes clickstream events from Kafka, folds them into
// sliding-window rollups, and streams those rollups to dashboard clients.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	connectcors "connectrpc.com/cors"
	pb "github.com/adityabansal/tapstream/server/gen/tapstream/v1"
	"github.com/adityabansal/tapstream/server/gen/tapstream/v1/tapstreamv1connect"
	"github.com/adityabansal/tapstream/server/internal/broadcast"
	"github.com/adityabansal/tapstream/server/internal/consume"
	"github.com/adityabansal/tapstream/server/internal/server"
	"github.com/adityabansal/tapstream/server/internal/window"
	"github.com/rs/cors"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	if err := run(); err != nil {
		log.Printf("aggregator: %v", err)
		os.Exit(1)
	}
}

func run() error {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka bootstrap brokers")
	group := flag.String("group", "aggregator", "Kafka consumer group id")
	clickTopic := flag.String("click-topic", "clickstream.events", "topic for click events")
	orderTopic := flag.String("order-topic", "clickstream.orders", "topic for order events")
	addr := flag.String("addr", ":8080", "address to serve the streaming API on")
	origin := flag.String("allowed-origin", "http://localhost:3000", "CORS allowed origin, or * for any")
	verbose := flag.Bool("v", false, "log each snapshot")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// Runs before the server shutdown deferred below, so a second Ctrl-C
	// during shutdown kills the process instead of being swallowed.
	defer stop()

	c := &consume.Consumer{
		Brokers:    strings.Split(*brokers, ","),
		GroupID:    *group,
		ClickTopic: *clickTopic,
		OrderTopic: *orderTopic,
	}
	registry := window.NewRegistry()
	broadcaster := broadcast.New()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           withCORS(dashboardHandler(broadcaster), *origin),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		// No WriteTimeout: these responses are long-lived streams. The
		// handler sets a per-send deadline instead.
		//
		// Streams end when their request context is cancelled, which happens
		// when the base context is cancelled on shutdown.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	serveErr := make(chan error, 1)
	go func() {
		log.Printf("serving DashboardService on %s (allowed origin %s)", *addr, *origin)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Reported rather than fatal, so the deferred Kafka reader
			// shutdown still runs and the consumer group is left cleanly.
			serveErr <- fmt.Errorf("serve: %w", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("consuming %s and %s as group %q", *clickTopic, *orderTopic, *group)

	events, errs := c.Run(ctx)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var failure error
	var staleSince time.Time
	fail := func(err error) {
		if failure == nil {
			failure = err
		}
		stop()
	}

	for events != nil {
		select {
		case event, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			// Bucketed by arrival, not by the event's own timestamp: see the
			// note in the window package. That makes a backlog indistinguishable
			// from live traffic in the numbers, so say so out loud instead.
			now := time.Now()
			staleSince = warnIfStale(event, now, staleSince)

			switch event.Kind {
			case consume.KindOrder:
				registry.RecordOrder(now, event.Category, event.Amount)
			case consume.KindClick:
				registry.RecordClick(now, event.Category)
			default:
				log.Printf("ignoring unknown event kind %v", event.Kind)
			}

		case err := <-errs:
			if err != nil {
				// One reader failing leaves the other running on half the
				// input, which looks healthy. Stop everything and exit
				// non-zero.
				fail(err)
			}

		case err := <-serveErr:
			fail(err)

		case now := <-ticker.C:
			update := snapshot(registry, now)
			if dropped := broadcaster.Publish(update); dropped > 0 {
				log.Printf("dropped update for %d slow subscriber(s)", dropped)
			}
			if *verbose {
				log.Printf("%d subscriber(s): %.1f clicks/s, %.1f orders/s, $%.2f/s",
					broadcaster.Len(), update.GetTotal().GetClicksPerSec(),
					update.GetTotal().GetOrdersPerSec(), update.GetTotal().GetRevenuePerSec())
			}
		}
	}
	return failure
}

func dashboardHandler(b *broadcast.Broadcaster) http.Handler {
	mux := http.NewServeMux()
	path, handler := tapstreamv1connect.NewDashboardServiceHandler(server.NewDashboard(b))
	mux.Handle(path, handler)
	return server.WithWriteDeadlines(mux)
}

// withCORS is required for a browser on another origin to reach this API at
// all: without it the preflight fails before any RPC is attempted. Connect
// needs its own header allow-lists, which the connectcors package supplies.
//
// Served over HTTP/1.1, where Connect streams via chunked responses. Browsers
// require TLS for HTTP/2, so h2c would only help non-browser clients.
func withCORS(h http.Handler, origin string) http.Handler {
	return cors.New(cors.Options{
		AllowedOrigins: []string{origin},
		AllowedMethods: connectcors.AllowedMethods(),
		AllowedHeaders: connectcors.AllowedHeaders(),
		ExposedHeaders: connectcors.ExposedHeaders(),
		// Without this browsers re-run the preflight every few seconds, so
		// each stream reconnect pays an extra round trip.
		MaxAge: int((2 * time.Hour).Seconds()),
	}).Handler(h)
}

// warnIfStale reports when consumed events are older than the window, which
// happens when a restarted group drains a backlog. Those events land in the
// current arrival buckets and inflate the reported rate until they age out.
// Logged at most once per stale period to keep the drain from flooding stderr.
func warnIfStale(event consume.Event, now, staleSince time.Time) time.Time {
	age := now.Sub(event.Ts)
	if event.Ts.IsZero() || age < window.Size*time.Second {
		return time.Time{}
	}
	if staleSince.IsZero() {
		log.Printf("consuming a backlog: events are %s old, so rates will read high until it drains", age.Truncate(time.Second))
		return now
	}
	return staleSince
}

// snapshot converts the registry's trailing-window totals into one update.
func snapshot(r *window.Registry, now time.Time) *pb.DashboardUpdate {
	byCategory, total := r.Snapshot(now)

	// Stable order so the dashboard's series do not jump.
	categories := slices.Sorted(maps.Keys(byCategory))

	update := &pb.DashboardUpdate{
		Ts:            timestamppb.New(now),
		WindowSeconds: window.Size,
		Total:         stats("", total),
		Categories:    make([]*pb.CategoryStats, 0, len(categories)),
	}
	for _, category := range categories {
		update.Categories = append(update.Categories, stats(category, byCategory[category]))
	}
	return update
}

// stats averages a window total over the window length. The current second's
// bucket is only partially elapsed, so rates read up to 1.7% low - a constant
// offset within a run that shifts between runs, which is worth knowing before
// chasing it as a bug.
func stats(category string, t window.Totals) *pb.CategoryStats {
	return &pb.CategoryStats{
		Category:      category,
		ClicksPerSec:  float64(t.Clicks) / window.Size,
		OrdersPerSec:  float64(t.Orders) / window.Size,
		RevenuePerSec: t.Revenue / window.Size,
	}
}
