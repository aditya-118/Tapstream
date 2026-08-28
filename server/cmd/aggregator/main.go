// Command aggregator consumes clickstream events from Kafka and folds them
// into sliding-window rollups.
//
// At this stage the rollups are printed once a second; the streaming API is
// layered on next.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/adityabansal/tapstream/server/internal/consume"
	"github.com/adityabansal/tapstream/server/internal/window"
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
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := &consume.Consumer{
		Brokers:    strings.Split(*brokers, ","),
		GroupID:    *group,
		ClickTopic: *clickTopic,
		OrderTopic: *orderTopic,
	}
	registry := window.NewRegistry()

	log.Printf("consuming %s and %s as group %q", *clickTopic, *orderTopic, *group)

	events, errs := c.Run(ctx)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var failure error
	var staleSince time.Time

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
			if err == nil {
				continue
			}
			// One reader failing leaves the other running on half the input,
			// which looks healthy. Stop everything and exit non-zero.
			if failure == nil {
				failure = err
			}
			stop()

		case now := <-ticker.C:
			report(registry, now)
		}
	}
	return failure
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

// report prints the trailing-window rollup.
func report(r *window.Registry, now time.Time) {
	byCategory, total := r.Snapshot(now)
	if len(byCategory) == 0 {
		log.Printf("%ds window: idle", window.Size)
		return
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%ds window: %.1f clicks/s, %.1f orders/s, $%.2f/s\n",
		window.Size, perSecond(total.Clicks), perSecond(total.Orders), total.Revenue/window.Size)
	for _, category := range slices.Sorted(maps.Keys(byCategory)) {
		t := byCategory[category]
		fmt.Fprintf(&b, "    %-12s %7.1f clicks/s %6.1f orders/s %10.2f $/s\n",
			category, perSecond(t.Clicks), perSecond(t.Orders), t.Revenue/window.Size)
	}
	log.Print(b.String())
}

// perSecond averages a window total over the window length. The current
// second's bucket is only partially elapsed, so this reads up to 1.7% low -
// a constant offset within a run that shifts between runs, which is worth
// knowing before chasing it as a bug.
func perSecond(n int64) float64 { return float64(n) / window.Size }
