// Command aggregator consumes clickstream events from Kafka.
//
// At this stage it only decodes and logs them; windowed aggregation and the
// streaming API are layered on next.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/adityabansal/tapstream/server/internal/consume"
)

func main() {
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

	log.Printf("consuming %s and %s as group %q", *clickTopic, *orderTopic, *group)

	var n int
	for event := range c.Run(ctx) {
		n++
		if event.Kind == consume.KindOrder {
			log.Printf("%-5s %-12s %-14s $%.2f", event.Kind, event.ID, event.Category, event.Amount)
		} else {
			log.Printf("%-5s %-12s %-14s", event.Kind, event.ID, event.Category)
		}
	}
	log.Printf("stopped after %d events", n)
}
