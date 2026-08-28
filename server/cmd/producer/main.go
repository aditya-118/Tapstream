// Command producer emits synthetic clickstream and order events to Kafka at a
// configurable rate, standing in for real storefront traffic.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "github.com/adityabansal/tapstream/server/gen/tapstream/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var categories = []string{"electronics", "apparel", "home", "books", "toys"}

// Events are emitted in batches on a ticker rather than one per tick, so the
// achievable rate is not capped by the writer's batch latency.
const ticksPerSecond = 10

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka bootstrap brokers")
	clickTopic := flag.String("click-topic", "clickstream.events", "topic for click events")
	orderTopic := flag.String("order-topic", "clickstream.orders", "topic for order events")
	rate := flag.Int("rate", 20, "click events per second")
	orderRatio := flag.Float64("order-ratio", 0.1, "fraction of clicks that also produce an order")
	flag.Parse()

	if *rate <= 0 {
		log.Fatal("-rate must be positive")
	}
	if *orderRatio < 0 || *orderRatio > 1 {
		log.Fatal("-order-ratio must be between 0 and 1")
	}

	// Topic is set per message so one writer can serve both topics. Hashing on
	// the key keeps a category's events on a single partition, which is what
	// makes their relative order meaningful downstream.
	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(*brokers, ",")...),
		Balancer:     &kafka.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		// Must be set explicitly. A Writer struct literal leaves this at
		// RequireNone, where kafka-go never reads the produce response and
		// every write reports success - so a broker rejecting records looks
		// identical to one accepting them. (The deprecated NewWriter path
		// defaults to RequireAll, which is what makes this easy to miss.)
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("producing %d clicks/sec (order ratio %.2f) to %s and %s on %s",
		*rate, *orderRatio, *clickTopic, *orderTopic, *brokers)

	ticker := time.NewTicker(time.Second / ticksPerSecond)
	defer ticker.Stop()
	report := time.NewTicker(time.Second)
	defer report.Stop()

	// Carries the fractional remainder so rates that do not divide evenly
	// across ticks still average out to the requested rate.
	var owed float64
	perTick := float64(*rate) / ticksPerSecond
	var seq, clicks, orders, lastClicks, lastOrders int

	for {
		select {
		case <-ctx.Done():
			log.Printf("stopped after %d clicks, %d orders", clicks, orders)
			return

		case <-report.C:
			log.Printf("%d clicks/sec, %d orders/sec (%d / %d total)",
				clicks-lastClicks, orders-lastOrders, clicks, orders)
			lastClicks, lastOrders = clicks, orders

		case <-ticker.C:
			owed += perTick
			n := int(owed)
			owed -= float64(n)
			if n == 0 {
				continue
			}

			msgs := make([]kafka.Message, 0, n*2)
			batchOrders := 0
			for range n {
				seq++
				click := newClick(seq)
				msgs = append(msgs, message(*clickTopic, click.GetCategory(), click))

				// A click converts into an order some of the time, so orders
				// share the category distribution of the traffic that drove them.
				if rand.Float64() < *orderRatio {
					order := newOrder(seq, click.GetCategory())
					msgs = append(msgs, message(*orderTopic, order.GetCategory(), order))
					batchOrders++
				}
			}

			if err := w.WriteMessages(ctx, msgs...); err != nil {
				if ctx.Err() != nil {
					log.Printf("stopped after %d clicks, %d orders", clicks, orders)
					return
				}
				log.Printf("write: %v", err)
				continue
			}
			clicks += n
			orders += batchOrders
		}
	}
}

func message(topic, key string, m proto.Message) kafka.Message {
	value, err := proto.Marshal(m)
	if err != nil {
		log.Fatalf("marshal %T: %v", m, err)
	}
	return kafka.Message{Topic: topic, Key: []byte(key), Value: value}
}

func newClick(seq int) *pb.ClickEvent {
	category := categories[rand.Intn(len(categories))]
	return &pb.ClickEvent{
		EventId:   fmt.Sprintf("evt-%d", seq),
		SessionId: fmt.Sprintf("sess-%d", rand.Intn(50)),
		Page:      fmt.Sprintf("/%s/p/%d", category, rand.Intn(200)),
		Category:  category,
		Ts:        timestamppb.Now(),
	}
}

func newOrder(seq int, category string) *pb.OrderEvent {
	return &pb.OrderEvent{
		OrderId:  fmt.Sprintf("ord-%d", seq),
		Category: category,
		Amount:   float64(rand.Intn(49500)+500) / 100, // $5.00 - $499.99
		Ts:       timestamppb.Now(),
	}
}
