// Command producer emits synthetic clickstream events to Kafka at a
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
	topic := flag.String("topic", "clickstream.events", "topic to produce click events to")
	rate := flag.Int("rate", 20, "click events per second")
	flag.Parse()

	if *rate <= 0 {
		log.Fatal("-rate must be positive")
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(*brokers, ",")...),
		Topic:        *topic,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer w.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("producing %d events/sec to %s on %s", *rate, *topic, *brokers)

	ticker := time.NewTicker(time.Second / ticksPerSecond)
	defer ticker.Stop()
	report := time.NewTicker(time.Second)
	defer report.Stop()

	// Carries the fractional remainder so rates that do not divide evenly
	// across ticks still average out to the requested rate.
	var owed float64
	perTick := float64(*rate) / ticksPerSecond
	var seq, sent, lastReport int

	for {
		select {
		case <-ctx.Done():
			log.Printf("stopped after %d events", sent)
			return

		case <-report.C:
			log.Printf("%d events/sec (%d total)", sent-lastReport, sent)
			lastReport = sent

		case <-ticker.C:
			owed += perTick
			n := int(owed)
			owed -= float64(n)
			if n == 0 {
				continue
			}

			msgs := make([]kafka.Message, n)
			for i := range msgs {
				seq++
				value, err := proto.Marshal(newClick(seq))
				if err != nil {
					log.Fatalf("marshal click event: %v", err)
				}
				msgs[i] = kafka.Message{Value: value}
			}

			if err := w.WriteMessages(ctx, msgs...); err != nil {
				if ctx.Err() != nil {
					log.Printf("stopped after %d events", sent)
					return
				}
				log.Printf("write: %v", err)
				continue
			}
			sent += n
		}
	}
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
