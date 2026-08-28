// Package consume reads clickstream and order events from Kafka and decodes
// them into a single stream, so callers do not care which topic they came from.
package consume

import (
	"context"
	"log"
	"sync"

	pb "github.com/adityabansal/tapstream/server/gen/tapstream/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type Kind int

const (
	KindClick Kind = iota
	KindOrder
)

func (k Kind) String() string {
	if k == KindOrder {
		return "order"
	}
	return "click"
}

// Event is a decoded Kafka message. Amount is zero for clicks.
type Event struct {
	Kind     Kind
	ID       string
	Category string
	Amount   float64
}

type Consumer struct {
	Brokers    []string
	GroupID    string
	ClickTopic string
	OrderTopic string
}

// Run consumes both topics until ctx is cancelled, sending decoded events on
// the returned channel. The channel is closed once both readers have stopped.
func (c *Consumer) Run(ctx context.Context) <-chan Event {
	out := make(chan Event, 1024)
	var wg sync.WaitGroup

	c.start(ctx, &wg, out, c.ClickTopic, decodeClick)
	c.start(ctx, &wg, out, c.OrderTopic, decodeOrder)

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func (c *Consumer) start(ctx context.Context, wg *sync.WaitGroup, out chan<- Event, topic string, decode func([]byte) (Event, error)) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// One reader per topic, sharing a group so partitions can later be
		// split across several aggregator instances.
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: c.Brokers,
			GroupID: c.GroupID,
			Topic:   topic,
		})
		defer r.Close()

		for {
			// ReadMessage commits the previous offset before returning the
			// next message, so a restart resumes where this group left off.
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("consume %s: %v", topic, err)
				}
				return
			}

			event, err := decode(msg.Value)
			if err != nil {
				// A malformed message must not stall the partition.
				log.Printf("decode %s offset %d: %v", topic, msg.Offset, err)
				continue
			}

			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
}

func decodeClick(b []byte) (Event, error) {
	var m pb.ClickEvent
	if err := proto.Unmarshal(b, &m); err != nil {
		return Event{}, err
	}
	return Event{Kind: KindClick, ID: m.GetEventId(), Category: m.GetCategory()}, nil
}

func decodeOrder(b []byte) (Event, error) {
	var m pb.OrderEvent
	if err := proto.Unmarshal(b, &m); err != nil {
		return Event{}, err
	}
	return Event{Kind: KindOrder, ID: m.GetOrderId(), Category: m.GetCategory(), Amount: m.GetAmount()}, nil
}
