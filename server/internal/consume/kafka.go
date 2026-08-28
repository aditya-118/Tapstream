// Package consume reads clickstream and order events from Kafka and decodes
// them into a single stream, so callers do not care which topic they came from.
package consume

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/adityabansal/tapstream/server/gen/tapstream/v1"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Kind int

const (
	KindClick Kind = iota
	KindOrder
)

func (k Kind) String() string {
	switch k {
	case KindClick:
		return "click"
	case KindOrder:
		return "order"
	default:
		return "unknown"
	}
}

// Event is a decoded Kafka message. Amount is zero for clicks. Ts is when the
// producer emitted the event, which lets a consumer tell live traffic from a
// backlog it is catching up on.
type Event struct {
	Kind     Kind
	ID       string
	Category string
	Amount   float64
	Ts       time.Time
}

type Consumer struct {
	Brokers    []string
	GroupID    string
	ClickTopic string
	OrderTopic string
}

// Run consumes both topics until ctx is cancelled, sending decoded events on
// the returned channel. Both channels are closed once both readers stop.
//
// A reader that fails reports on errs before stopping. Without that, losing
// only the order reader would leave the caller consuming clicks forever and
// reporting zero orders, which reads as a quiet shop rather than a fault.
func (c *Consumer) Run(ctx context.Context) (<-chan Event, <-chan error) {
	out := make(chan Event, 1024)
	errs := make(chan error, 2) // one per reader, so neither blocks on the way out
	var wg sync.WaitGroup

	c.start(ctx, &wg, out, errs, c.ClickTopic, decodeClick)
	c.start(ctx, &wg, out, errs, c.OrderTopic, decodeOrder)

	go func() {
		wg.Wait()
		close(out)
		close(errs)
	}()
	return out, errs
}

func (c *Consumer) start(
	ctx context.Context,
	wg *sync.WaitGroup,
	out chan<- Event,
	errs chan<- error,
	topic string,
	decode func([]byte) (Event, error),
) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// One reader per topic, sharing a group so partitions can later be
		// split across several aggregator instances.
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: c.Brokers,
			GroupID: c.GroupID,
			Topic:   topic,
			// Surfaces rebalances and commit retries, which are most of what
			// is interesting about running in a consumer group.
			ErrorLogger: kafka.LoggerFunc(log.Printf),
		})
		defer r.Close()

		for {
			// ReadMessage commits this message's offset before returning it,
			// making delivery at-most-once: an event dropped between here and
			// the aggregator is never redelivered. That is the right trade
			// here because the windows are in-memory anyway - a restart loses
			// the whole window regardless - and at-least-once would instead
			// double-count on every rebalance, which shows up as a rate that
			// is visibly too high rather than imperceptibly too low.
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() == nil {
					errs <- fmt.Errorf("consume %s: %w", topic, err)
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
	return Event{
		Kind:     KindClick,
		ID:       m.GetEventId(),
		Category: m.GetCategory(),
		Ts:       eventTime(m.GetTs()),
	}, nil
}

func decodeOrder(b []byte) (Event, error) {
	var m pb.OrderEvent
	if err := proto.Unmarshal(b, &m); err != nil {
		return Event{}, err
	}
	return Event{
		Kind:     KindOrder,
		ID:       m.GetOrderId(),
		Category: m.GetCategory(),
		Amount:   m.GetAmount(),
		Ts:       eventTime(m.GetTs()),
	}, nil
}

// eventTime returns the zero time for a message with no timestamp, so callers
// can tell "no timestamp" from "very old".
func eventTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}
