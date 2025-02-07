package pbsourcing

import (
	"errors"
	"log/slog"
	"sync"

	pb "github.com/gehhilfe/pbsourcing/proto"
)

type Unsubscriber interface {
	Unsubscribe() error
}

type UnsubscribeFunc func() error

func (u UnsubscribeFunc) Unsubscribe() error {
	return u()
}

type MessageBus interface {
	Publish(payload *pb.BusPayload) error
	Subscribe(handler func(*pb.BusPayload, Metadata) error) (Unsubscriber, error)
}

type InMemoryMessageBus struct {
	mu            sync.RWMutex
	subscriptions []func(message *pb.BusPayload, metadata Metadata) error
}

func NewInMemoryMessageBus() *InMemoryMessageBus {
	return &InMemoryMessageBus{
		subscriptions: make([]func(message *pb.BusPayload, metadata Metadata) error, 0),
	}
}

func (b *InMemoryMessageBus) Publish(payload *pb.BusPayload) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	slog.Info("publishing message", slog.Any("payload", payload))
	for _, handler := range b.subscriptions {
		go handler(payload, Metadata{})
	}
	return nil
}

func (b *InMemoryMessageBus) Subscribe(handler func(message *pb.BusPayload, metadata Metadata) error) (Unsubscriber, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subscriptions = append(b.subscriptions, handler)
	return &unsubscriber{
		bus:     b,
		handler: &handler,
	}, nil
}

type unsubscriber struct {
	bus     *InMemoryMessageBus
	handler *func(message *pb.BusPayload, metadata Metadata) error
}

func (u *unsubscriber) Unsubscribe() error {
	u.bus.mu.Lock()
	defer u.bus.mu.Unlock()

	handlers := u.bus.subscriptions
	for i, handler := range handlers {
		if &handler == u.handler {
			u.bus.subscriptions = append(handlers[:i], handlers[i+1:]...)
			return nil
		}
	}
	return errors.New("subscription not found")
}
