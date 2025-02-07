package pbsourcing

import (
	"errors"
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
	Publish(payload *pb.BusPaylod) error
	Subscribe(handler func(*pb.BusPaylod, Metadata) error) (Unsubscriber, error)
}

type InMemoryMessageBus struct {
	mu            sync.RWMutex
	subscriptions []func(message *pb.BusPaylod, metadata Metadata) error
}

func NewInMemoryMessageBus() *InMemoryMessageBus {
	return &InMemoryMessageBus{
		subscriptions: make([]func(message *pb.BusPaylod, metadata Metadata) error, 0),
	}
}

func (b *InMemoryMessageBus) Publish(payload *pb.BusPaylod) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, handler := range b.subscriptions {
		if err := handler(payload, Metadata{}); err != nil {
			return err
		}
	}
	return nil
}

func (b *InMemoryMessageBus) Subscribe(handler func(message *pb.BusPaylod, metadata Metadata) error) (Unsubscriber, error) {
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
	handler *func(message *pb.BusPaylod, metadata Metadata) error
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
