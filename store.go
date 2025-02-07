package pbsourcing

import (
	"context"
	"sync"

	"github.com/gehhilfe/pbsourcing/proto"
)

type Store interface {
	Save(events []proto.Event) error
	Get(ctx context.Context, aggregateId string, aggregateType string, afterVersion uint64) ([]proto.Event, error)
}

type MemoryStore struct {
	mu              sync.Mutex
	events          []proto.Event
	aggregateEvents map[string][]proto.Event
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		events:          make([]proto.Event, 0),
		aggregateEvents: make(map[string][]proto.Event),
	}
}

func (s *MemoryStore) Save(events []proto.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		s.events = append(s.events, event)
		s.aggregateEvents[event.AggregateId] = append(s.aggregateEvents[event.AggregateId], event)
	}
	return nil
}

func (s *MemoryStore) Get(ctx context.Context, aggregateId string, aggregateType string, afterVersion uint64) ([]proto.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.aggregateEvents[aggregateId]
	if events == nil {
		return nil, nil
	}

	filteredEvents := make([]proto.Event, 0)
	for _, event := range events {
		if event.Version > afterVersion && event.AggregateType == aggregateType {
			filteredEvents = append(filteredEvents, proto.Event{
				AggregateId:   event.AggregateId,
				AggregateType: event.AggregateType,
				Version:       event.Version,
				CreatedAt:     event.CreatedAt,
				Data:          event.Data,
				Metadata:      event.Metadata,
			})
		}
	}
	return filteredEvents, nil
}
