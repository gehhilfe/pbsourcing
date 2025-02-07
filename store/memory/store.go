package memory

import (
	"iter"
	"sync"

	"github.com/gehhilfe/pbsourcing"
	pb "github.com/gehhilfe/pbsourcing/proto"
	"github.com/google/uuid"
)

type StoreManager struct {
	mu          sync.RWMutex
	stores      map[pbsourcing.StoreId]*SubStore
	globalStore []*pb.Event

	onSaveHandlers []func(pbsourcing.SubStore, []*pb.Event)
}

// All implements pbsourcing.StoreManager.
func (s *StoreManager) All(globalVersion uint64, filter pbsourcing.Filter) (iter.Seq[*pb.Event], error) {
	s.mu.RLock()
	return func(yield func(*pb.Event) bool) {
		defer s.mu.RUnlock()

		if globalVersion >= uint64(len(s.globalStore)) {
			return
		}
	eventLoop:
		for _, gEntry := range s.globalStore[globalVersion:] {
			if filter.AggregateType != nil && gEntry.SubStoreEvent.AggregateType != *filter.AggregateType {
				continue eventLoop
			}
			if filter.AggregateID != nil && uuid.UUID(gEntry.SubStoreEvent.AggregateId.Id) != *filter.AggregateID {
				continue eventLoop
			}
			if filter.Metadata != nil {
				for k, v := range *filter.Metadata {
					if gEntry.SubStoreEvent.Metadata[k] != v {
						continue eventLoop
					}
				}
			}

			store := s.stores[pbsourcing.StoreId(gEntry.StoreId.Id)]
			if store == nil {
				return
			}

			if filter.StoreMetadata != nil {
				for k, v := range *filter.StoreMetadata {
					if store.metadata[k] != v {
						continue eventLoop
					}
				}
			}

			if !yield(gEntry) {
				return
			}
		}
	}, nil
}

// Create implements pbsourcing.StoreManager.
func (s *StoreManager) Create(id pbsourcing.StoreId, metadata pbsourcing.Metadata) (pbsourcing.SubStore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s, ok := s.stores[id]; ok {
		return s, nil
	}

	store := &SubStore{
		id:           id,
		metadata:     metadata,
		manager:      s,
		storeVersion: 0,
		storeEvents:  make([]*pb.SubStoreEvent, 0),
		aggregates:   make(map[uuid.UUID]*aggregateBucket),
	}

	s.stores[id] = store
	return store, nil
}

// Get implements pbsourcing.StoreManager.
func (s *StoreManager) Get(id pbsourcing.StoreId) (pbsourcing.SubStore, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	store, ok := s.stores[id]
	if !ok {
		return nil, pbsourcing.ErrStoreNotFound
	}
	return store, nil
}

// List implements pbsourcing.StoreManager.
func (s *StoreManager) List(metadata pbsourcing.Metadata) iter.Seq[pbsourcing.SubStore] {
	s.mu.RLock()
	return func(yield func(pbsourcing.SubStore) bool) {
		defer s.mu.RUnlock()
	storeLoop:
		for _, store := range s.stores {
			for k, v := range metadata {
				if store.metadata[k] != v {
					continue storeLoop
				}
			}
			if !yield(store) {
				break
			}
		}
	}
}

func (s *StoreManager) saved(store pbsourcing.SubStore, events []*pb.Event) {
	for _, handler := range s.onSaveHandlers {
		handler(store, events)
	}
}

// OnCommit implements pbsourcing.StoreManager.
func (s *StoreManager) OnSave(handler func(pbsourcing.SubStore, []*pb.Event)) pbsourcing.Unsubscriber {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.onSaveHandlers = append(s.onSaveHandlers, handler)
	return pbsourcing.UnsubscribeFunc(func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		for i, c := range s.onSaveHandlers {
			if &c == &handler {
				s.onSaveHandlers = append(s.onSaveHandlers[:i], s.onSaveHandlers[i+1:]...)
				return nil
			}
		}
		return nil
	})
}

func NewStore() pbsourcing.StoreManager {
	return &StoreManager{
		stores:         make(map[pbsourcing.StoreId]*SubStore),
		globalStore:    make([]*pb.Event, 0),
		onSaveHandlers: make([]func(pbsourcing.SubStore, []*pb.Event), 0),
	}
}
