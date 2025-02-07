package memory

import (
	"context"
	"errors"
	"iter"
	"sync"

	"github.com/gehhilfe/pbsourcing"
	pb "github.com/gehhilfe/pbsourcing/proto"
	"github.com/google/uuid"
)

type aggregateBucket struct {
	aggregateId   uuid.UUID
	aggregateType string
	events        []*pb.SubStoreEvent
}

type SubStore struct {
	mu sync.RWMutex

	id       pbsourcing.StoreId
	metadata pbsourcing.Metadata

	manager *StoreManager

	storeVersion uint64
	storeEvents  []*pb.SubStoreEvent
	aggregates   map[uuid.UUID]*aggregateBucket
}

// Save implements pbsourcing.SubStore.
func (s *SubStore) Save(events []*pb.SubStoreEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	aggregateType := events[0].AggregateType
	aggregateId := uuid.UUID(events[0].AggregateId.Id)

	bucket, ok := s.aggregates[aggregateId]
	if !ok {
		s.aggregates[aggregateId] = &aggregateBucket{
			aggregateId:   aggregateId,
			aggregateType: aggregateType,
			events:        make([]*pb.SubStoreEvent, 0),
		}
		bucket = s.aggregates[aggregateId]
	}

	curStoreVersion := s.storeVersion
	curBucketVersion := uint64(len(bucket.events))

	gEvents := make([]*pb.Event, 0, len(events))

	for i, event := range events {
		storeVersion := curStoreVersion + uint64(i) + 1
		bucketVersion := curBucketVersion + uint64(i) + 1

		if event.AggregateVersion != bucketVersion {
			return pbsourcing.ErrConcurrency
		}

		gEvent := &pb.Event{}
		event.StoreVersion = storeVersion
		gEvent.SubStoreEvent = event
		gEvent.StoreId = &pb.Id{Id: s.id[:]}
		gEvent.GlobalVersion = uint64(len(s.manager.globalStore) + 1)

		bucket.events = append(bucket.events, event)
		s.storeEvents = append(s.storeEvents, event)
		gEvents = append(gEvents, gEvent)
		s.manager.globalStore = append(s.manager.globalStore, gEvent)

		s.storeVersion = storeVersion
	}

	go s.manager.saved(s, gEvents)
	return nil
}

func (s *SubStore) Get(ctx context.Context, id uuid.UUID, aggregateType string, afterVersion uint64) (iter.Seq[*pb.SubStoreEvent], error) {
	bucket, ok := s.aggregates[id]
	if !ok {
		return nil, errors.New("no aggregate event stream")
	}

	return func(yield func(*pb.SubStoreEvent) bool) {
		for _, event := range bucket.events[afterVersion:] {
			if !yield(event) {
				return
			}
		}
	}, nil
}

// All implements pbsourcing.SubStore.
func (s *SubStore) All(start uint64) (iter.Seq[*pb.SubStoreEvent], error) {
	s.mu.RLock()
	return func(yield func(*pb.SubStoreEvent) bool) {
		defer s.mu.RUnlock()

		for _, event := range s.storeEvents[start:] {
			if !yield(event) {
				break
			}
		}
	}, nil
}

// Append implements pbsourcing.SubStore.
func (s *SubStore) Append(event *pb.SubStoreEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	nextStoreVersion := s.storeVersion + 1

	if event.StoreVersion < nextStoreVersion {
		return pbsourcing.ErrEventExists
	} else if event.StoreVersion > nextStoreVersion {
		return &pbsourcing.EventOutOfOrderError{
			StoreId:  s.id,
			Expected: nextStoreVersion,
			Actual:   event.StoreVersion,
		}
	}

	aggregateId := uuid.UUID(event.AggregateId.Id)
	bucket, ok := s.aggregates[aggregateId]
	if !ok {
		s.aggregates[aggregateId] = &aggregateBucket{
			aggregateId:   aggregateId,
			aggregateType: event.AggregateType,
			events:        make([]*pb.SubStoreEvent, 0),
		}
		bucket = s.aggregates[aggregateId]
	}

	nextAggregateVersion := uint64(len(bucket.events) + 1)
	if event.AggregateVersion < nextAggregateVersion {
		return pbsourcing.ErrEventExists
	} else if event.AggregateVersion > nextAggregateVersion {
		return &pbsourcing.EventOutOfOrderError{
			StoreId:  s.id,
			Expected: nextStoreVersion,
			Actual:   event.StoreVersion,
		}
	}

	bucket.events = append(bucket.events, event)
	s.storeEvents = append(s.storeEvents, event)

	gEvent := &pb.Event{
		StoreId: &pb.Id{
			Id: s.id[:],
		},
		GlobalVersion: uint64(len(s.manager.globalStore) + 1),
		SubStoreEvent: event,
	}

	s.manager.globalStore = append(s.manager.globalStore, gEvent)
	s.storeVersion = nextStoreVersion

	return nil
}

// Id implements pbsourcing.SubStore.
func (s *SubStore) Id() pbsourcing.StoreId {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.id
}

// LastVersion implements pbsourcing.SubStore.
func (s *SubStore) LastVersion() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.storeVersion
}

// Metadata implements pbsourcing.SubStore.
func (s *SubStore) Metadata() pbsourcing.Metadata {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.metadata
}

// UpdateMetadata implements pbsourcing.SubStore.
func (s *SubStore) UpdateMetadata(metadata pbsourcing.Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metadata = metadata
	return nil
}

var _ pbsourcing.SubStore = &SubStore{}
