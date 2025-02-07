package memory

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"sync"

	"github.com/gehhilfe/pbsourcing"
	pb "github.com/gehhilfe/pbsourcing/proto"
)

type aggregateBucket struct {
	aggregateId   string
	aggregateType string
	events        []*pb.Event
}

type SubStore struct {
	mu sync.RWMutex

	id       pbsourcing.StoreId
	metadata pbsourcing.Metadata

	manager *StoreManager

	storeVersion uint64
	storeEvents  []*pb.Event
	aggregates   map[string]*aggregateBucket
}

// Save implements pbsourcing.SubStore.
func (s *SubStore) Save(events []*pb.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	aggregateType := events[0].AggregateType
	aggregateId := events[0].AggregateId

	bucket, ok := s.aggregates[aggregateId]
	if !ok {
		s.aggregates[aggregateId] = &aggregateBucket{
			aggregateId:   aggregateId,
			aggregateType: aggregateType,
			events:        make([]*pb.Event, 0),
		}
		bucket = s.aggregates[aggregateId]
	}

	curStoreVersion := s.storeVersion
	curBucketVersion := uint64(len(bucket.events))

	sMetadata, err := json.Marshal(s.metadata)
	if err != nil {
		return err
	}

	for i, event := range events {
		storeVersion := curStoreVersion + uint64(i) + 1
		bucketVersion := curBucketVersion + uint64(i) + 1

		if event.Version != bucketVersion {
			return pbsourcing.ErrConcurrency
		}

		event.StoreVersion = storeVersion
		event.StoreId = s.id.String()
		event.StoreMetadata = string(sMetadata)
		event.GlobalVersion = uint64(len(s.manager.globalStore) + 1)

		bucket.events = append(bucket.events, event)
		s.storeEvents = append(s.storeEvents, event)
		s.manager.globalStore = append(s.manager.globalStore, event)
	}

	go s.manager.saved(s, events)
	return nil
}

func (s *SubStore) Get(ctx context.Context, id string, aggregateType string, afterVersion uint64) (iter.Seq[*pb.Event], error) {
	bucket, ok := s.aggregates[id]
	if !ok {
		return nil, errors.New("no aggregate event stream")
	}

	return func(yield func(*pb.Event) bool) {
		for _, event := range bucket.events[afterVersion:] {
			if !yield(event) {
				return
			}
		}
	}, nil
}

// All implements pbsourcing.SubStore.
func (s *SubStore) All(start uint64) (iter.Seq[*pb.Event], error) {
	s.mu.RLock()
	return func(yield func(*pb.Event) bool) {
		defer s.mu.RUnlock()

		for _, event := range s.storeEvents[start:] {
			if !yield(event) {
				break
			}
		}
	}, nil
}

// Append implements pbsourcing.SubStore.
func (s *SubStore) Append(event *pb.Event) error {
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

	aggregateId := event.AggregateId
	bucket, ok := s.aggregates[aggregateId]
	if !ok {
		s.aggregates[aggregateId] = &aggregateBucket{
			aggregateId:   aggregateId,
			aggregateType: event.AggregateType,
			events:        make([]*pb.Event, 0),
		}
		bucket = s.aggregates[aggregateId]
	}

	nextAggregateVersion := uint64(len(bucket.events) + 1)
	if event.Version < nextAggregateVersion {
		return pbsourcing.ErrEventExists
	} else if event.Version > nextAggregateVersion {
		return &pbsourcing.EventOutOfOrderError{
			StoreId:  s.id,
			Expected: nextStoreVersion,
			Actual:   event.StoreVersion,
		}
	}

	event.StoreId = s.id.String()
	sMetadata, err := json.Marshal(s.metadata)
	if err != nil {
		return err
	}

	event.StoreMetadata = string(sMetadata)
	event.GlobalVersion = uint64(len(s.manager.globalStore) + 1)
	bucket.events = append(bucket.events, event)
	s.storeEvents = append(s.storeEvents, event)
	s.manager.globalStore = append(s.manager.globalStore, event)

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
