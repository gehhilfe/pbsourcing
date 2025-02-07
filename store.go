package pbsourcing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"

	pb "github.com/gehhilfe/pbsourcing/proto"
	"github.com/google/uuid"
)

type Metadata map[string]string

func (m *Metadata) Scan(src any) error {
	data, ok := src.([]uint8)
	if !ok {
		return errors.New("invalid data type for Metadata")
	}
	return json.Unmarshal(data, m)
}

type Filter struct {
	AggregateType *string
	AggregateID   *uuid.UUID
	Metadata      *Metadata
	StoreMetadata *Metadata
}

var (
	ErrConcurrency   = errors.New("concurrency error")
	ErrStoreNotFound = errors.New("store not found")
	ErrEventExists   = errors.New("event already exists")
)

type EventOutOfOrderError struct {
	StoreId  StoreId
	Expected uint64
	Actual   uint64
}

func (e *EventOutOfOrderError) Error() string {
	return fmt.Sprintf("event out of order: store=%s, expected=%d, actual=%d", e.StoreId, e.Expected, e.Actual)
}

type StoreManager interface {
	List(metadata Metadata) iter.Seq[SubStore]
	Create(id StoreId, metadata Metadata) (SubStore, error)
	Get(id StoreId) (SubStore, error)
	OnSave(handler func(SubStore, []*pb.Event)) Unsubscriber
	All(globalVersion uint64, filter Filter) (iter.Seq[*pb.Event], error)
}

type SubStore interface {
	Id() StoreId
	Metadata() Metadata
	Save(events []*pb.SubStoreEvent) error

	// start is the non inclusive version to start from
	All(storeVersion uint64) (iter.Seq[*pb.SubStoreEvent], error)
	Get(ctx context.Context, id uuid.UUID, aggregateType string, afterVersion uint64) (iter.Seq[*pb.SubStoreEvent], error)
	Append(event *pb.SubStoreEvent) error
	LastVersion() uint64
	UpdateMetadata(metadata Metadata) error
}
