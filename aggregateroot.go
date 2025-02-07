package pbsourcing

import (
	"errors"
	"reflect"

	pb "github.com/gehhilfe/pbsourcing/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func aggregateType(a aggregate) string {
	return reflect.TypeOf(a).Elem().Name()
}

// AggregateRoot to be included into aggregates
type AggregateRoot struct {
	aggregateID           uuid.UUID
	aggregateVersion      uint64
	aggregateStoreVersion uint64
	aggregateEvents       []*pb.SubStoreEvent
}

const (
	emptyAggregateID = ""
)

// ErrAggregateAlreadyExists returned if the aggregateID is set more than one time
var ErrAggregateAlreadyExists = errors.New("its not possible to set ID on already existing aggregate")

// ErrAggregateNeedsToBeAPointer return if aggregate is sent in as value object
var ErrAggregateNeedsToBeAPointer = errors.New("aggregate needs to be a pointer")

// TrackChange is used internally by behaviour methods to apply a state change to
// the current instance and also track it in order that it can be persisted later.
func (ar *AggregateRoot) TrackChange(a aggregate, data proto.Message) {
	ar.TrackChangeWithMetadata(a, data, nil)
}

// TrackChangeWithMetadata is used internally by behaviour methods to apply a state change to
// the current instance and also track it in order that it can be persisted later.
// meta data is handled by this func to store none related application state
func (ar *AggregateRoot) TrackChangeWithMetadata(a aggregate, data proto.Message, metadata Metadata) {
	// This can be overwritten in the constructor of the aggregate
	if ar.aggregateID == uuid.Nil {
		ar.aggregateID = uuid.New()
	}

	d, _ := anypb.New(data)

	event := &pb.SubStoreEvent{
		AggregateId:      &pb.Id{Id: ar.aggregateID[:]},
		AggregateType:    aggregateType(a),
		AggregateVersion: ar.nextVersion(),
		CreatedAt:        timestamppb.Now(),
		Data:             d,
		Metadata:         metadata,
	}
	ar.aggregateEvents = append(ar.aggregateEvents, event)
	a.Transition(event)
}

// BuildFromHistory builds the aggregate state from events
func (ar *AggregateRoot) BuildFromHistory(a aggregate, events []*pb.SubStoreEvent) {
	for _, event := range events {
		a.Transition(event)
		//Set the aggregate ID
		ar.aggregateID = uuid.UUID(event.AggregateId.Id)
		// Make sure the aggregate is in the correct version (the last event)
		ar.aggregateVersion = event.AggregateVersion
		ar.aggregateStoreVersion = event.StoreVersion
	}
}

func (ar *AggregateRoot) nextVersion() uint64 {
	return ar.Version() + 1
}

// update sets the AggregateVersion and AggregateGlobalVersion to the values in the last event
// This function is called after the aggregate is saved in the repository
func (ar *AggregateRoot) update() {
	if len(ar.aggregateEvents) > 0 {
		lastEvent := ar.aggregateEvents[len(ar.aggregateEvents)-1]
		ar.aggregateVersion = lastEvent.AggregateVersion
		ar.aggregateStoreVersion = lastEvent.StoreVersion
		ar.aggregateEvents = []*pb.SubStoreEvent{}
	}
}

// path return the full name of the aggregate making it unique to other aggregates with
// the same name but placed in other packages.
func (ar *AggregateRoot) path() string {
	return reflect.TypeOf(ar).Elem().PkgPath()
}

// SetID opens up the possibility to set manual aggregate ID from the outside
func (ar *AggregateRoot) SetID(id uuid.UUID) error {
	if ar.aggregateID != uuid.Nil {
		return ErrAggregateAlreadyExists
	}
	ar.aggregateID = id
	return nil
}

// ID returns the aggregate ID as a uuid.UUID
func (ar *AggregateRoot) ID() uuid.UUID {
	return ar.aggregateID
}

// Root returns the included Aggregate Root state, and is used from the interface Aggregate.
func (ar *AggregateRoot) Root() *AggregateRoot {
	return ar
}

// Version return the version based on events that are not stored
func (ar *AggregateRoot) Version() uint64 {
	if len(ar.aggregateEvents) > 0 {
		return ar.aggregateEvents[len(ar.aggregateEvents)-1].AggregateVersion
	}
	return ar.aggregateVersion
}

// GlobalVersion returns the global version based on the last stored event
func (ar *AggregateRoot) GlobalVersion() uint64 {
	return ar.aggregateStoreVersion
}

// Events return the aggregate events from the aggregate
// make a copy of the slice preventing outsiders modifying events.
func (ar *AggregateRoot) Events() []*pb.SubStoreEvent {
	e := make([]*pb.SubStoreEvent, len(ar.aggregateEvents))
	// convert internal event to external event
	for i, event := range ar.aggregateEvents {
		e[i] = event
	}
	return e
}

// UnsavedEvents return true if there's unsaved events on the aggregate
func (ar *AggregateRoot) UnsavedEvents() bool {
	return len(ar.aggregateEvents) > 0
}
