package pbsourcing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	pb "github.com/gehhilfe/pbsourcing/proto"
	"google.golang.org/protobuf/proto"
)

// Aggregate interface to use the aggregate root specific methods
type aggregate interface {
	Root() *AggregateRoot
	Transition(event Event)
	EventType() proto.Message
}

type EventRepository struct {
	register   Register
	eventStore SubStore
}

func NewEventRepository(register Register, store SubStore) *EventRepository {
	return &EventRepository{
		register:   register,
		eventStore: store,
	}
}

func (er *EventRepository) Save(a aggregate) error {
	var esEvents = make([]*pb.Event, 0)

	if !er.register.AggregateRegistered(a) {
		return errors.New("aggregate not registered")
	}
	root := a.Root()

	// return as quick as possible when no events to process
	if len(root.aggregateEvents) == 0 {
		return nil
	}

	for _, event := range root.aggregateEvents {
		pbEvent, err := event.ToPb()
		if err != nil {
			return fmt.Errorf("error converting event to pb: %w", err)
		}

		esEvents = append(esEvents, pbEvent)
	}

	err := er.eventStore.Save(esEvents)
	if err != nil {
		return fmt.Errorf("error from event store: %w", err)
	}

	// update the global version on event bound to the aggregate
	for i, event := range esEvents {
		root.aggregateEvents[i].GlobalVersion = event.GlobalVersion
	}

	// update the internal aggregate state
	root.update()
	return nil
}

func (er *EventRepository) Load(ctx context.Context, id string, a aggregate) error {
	it, err := er.eventStore.Get(ctx, id, aggregateType(a), 0)
	if err != nil {
		return fmt.Errorf("error getting events from store: %w", err)
	}

	for e := range it {
		// convert pb event to internal event
		event := &Event{
			StoreId:       id,
			StoreMetadata: Metadata{},
			StoreVersion:  e.StoreVersion,
			AggregateId:   id,
			AggregateType: e.AggregateType,
			Version:       e.Version,
			GlobalVersion: e.GlobalVersion,
			CreatedAt:     e.CreatedAt.AsTime(),
			Data:          nil,
			Metadata:      Metadata{},
		}

		err := json.Unmarshal([]byte(e.Metadata), &event.Metadata)
		if err != nil {
			return fmt.Errorf("error unmarshalling metadata: %w", err)
		}

		err = json.Unmarshal([]byte(e.StoreMetadata), &event.StoreMetadata)
		if err != nil {
			return fmt.Errorf("error unmarshalling store metadata: %w", err)
		}

		event.Data = er.register.Get(a)
		err = proto.Unmarshal(e.Data, event.Data)
		if err != nil {
			return fmt.Errorf("error unmarshalling data: %w", err)
		}

		a.Transition(*event)
	}
	return nil
}
