package pbsourcing

import (
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
	eventStore Store
}

func NewEventRepository(register Register, store Store) *EventRepository {
	return &EventRepository{
		register:   register,
		eventStore: store,
	}
}

func (er *EventRepository) Save(a aggregate) error {
	var esEvents = make([]pb.Event, 0)

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

		esEvents = append(esEvents, *pbEvent)
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
