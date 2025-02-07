package pbsourcing

import (
	"context"
	"fmt"

	pb "github.com/gehhilfe/pbsourcing/proto"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// Aggregate interface to use the aggregate root specific methods
type aggregate interface {
	Root() *AggregateRoot
	Transition(event *pb.SubStoreEvent)
	EventType() proto.Message
}

type EventRepository struct {
	eventStore SubStore
}

func NewEventRepository(store SubStore) *EventRepository {
	return &EventRepository{
		eventStore: store,
	}
}

func (er *EventRepository) Save(a aggregate) error {
	root := a.Root()

	// return as quick as possible when no events to process
	if len(root.aggregateEvents) == 0 {
		return nil
	}

	err := er.eventStore.Save(root.aggregateEvents)
	if err != nil {
		return fmt.Errorf("error from event store: %w", err)
	}

	// update the internal aggregate state
	root.update()
	return nil
}

func (er *EventRepository) Load(ctx context.Context, id uuid.UUID, a aggregate) error {
	it, err := er.eventStore.Get(ctx, id, aggregateType(a), 0)
	if err != nil {
		return fmt.Errorf("error getting events from store: %w", err)
	}

	for e := range it {
		// convert pb event to internal event
		a.Transition(e)
	}
	return nil
}
