package pbsourcing

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strconv"
	"time"

	pb "github.com/gehhilfe/pbsourcing/proto"
)

type SynchronizedStore struct {
	manager StoreManager
	bus     MessageBus
}

func NewSynchronizedStore(manager StoreManager, bus MessageBus) *SynchronizedStore {
	return &SynchronizedStore{
		manager: manager,
		bus:     bus,
	}
}

func (s *SynchronizedStore) Synchronize(ctx context.Context) error {
	saveSub := s.manager.OnSave(func(store SubStore, events []*pb.Event) {
		for _, event := range events {
			payload := &pb.BusPayload{
				Payload: &pb.BusPayload_CommittedEvent_{
					CommittedEvent: &pb.BusPayload_CommittedEvent{
						Event: event,
					}},
			}
			err := s.bus.Publish(payload)
			if err != nil {
				slog.Error("error publishing event", slog.Any("err", err))
			}
		}
	})
	defer saveSub.Unsubscribe()

	go func() {
		t := time.NewTicker(5 * time.Minute)

		for range t.C {
			select {
			case <-ctx.Done():
				return
			default:
			}

			for ss := range s.manager.List(Metadata{"type": "local"}) {
				storeId := ss.Id()
				s.bus.Publish(&pb.BusPayload{
					Payload: &pb.BusPayload_HeartBeat_{
						HeartBeat: &pb.BusPayload_HeartBeat{
							Header: &pb.BusPayload_Header{
								StoreId: &pb.Id{
									Id: storeId[:],
								},
							},
							LastStoreVersion: ss.LastVersion(),
						},
					},
				})
			}
		}
	}()

	busSub, err := s.bus.Subscribe(func(payload *pb.BusPayload, metadata Metadata) error {
		switch p := payload.Payload.(type) {
		case *pb.BusPayload_CommittedEvent_:
			return s.committedReceived(p.CommittedEvent, metadata)
		case *pb.BusPayload_RequestRsync_:
			return s.requestResyncReceived(p.RequestRsync, metadata)
		case *pb.BusPayload_ResponseRsync_:
			return s.resyncEventsReceived(p.ResponseRsync, metadata)
		case *pb.BusPayload_HeartBeat_:
			return s.heartBeatReceived(p.HeartBeat, metadata)
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer busSub.Unsubscribe()

	<-ctx.Done()
	return nil
}

func (s *SynchronizedStore) committedReceived(m *pb.BusPayload_CommittedEvent, busMetadata Metadata) error {
	slog.Info("committed event received", slog.Any("event", m.Event))
	// Get the store
	storeId := StoreId(m.Event.StoreId.Id)
	store, err := s.manager.Get(storeId)
	if errors.Is(err, ErrStoreNotFound) {
		metadata := Metadata{}
		metadata["type"] = "remote"
		for k, v := range busMetadata {
			if k != "type" {
				metadata[k] = v
			}
		}
		store, err = s.manager.Create(storeId, metadata)
		if err != nil {
			return fmt.Errorf("failed to create remote store: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to get store: %w", err)
	}

	// Ignore if not a remote store
	if store.Metadata()["type"] != "remote" {
		return nil
	}

	err = store.Append(m.Event.SubStoreEvent)
	ooo := &EventOutOfOrderError{}
	if errors.As(err, &ooo) {
		slog.Warn("event out of order", slog.Any("store", ooo.StoreId), slog.Any("expected", ooo.Expected), slog.Any("actual", ooo.Actual))
		// Request resync
		s.bus.Publish(&pb.BusPayload{
			Payload: &pb.BusPayload_RequestRsync_{RequestRsync: &pb.BusPayload_RequestRsync{
				Header: &pb.BusPayload_Header{
					StoreId: &pb.Id{
						Id: storeId[:],
					},
				},
				From: ooo.Expected - 1, // The last version we have
			}},
		})
	}

	return nil
}

func (s *SynchronizedStore) requestResyncReceived(m *pb.BusPayload_RequestRsync, busMetadata Metadata) error {
	_ = busMetadata
	storeId := StoreId(m.Header.StoreId.Id)
	store, err := s.manager.Get(storeId)
	if errors.Is(err, ErrStoreNotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get store: %w", err)
	}

	// Ignore if not a local store
	if store.Metadata()["type"] != "local" {
		return nil
	}

	// Get all events from the requested version
	iterator, err := store.All(m.From)
	if err != nil {
		return fmt.Errorf("failed to get all events: %w", err)
	}
	batchSize := 10

	for events := range chunk(iterator, batchSize) {
		s.bus.Publish(&pb.BusPayload{
			Payload: &pb.BusPayload_ResponseRsync_{ResponseRsync: &pb.BusPayload_ResponseRsync{
				Events: events,
			}},
		})
	}

	return nil
}

func (s *SynchronizedStore) resyncEventsReceived(m *pb.BusPayload_ResponseRsync, busMetadata Metadata) error {
	storeId := StoreId(m.Header.StoreId.Id)
	store, err := s.manager.Get(storeId)
	if errors.Is(err, ErrStoreNotFound) {
		metadata := Metadata{"type": "remote"}
		for k, v := range busMetadata {
			if k != "type" {
				metadata[k] = v
			}
		}
		store, err = s.manager.Create(storeId, metadata)
		if err != nil {
			return nil
		}
	} else if err != nil {
		return nil
	}

	if store.Metadata()["type"] != "remote" {
		return nil
	}

	for _, e := range m.Events {
		err := store.Append(e)
		if err != nil && !errors.Is(err, ErrEventExists) {
			return fmt.Errorf("failed to append event: %w", err)
		}
	}

	return nil
}

func chunk[E any](seq iter.Seq[E], size int) iter.Seq[[]E] {
	return func(yield func([]E) bool) {
		var chunk []E
		for e := range seq {
			chunk = append(chunk, e)
			if len(chunk) == size {
				if !yield(chunk) {
					return
				}
				chunk = nil
			}
		}
		if len(chunk) > 0 {
			yield(chunk)
		}
	}
}

func (s *SynchronizedStore) heartBeatReceived(m *pb.BusPayload_HeartBeat, busMetadata Metadata) error {
	if m.LastStoreVersion == 0 {
		return nil
	}

	storeId := StoreId(m.Header.StoreId.Id)
	store, err := s.manager.Get(storeId)
	if errors.Is(err, ErrStoreNotFound) {
		// create store
		metadata := Metadata{"type": "remote"}
		for k, v := range busMetadata {
			if k != "type" {
				metadata[k] = v
			}
		}
		store, err = s.manager.Create(storeId, metadata)
		if err != nil {
			return nil
		}
		s.bus.Publish(&pb.BusPayload{
			Payload: &pb.BusPayload_RequestRsync_{RequestRsync: &pb.BusPayload_RequestRsync{
				Header: &pb.BusPayload_Header{
					StoreId: m.Header.StoreId,
				},
				From: 0,
			}},
		})
		return nil
	} else if err != nil {
		return nil
	}

	if store.Metadata()["type"] != "remote" {
		return nil
	}

	// Update metedata
	metadata := make(Metadata, len(store.Metadata()))
	for k, v := range store.Metadata() {
		metadata[k] = v
	}
	metadata["last_heartbeat_at"] = time.Now().Format(time.RFC3339)
	metadata["last_heartbeat_version"] = strconv.FormatUint(uint64(m.LastStoreVersion), 10)
	if err := store.UpdateMetadata(metadata); err != nil {
		return nil
	}

	if m.LastStoreVersion > store.LastVersion() {
		s.bus.Publish(&pb.BusPayload{
			Payload: &pb.BusPayload_RequestRsync_{RequestRsync: &pb.BusPayload_RequestRsync{
				Header: &pb.BusPayload_Header{
					StoreId: m.Header.StoreId,
				},
				From: store.LastVersion(),
			}},
		})
	}

	return nil
}
