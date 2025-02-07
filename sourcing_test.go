package pbsourcing_test

import (
	"context"
	"testing"
	"time"

	"github.com/gehhilfe/pbsourcing"
	pb "github.com/gehhilfe/pbsourcing/proto"
	"github.com/gehhilfe/pbsourcing/store/memory"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/google/uuid"
)

func Test(t *testing.T) {
	bus := pbsourcing.NewInMemoryMessageBus()
	storeA := memory.NewStore()
	storeB := memory.NewStore()

	syncStoreA := pbsourcing.NewSynchronizedStore(storeA, bus)
	syncStoreB := pbsourcing.NewSynchronizedStore(storeB, bus)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go syncStoreA.Synchronize(ctx)
	go syncStoreB.Synchronize(ctx)

	storeAId := pbsourcing.StoreId(uuid.New())
	ssA, _ := storeA.Create(storeAId, pbsourcing.Metadata{"type": "local"})

	ssA.Save([]*pb.Event{
		&pb.Event{
			StoreId:       "",
			StoreMetadata: "",
			AggregateId:   "1234",
			AggregateType: "test",
			Version:       1,
			StoreVersion:  0,
			GlobalVersion: 0,
			CreatedAt:     &timestamp.Timestamp{},
			Data:          []byte{},
			Metadata:      "",
		},
	})

	time.Sleep(5 * time.Second)
	ssA, _ = storeB.Get(storeAId)
}
