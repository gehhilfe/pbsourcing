package pbsourcing_test

import (
	"context"
	"testing"
	"time"

	"github.com/gehhilfe/pbsourcing"
	pb "github.com/gehhilfe/pbsourcing/proto"
	"github.com/gehhilfe/pbsourcing/store/memory"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	time.Sleep(100 * time.Millisecond)

	storeAId := pbsourcing.StoreId(uuid.New())
	ssA, _ := storeA.Create(storeAId, pbsourcing.Metadata{"type": "local"})

	ssA.Save([]*pb.SubStoreEvent{
		&pb.SubStoreEvent{
			AggregateId: &pb.Id{
				Id: storeAId[:],
			},
			AggregateType:    "test",
			AggregateVersion: 1,
			StoreVersion:     1,
			CreatedAt:        &timestamppb.Timestamp{},
			Data:             nil,
			Metadata:         map[string]string{},
		},
	})

	time.Sleep(100 * time.Millisecond)

	ssA, _ = storeB.Get(storeAId)
	if ssA == nil {
		t.Fatal("store not found")
	}

	if ssA.LastVersion() != 1 {
		t.Fatalf("unexpected version: %v", ssA.LastVersion())
	}
}
